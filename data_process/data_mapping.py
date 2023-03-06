import query_data_module
import pandas as pd
import logging


class DataMapping:
    def __init__(self):
        mongo_conf = query_data_module.ConnectToMongo()
        self.db = mongo_conf.db

    def get_data(self, tbname) -> int:
        print('time:', self.move_in_time, self.move_out_time)
        colle_wip = self.db[tbname]
        match = {"$match": {"FAB_ID": {"$ne": None},
                            "TIME": {"$gte": self.move_in_time, "$lte": self.move_out_time}}}
        project = {"$project": {"_id": 0}}
        pipeline = [match, project]
        self.source_df = pd.DataFrame(list(colle_wip.aggregate(pipeline)))
        return len(self.source_df)

    def step_mapping(self) -> None:
        # eis/ event: lot + eqp + time --> step
        pass

    def step_eqp_mapping(self) -> None:
        # spc: lot + time --> step + eqp
        pass

    def lot_mapping(self) -> None:
        # eqp + time --> lot
        mask = (self.source_df['EQP_ID'].notnull()) & (self.source_df['LOT_ID'].isnull())
        target_df = self.source_df[mask]
        rest_df = self.source_df[~mask]

        wip_df = self.wip_df[['FAB_ID', 'EQP_ID', 'LOT_ID', 'MOVE_IN_TIME', 'MOVE_OUT_TIME']]
        target_df = target_df.drop(['LOT_ID'], axis=1)
        target_df = pd.merge(target_df, wip_df, on=['FAB_ID', 'EQP_ID'])
        mask = (target_df['TIME'] >= target_df['MOVE_IN_TIME']) & (target_df['TIME'] <= target_df['MOVE_OUT_TIME'])
        target_df = target_df[mask].drop(['MOVE_IN_TIME', 'MOVE_OUT_TIME'], axis=1)
        self.source_df = pd.concat([target_df, rest_df])
        log_text = f"finished lot mapping"
        logging.info(log_text)

    def prod_mapping(self) -> None:
        # lot --> prod + lot_type
        mask = (self.source_df['LOT_ID'].notnull())
        target_df = self.source_df[mask]
        wip_df = self.wip_df[['FAB_ID', 'LOT_ID', 'PROD_ID', 'LOT_TYPE']].drop_duplicates()
        target_df = pd.merge(target_df, wip_df, on=['FAB_ID', 'LOT_ID'], how='left')
        self.source_df = target_df
