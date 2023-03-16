import pandas as pd
import logging


class DataMapping:

    def get_data(self) -> int:
        print('time:', self.move_in_time, self.move_out_time)
        colle_wip = self.db[self.tbname]
        match = {"$match": {"FAB_ID": {"$ne": None}, "LOT_ID": {"$ne": None},
                            self.time_col: {"$gte": self.move_in_time, "$lte": self.move_out_time}}}
        project = {"$project": {"_id": 0}}
        pipeline = [match, project]
        self.source_df = pd.DataFrame(list(colle_wip.aggregate(pipeline)))
        return len(self.source_df)

    def step_mapping(self):
        # eis/ event: lot + eqp + time --> step
        mask = (self.source_df['EQP_ID'].notnull()) & (self.source_df['LOT_ID'].notnull())
        target_df = self.source_df[mask]
        wip_df = self.wip_df[['FAB_ID', 'EQP_ID', 'LOT_ID', 'STEP', 'MOVE_IN_TIME', 'MOVE_OUT_TIME']]
        target_df = pd.merge(target_df, wip_df, on=['FAB_ID', 'EQP_ID', 'LOT_ID'])

        mask = (target_df[self.time_col] >= target_df['MOVE_IN_TIME']) & (target_df[self.time_col] <= target_df['MOVE_OUT_TIME'])
        target_df = target_df[mask].drop(['MOVE_IN_TIME', 'MOVE_OUT_TIME'], axis=1)
        self.source_df = target_df
        log_text = f"finished step mapping"
        logging.info(log_text)

    def step_eqp_mapping(self) -> None:
        # spc: lot + time --> step + eqp
        pass

    def prod_mapping(self) -> None:
        # lot --> prod + lot_type + layer + station
        mask = (self.source_df['LOT_ID'].notnull())
        target_df = self.source_df[mask]
        wip_df = self.wip_df[['FAB_ID', 'LOT_ID', 'PROD_ID', 'LOT_TYPE', 'LAYER', 'STATION']].drop_duplicates()
        target_df = pd.merge(target_df, wip_df, on=['FAB_ID', 'LOT_ID'], how='left')
        self.source_df = target_df
        log_text = f"finished prod mapping"
        logging.info(log_text)
