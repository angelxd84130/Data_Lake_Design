from data_process.data_mapping import DataMapping
from pymongo import UpdateOne
import pandas as pd
import logging


class SPCGroup(DataMapping):
    def __init__(self, wip_df: pd.DataFrame, db, mongo_remove, mongo_import, bulk_write):
        super().__init__()
        self.wip_df = wip_df.sort_values(by=['MOVE_IN_TIME'], ascending=True).reset_index(drop=True)
        self.move_in_time = self.wip_df.get('MOVE_IN_TIME')[0]
        self.wip_df = wip_df.sort_values(by=['MOVE_OUT_TIME'], ascending=True).reset_index(drop=True)
        self.move_out_time = self.wip_df.get('MOVE_OUT_TIME')[len(self.wip_df)-1]
        self.time_col = 'TIME'
        self.tbname = 'spc_group_lot'
        self.update_list = []
        self.db = db
        self.mongo_remove = mongo_remove
        self.mongo_import = mongo_import
        self.bulk_write = bulk_write

    def main_function(self) -> None:
        df_size = self.get_data()
        if df_size > 0:
            self.source_df[self.time_col] = self.source_df[self.time_col].dt.tz_localize("UTC")
            self.step_eqp_mapping()
            self.source_df = self.source_df.drop(['PROD_ID', 'LOT_TYPE'], axis=1)
            self.prod_mapping()
            self.source_df['PROD_ID'] = self.source_df['PROD_ID'].str[:7]
            self.source_df['LOT_TYPE'] = 'standard'
            self.data_upsert()

    def step_eqp_mapping(self) -> None:
        # spc: lot + time --> step + eqp
        mask = (self.source_df['LOT_ID'].notnull())
        target_df = self.source_df[mask]
        target_df = target_df.drop(['STEP', 'EQP_ID'], axis=1)
        wip_df = self.wip_df[['FAB_ID', 'EQP_ID', 'LOT_ID', 'STEP', 'MOVE_IN_TIME', 'MOVE_OUT_TIME']]
        target_df = pd.merge(target_df, wip_df, on=['FAB_ID', 'LOT_ID'])

        mask = (target_df[self.time_col] >= target_df['MOVE_IN_TIME']) & (target_df[self.time_col] <= target_df['MOVE_OUT_TIME'])
        target_df = target_df[mask].drop(['MOVE_IN_TIME', 'MOVE_OUT_TIME'], axis=1)
        self.source_df = target_df
        log_text = f"finished step & eqp mapping"
        logging.info(log_text)

    def data_upsert(self):
        updates = []
        for _, row in self.source_df.iterrows():
            query = {'FAB_ID': row.get('FAB_ID'),
                     'EQP_ID': row.get('EQP_ID'),
                     'DEPARTMENT': row.get('DEPARTMENT'),
                     'CTRL_ID': row.get('CTRL_ID'),
                     'TIME': row.get('TIME'),
                     'LOT_ID': row.get('LOT_ID'),
                     'STEP': row.get('STEP'),
                     'PROD_ID': row.get('PROD_ID'),
                     'LOT_TYPE': row.get('LOT_TYPE'),
                     'FILE_ID': row.get('FILE_ID'),
                     'FILE_NAME': row.get('FILE_NAME'),
                     'PARAMETER_ID': row.get('PARAMETER_ID'),
                     'PROPERTY': row.get('PROPERTY'),
                     'LAYER': row.get('LAYER'),
                     'STATION': row.get('STATION'),
                     'STATION_RAW': row.get('STATION_RAW'),
                     }
            set = {"$set": {"VALUE": row.get('VALUE'), "STD": row.get('STD'), "VALUES": row.get('VALUES')}}
            updates.append(UpdateOne(query, set, upsert=True))
        self.bulk_write(self.tbname, 'spc_lot', updates)

class SPCCompression:

    def __init__(self, df, db, mongo_remove, mongo_import, bulk_write):
        self.source_df = df
        self.tbname = 'spc_original_lot'
        self.update_list = []
        self.db = db
        self.mongo_remove = mongo_remove
        self.mongo_import = mongo_import
        self.bulk_write = bulk_write

    def get_sample_size(self):
        colle_spc_plan = self.db["spc_plan"]
        match = {"$match": {}}
        project = {"$project": {"_id": 0, "sample_size": 1, "CTRL_ID": 1}}
        pipeline = [match, project]
        return pd.DataFrame(list(colle_spc_plan.aggregate(pipeline)))

    def main_function(self):
        spc_plan_df = self.get_sample_size()
        self.source_df = pd.merge(self.source_df, spc_plan_df, on=['CTRL_ID'], how='left')
        self.transfer_compression_data()

    def check_spc_plan_update(self) -> None:
        mask = (self.source_df['sample_size'].notnull())
        source_df_without_sample_size = self.source_df[~mask]
        self.source_df = self.source_df[mask]
        logging.info('error!!! cannot find these ctrl_id in spc_plan:',
              source_df_without_sample_size['CTRL_ID'].unique().tolist(),
              'please update spc_plan')

    def transfer_compression_data(self) -> None:
        self.source_df = self.source_df.dropna(subset=['LOT_ID', 'STEP', 'LAYER'])
        self.check_spc_plan_update()

        if len(self.source_df) < 1:
            log_text = f"no spc_original_lot data to transfer"
            logging.info(log_text)
            return

        checkList = self.source_df.groupby(['FAB_ID', 'STATION', 'DEPARTMENT', 'FILE_ID', 'CTRL_ID', 'sample_size'])
        for key, item in checkList:
            sample_size = int(item['sample_size'].head(1).values[0])
            while sample_size <= len(item):
                df_chunck = item.iloc[:sample_size, :]
                item = item.iloc[sample_size:, :]
                self.group_document(df_chunck)

            if len(item) < sample_size and len(item) > 0:
                logging.info('cannot find pairs with ctrl_id:' + str(int(item['sample_size'].head(1).values[0])))
        self.bulk_write(self.tbname, 'spc_group_lot', self.update_list)
        log_text = f"[UPDATE STEP]: the total of rows is {len(checkList)}"
        logging.info(log_text)

    def _get_value(self, row: pd.DataFrame, col: str) -> str:
        return row[col].sort_values().head(1).values[0]

    def group_document(self, row: pd.DataFrame) -> None:
        query = {"FAB_ID": self._get_value(row, 'FAB_ID'),
                 "PROD_ID": self._get_value(row, 'PROD_ID'),
                 "PROD_ID_RAW": self._get_value(row, 'PROD_ID_RAW'),
                 "STEP": self._get_value(row, 'STEP'),
                 "EQP_ID": self._get_value(row, 'EQP_ID'),
                 "LOT_ID": self._get_value(row, 'LOT_ID'),
                 "PARAMETER_ID": self._get_value(row, 'PARAMETER_ID'),
                 "STATION": self._get_value(row, 'STATION'),
                 "PROPERTY": self._get_value(row, 'PROPERTY'),
                 "DEPARTMENT": self._get_value(row, 'DEPARTMENT'),
                 "FILE_NAME": self._get_value(row, 'FILE_NAME'),
                 "FILE_ID": int(self._get_value(row, 'FILE_ID')),  # monog doesn't accept numpy types
                 "CTRL_ID": int(self._get_value(row, 'CTRL_ID')),
                 "TIME": pd.to_datetime(self._get_value(row, 'TIME')),
                 "LOT_TYPE": "standard"}
        set = {"$set": {"VALUE": round(float(row['VALUE'].mean()), 6),
                        "STD": round(float(row['VALUE'].std()), 6),
                        "VALUES": round(row[['VALUE']], 6).values.flatten().tolist()}}
        self.update_list.append(UpdateOne(query, set, upsert=True))
