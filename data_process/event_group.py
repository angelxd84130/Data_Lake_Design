from data_process.data_mapping import DataMapping
from pymongo import UpdateOne
import pandas as pd


class EventGroup(DataMapping):
    def __init__(self, wip_df: pd.DataFrame, db, mongo_remove, mongo_import, bulk_write):
        super().__init__()
        self.wip_df = wip_df.sort_values(by=['MOVE_IN_TIME'], ascending=True)
        self.move_in_time = self.wip_df.get('MOVE_IN_TIME')[0]
        self.move_out_time = self.wip_df.get('MOVE_OUT_TIME')[0]
        self.time_col = 'START_TIME'
        self.tbname = 'events_original'
        self.db = db
        self.mongo_remove = mongo_remove
        self.mongo_import = mongo_import
        self.bulk_write = bulk_write


    def main_function(self) -> None:
        df_size = self.get_data()
        if df_size > 0:
            self.source_df[self.time_col] = self.source_df[self.time_col].dt.tz_localize("UTC")
            self.step_mapping()
            self.prod_mapping()
            self.data_upsert()

    def data_upsert(self):
        updates = []
        for _, row in self.source_df.iterrows():
            query = {'FAB_ID': row.get('FAB_ID'),
                     'EQP_ID': row.get('EQP_ID'),
                     'TYPE': row.get('TYPE'),
                     'EVENT_MSG': row.get('EVENT_MSG'),
                     'START_TIME': row.get('START_TIME'),
                     'LOT_ID': row.get('LOT_ID'),
                     'STEP': row.get('STEP'),
                     'PROD_ID': row.get('PROD_ID'),
                     'LOT_TYPE': row.get('LOT_TYPE'),
                     'LAYER': row.get('LAYER'),
                     'STATION': row.get('STATION')}
            set = {"$set": {"TYPE": row.get('TYPE'), "EVENT_MSG": row.get('EVENT_MSG')}}
            updates.append(UpdateOne(query, set, upsert=True))
        self.bulk_write(self.tbname, 'events', updates)