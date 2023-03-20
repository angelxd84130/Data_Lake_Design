from mes_module import SourceDataProcess
from datetime import datetime, timedelta
from query_data_module import ConnectToMongo
import pandas as pd

class DailyCheck():
    def __init__(self):
        super().__init__()
        self.tbname = 'wip_lot'
        self.fab = 'S3'
        self.start_time = datetime.now() - timedelta(days=5)
        self.end_time = datetime.now()
        self.wip_df = self.get_wip()
        self.time_col = ["MOVE_IN_TIME", "MOVE_OUT_TIME"]

    def get_wip(self):

        colle_wip = self.db[self.tbname]
        match = {"$match": {"FAB_ID": self.fab,
                            "MOVE_OUT_TIME": {"$gte": self.start_time, "$lte": self.end_time}}}
        project = {"$project": {"_id": 0}}
        pipeline = [match, project]
        wip_df = pd.DataFrame(list(colle_wip.aggregate(pipeline)))
        return wip_df

    def mapping_check(self):
        if not self.wip_df.empty:
            self.wip_df["MOVE_IN_TIME"] = self.wip_df["MOVE_IN_TIME"].dt.tz_localize("UTC")
            self.wip_df["MOVE_OUT_TIME"] = self.wip_df["MOVE_OUT_TIME"].dt.tz_localize("UTC")
            source_data_process = SourceDataProcess(self.wip_df, self.db, self.mongo_remove, self.mongo_import,
                                                    self.bulk_write)
            source_data_process.main_funtion()


daily_check = DailyCheck()
daily_check.mapping_check()
