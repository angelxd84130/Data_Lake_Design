from mes_module import SourceDataProcess
from datetime import datetime, timedelta
from query_data_module import ConnectToMongo
import pandas as pd

class DailyCheck:
    def __init__(self):
        mongo_con = ConnectToMongo()
        self.db = mongo_con.db
        self.tbname = 'wip_lot'
        self.fab = 'S3'
        self.start_time = datetime.now() - timedelta(days=1)
        self.end_time = datetime.now()
        self.wip_df = self.get_wip()
        source_data_process = SourceDataProcess(self.wip_df)
        source_data_process.main_funtion()

    def get_wip(self):

        colle_wip = self.db[self.tbname]
        match = {"$match": {"FAB_ID": self.fab,
                            "MOVE_OUT_TIME": {"$gte": self.start_time, "$lte": self.end_time}}}
        project = {"$project": {"_id": 0}}
        pipeline = [match, project]
        wip_df = pd.DataFrame(list(colle_wip.aggregate(pipeline)))
        return wip_df


daily_check = DailyCheck()

