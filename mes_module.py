import pandas as pd
from datetime import datetime, timedelta
from idatamation_module import IdatamationFlow
from data_process.ms_group import MSGroup
from data_process.spc_group import SPCGroup
from data_process.event_group import EventGroup


class MESIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "MES"
        self.type_dict = {"Current_Plant": str, "Part_Number": str, "Lot_Number": str, "Layer": str, "Step": str,
                          "Step_Type": str, "Mc_No": str, "Recipe_Name": str}

        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"CURRENT_PLANT": "FAB_ID", "PART_NUMBER": "PROD_ID_RAW", "LOT_NUMBER": "LOT_ID",
                                    "MC_NO": "EQP_ID", "RECIPE_NAME": "RECIPE_ID", "CHECK_IN_TIME": "MOVE_IN_TIME",
                                    "CHECK_OUT_TIME": "MOVE_OUT_TIME", "PROCESSING_TIME": "PROCESS_TIME"}

        self.data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "STEP": str, "EQP_ID": str, "LOT_TYPE": str,
                          "RECIPE_ID": str, "STEP_TYPE": str, "PROCESS_TIME": float, "LAYER": str, "STATION": str,
                          "QUEUE_TIME": float, "MOVE_IN_TIME": object, "MOVE_OUT_TIME": object, "PROD_ID": str}

    def data_transformat(self, df, filename):
        # TODO: Changing the time zone is not required.
        df["CHECK_IN_TIME"] = pd.to_datetime(df["CHECK_IN_TIME"])
        df["CHECK_IN_TIME"] = df["CHECK_IN_TIME"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["CHECK_OUT_TIME"] = pd.to_datetime(df["CHECK_OUT_TIME"])
        df["CHECK_OUT_TIME"] = df["CHECK_OUT_TIME"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["CURRENT_PLANT"] = df["CURRENT_PLANT"].apply(lambda x: x.strip().replace("廠", ""))
        df["STATION"] = df["STEP"]
        df["STEP"] = df["LAYER"].astype(str) + "-" + df["STEP"].astype(str)
        df["MC_NO"] = df["MC_NO"].str.strip()
        df["STEP_TYPE"] = "MIXED"

        df = df.sort_values("CHECK_IN_TIME").reset_index(drop=True)
        df["SEQUENCE"] = None
        df = df.rename(columns=self.replace_column_list)
        df = self.get_prodID_and_lotTYPE(df)
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df["MOVE_IN_TIME"] = pd.to_datetime(df["MOVE_IN_TIME"])
        df["MOVE_OUT_TIME"] = pd.to_datetime(df["MOVE_OUT_TIME"])
        df = self.data_type_check(df, self.data_type)
        global dataframe
        dataframe = df
        key_col = {'FAB_ID', 'STEP', 'PROD_ID_RAW', 'EQP_ID', 'LOT_ID', 'RECIPE_ID', 'STEP_TYPE',
                   'MOVE_IN_TIME', 'MOVE_OUT_TIME', 'LAYER', 'STATION'}
        update_col = {'PROCESS_TIME', 'QUEUE_TIME', 'SEQUENCE', 'PROD_ID', 'LOT_TYPE'}
        self.mongo_insert_data(df, "wip_lot", filename, key_col, update_col)
        source_data_process = SourceDataProcess(dataframe, self.db, self.mongo_remove, self.mongo_import, self.bulk_write)
        source_data_process.main_funtion()
        return df.shape[0]


class SourceDataProcess:
    def __init__(self, df, db, mongo_remove, mongo_import, bulk_write):
        self.wip_df = df
        self.db = db
        self.mongo_remove = mongo_remove
        self.mongo_import = mongo_import
        self.bulk_write = bulk_write
    def ms_process(self):
        ms_data = MSGroup(self.wip_df, self.db, self.mongo_remove, self.mongo_import, self.bulk_write)
        ms_data.main_function()

    def spc_process(self):
        spc_data = SPCGroup(self.wip_df, self.db, self.mongo_remove, self.mongo_import, self.bulk_write)
        spc_data.main_function()

    def event_process(self):
        event_data = EventGroup(self.wip_df, self.db, self.mongo_remove, self.mongo_import, self.bulk_write)
        event_data.main_function()

    def main_funtion(self):
        print("ms", datetime.now())
        self.ms_process()
        print("spc", datetime.now())
        self.spc_process()
        print("event", datetime.now())
        self.event_process()
        print("finish", datetime.now())



process_data = MESIdatamation()
process_data.main_function()

