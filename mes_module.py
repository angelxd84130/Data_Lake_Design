import pandas as pd
from datetime import datetime, timedelta
from idatamation_module import IdatamationFlow
from data_process.ms_group import MSGroup
from data_process.spc_group import SPCGroup
from data_process.event_group import EventGroup

default_args = {
    "owner": "Angel",
    "start_date": datetime(2023, 2, 22),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "dependents_on_past": False,
}

fab_folder = "S3"
data_source = "MES"
type_dict = {"Current_Plant": str, "Part_Number": str, "Lot_Number": str, "Layer": str, "Step": str, "Step_Type": str,
             "Mc_No": str, "Recipe_Name": str}
data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "STEP": str, "EQP_ID": str,
             "RECIPE_ID": str, "STEP_TYPE": str, "PROCESS_TIME": float,  "LAYER": str, "STATION": str,
             "QUEUE_TIME": float, "MOVE_IN_TIME": object, "MOVE_OUT_TIME": object, "PROD_ID": str, "LOT_TYPE": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["CURRENT_PLANT", "PART_NUMBER", "LOT_NUMBER", "LAYER", "STEP", "STEP_TYPE", "MC_NO",
                           "RECIPE_NAME", "CHECK_IN_TIME", "CHECK_OUT_TIME", "PROCESSING_TIME",
                           "QUEUE_TIME", "CURRENT_PLANT_ID", "LAYER_ID"]
use_column_list = ["FAB_ID", "PROD_ID_RAW", "EQP_ID", "LOT_ID", "RECIPE_ID", "STEP", "STEP_TYPE",
                   "MOVE_IN_TIME", "MOVE_OUT_TIME", "PROCESS_TIME", "QUEUE_TIME", "SEQUENCE", "LAYER", "STATION"]
replace_column_list = {"CURRENT_PLANT": "FAB_ID", "PART_NUMBER": "PROD_ID_RAW", "LOT_NUMBER": "LOT_ID", "MC_NO": "EQP_ID",
                       "RECIPE_NAME": "RECIPE_ID", "CHECK_IN_TIME": "MOVE_IN_TIME", "CHECK_OUT_TIME": "MOVE_OUT_TIME",
                       "PROCESSING_TIME": "PROCESS_TIME"}


class MESIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
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
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df["MOVE_IN_TIME"] = pd.to_datetime(df["MOVE_IN_TIME"])
        df["MOVE_OUT_TIME"] = pd.to_datetime(df["MOVE_OUT_TIME"])
        df = self.get_prodID_and_lotTYPE(df)
        df = self.data_type_check(df, data_type)
        global dataframe
        dataframe = df
        key_col = {'FAB_ID', 'STEP', 'RPOD_ID_RAW', 'EQP_ID', 'LOT_ID', 'RECIPE_ID', 'STEP_TYPE',
                   'MOVE_IN_TIME', 'MOVE_OUT_TIME'}
        update_col = {'PROCESS_TIME', 'QUEUE_TIME', 'SEQUENCE', 'PROD_ID', 'LOT_TYPE'}
        self.mongo_insert_data(df, "wip_lot", filename, key_col, update_col)
        return df.shape[0]


class SourceDataProcess:
    def ms_process(self):
        ms_data = MSGroup(dataframe)
        ms_data.main_function()

    def spc_process(self):
        spc_data = SPCGroup(dataframe)
        spc_data.main_function()

    def event_process(self):
        event_data = EventGroup(dataframe)
        event_data.main_function()

    def main_funtion(self):
        print("ms", datetime.now())
        self.ms_process()
        print("spc", datetime.now())
        self.spc_process()
        print("event", datetime.now())
        self.event_process()
        print("finish", datetime.now())



process_data = MESIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)
source_data_process = SourceDataProcess()
source_data_process.main_funtion()
