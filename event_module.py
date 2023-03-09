import pandas as pd
from idatamation_module import IdatamationFlow

fab_folder = "S3"
data_source = "EVENT"
type_dict = {"Lot ID": str, "FAB_ID": str, "EQP_ID": str, "TYPE": str, "Alarm Code": str,
             "Comment(Chinese)": str, "Comment(English)": str}
data_type = {"FAB_ID": str, "LOT_ID": str, "START_TIME": object, "EQP_ID": str, "TYPE": str,
             "EVENT_MSG": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["INDEX", "DATE", "TIME", "PART NO.", "LOT ID", "FAB_ID", "STEP", "EQP_ID", "TYPE",
                           "ALARM CODE", "COMMENT(CHINESE)", "RECIPE NO.", "JOB NO.", "OP ID", "STATUS",
                           "COMMENT(ENGLISH)"]
use_column_list = ["FAB_ID", "EQP_ID", "LOT_ID", "TYPE", "EVENT_MSG", "START_TIME"]
replace_column_list = {"LOT ID": "LOT_ID"}


class EventIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        df["START_TIME"] = df["DATE"].astype(str) + " " + df["TIME"].astype(str)
        df["START_TIME"] = pd.to_datetime(df["START_TIME"])
        df["START_TIME"] = df["START_TIME"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["COMMENT(CHINESE)"] = df["COMMENT(CHINESE)"].astype(str).apply(lambda s: s.strip())
        df["COMMENT(ENGLISH)"] = df["COMMENT(ENGLISH)"].astype(str).apply(lambda e: e.strip())
        df["EVENT_MSG"] = df["COMMENT(CHINESE)"] + "-" + df["COMMENT(ENGLISH)"]
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df["START_TIME"] = pd.to_datetime(df["START_TIME"])
        df = self.data_type_check(df, data_type)
        self.log_csv_save_result(df, "events_original", filename)
        return df.shape[0]


process_data = EventIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)
