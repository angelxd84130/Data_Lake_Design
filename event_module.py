import pandas as pd
from idatamation_module import IdatamationFlow


class EventIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "EVENT"
        self.type_dict = {"Lot ID": str, "FAB_ID": str, "EQP_ID": str, "TYPE": str, "Alarm Code": str,
                          "Comment(Chinese)": str, "Comment(English)": str}
        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"LOT ID": "LOT_ID"}
        self.data_type = {"FAB_ID": str, "LOT_ID": str, "START_TIME": object, "EQP_ID": str, "TYPE": str,
                     "EVENT_MSG": str}

    def data_transformat(self, df, filename):
        df["START_TIME"] = df["DATE"].astype(str) + " " + df["TIME"].astype(str)
        df["START_TIME"] = pd.to_datetime(df["START_TIME"])
        df["START_TIME"] = df["START_TIME"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["COMMENT(CHINESE)"] = df["COMMENT(CHINESE)"].astype(str).apply(lambda s: s.strip())
        df["COMMENT(ENGLISH)"] = df["COMMENT(ENGLISH)"].astype(str).apply(lambda e: e.strip())
        df["EVENT_MSG"] = df["COMMENT(CHINESE)"] + "-" + df["COMMENT(ENGLISH)"]
        df = df.rename(columns=self.replace_column_list)
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df["START_TIME"] = pd.to_datetime(df["START_TIME"])
        df = self.data_type_check(df, self.data_type)
        self.mongo_insert_data(df, "events_original", filename, set(), set(), overlay_data=True)
        return df.shape[0]


process_data = EventIdatamation()
process_data.main_function()
