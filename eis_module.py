import pandas as pd
from idatamation_module import IdatamationFlow


class EISIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "EIS"
        self.type_dict = {"PartNo": str, "LotNo": str, "FabId": str, "MachineNo": str, "Step": str, "Description": str,
                          "VALUE": float, "TimeStamp": str}
        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"TIMESTAMP": "TIME", "PARTNO": "PROD_ID_RAW", "LOTNO": "LOT_ID", "FABID": "FAB_ID",
                           "MACHINENO": "EQP_ID", "DESCRIPTION": "PARAMETER_ID"}
        self.data_type = {"FAB_ID": str, "LOT_ID": str, "TIME": object, "EQP_ID": str, "PARAMETER_ID": str, "VALUE": float}

    def data_transformat(self, df, filename):
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
        df["TIMESTAMP"] = df["TIMESTAMP"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df = df.rename(columns=self.replace_column_list)
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.data_type_check(df, self.data_type)
        self.mongo_insert_data(df, "ms_original_lot", filename, set(), set(), duplicated_data=True)
        return df.shape[0]


process_data = EISIdatamation()
process_data.main_function()
