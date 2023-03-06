import pandas as pd
from idatamation_module import IdatamationFlow

fab_folder = "S3"
data_source = "EIS"
type_dict = {"PartNo": str, "LotNo": str, "FabId": str, "MachineNo": str, "Step": str, "Description": str}
data_type = {"FAB_ID": str, "LOT_ID": str, "TIME": object, "EQP_ID": str, "PARAMETER_ID": str, "VALUE": int}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["TIMESTAMP", "PARTNO", "LOTNO", "FABID", "MACHINENO", "STEP", "DESCRIPTION", "VALUE"]
use_column_list = ["FAB_ID", "LOT_ID", "TIME", "EQP_ID", "PARAMETER_ID", "VALUE"]
replace_column_list = {"TIMESTAMP": "TIME", "PARTNO": "PROD_ID_RAW", "LOTNO": "LOT_ID", "FABID": "FAB_ID",
                       "MACHINENO": "EQP_ID", "DESCRIPTION": "PARAMETER_ID"}


class EISIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        # TODO: Changing the time zone is not required.
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
        df["TIMESTAMP"] = df["TIMESTAMP"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.data_type_check(df, data_type)
        self.log_csv_save_result(df, "ms_original_lot", filename)
        return df.shape[0]


process_data = EISIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)
