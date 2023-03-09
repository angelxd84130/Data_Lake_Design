from datetime import datetime, timedelta
from idatamation_module import IdatamationFlow

fab_folder = "S3"
data_source = "SPC_plan_oqc_mapping"
type_dict = {"V用": str, "Station": str, "FileName": str, "CtrlName": str, "模組": str, "課別": str}
data_type = {"FAB_ID": str, "FILE_NAME": str, "PARAMETER_ID": str, "MODULE": str, "DEPARTMENT": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["V用", "STATION", "FILENAME", "CTRLNAME", "模組", "課別"]
use_column_list = ["FAB_ID", "FILE_NAME", "PARAMETER_ID", "MODULE", "DEPARTMENT"]
replace_column_list = {"FILENAME": "FILE_NAME", "CTRLNAME": "PARAMETER_ID", "模組": "MODULE", "課別": "DEPARTMENT", }


class SPCPlanOQCIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        df["FAB_ID"] = self.fab_folder
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df = self.data_type_check(df, data_type)
        self.log_csv_save_result(df, "spc_plan_oqc_mapping", filename)
        return df.shape[0]


process_data = SPCPlanOQCIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)