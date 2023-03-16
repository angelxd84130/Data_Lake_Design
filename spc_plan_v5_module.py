from idatamation_module import IdatamationFlow

fab_folder = "S3"
data_source = "SPC_plan_v5"
type_dict = {"Factory": str, "Station": str, "FileName": str, "CtrlName": str, "料號": str}
data_type = {"FAB_ID": str, "STEP": str, "FILE_NAME": str, "PARAMETER_ID": str,
             "usl_val": float, "lsl_val": float, "ucl_val": float, "lcl_val": float, "PROD_ID": str, "LOT_TYPE": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["FACTORY", "STATION", "FILENAME", "CTRLNAME", "料號", "管制分類", "樣本大小", "小數位數",
                           "單位", "DEFAULTCHART", "預設管制圖", "等級", "USL", "LSL", "上圖_XTARGET", "上圖_RTARGET",
                           "下圖_RTARTET", "LCL_TARGET", "UAL", "AL", "LAL", "CPK", "CIR", "TOPUCL", "TOPCL", "TOPLCL",
                           "DOWNUCL", "DOWNCL", "DOWNLCL"]

use_column_list = ["FAB_ID", "PROD_ID_RAW", "STEP", "PARAMETER_ID", "FILE_NAME", "usl_val", "lsl_val", "ucl_val", "lcl_val"]
replace_column_list = {"FACTORY": "FAB_ID", "STATION": "STEP", "料號": "PROD_ID_RAW", "CTRLNAME": "PARAMETER_ID",
                       "USL": "usl_val", "LSL": "lsl_val", "TOPUCL": "ucl_val", "TOPLCL": "lcl_val",
                       "FILENAME": "FILE_NAME"}


class SPCPlanv5Idatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        df = df.rename(columns=replace_column_list)
        df["FAB_ID"] = self.fab_folder
        df = df[use_column_list]

        # final check column type is correct
        df = self.get_prodID_and_lotTYPE(df, 7, "standard")  # spc data has special rules
        df = self.data_type_check(df, data_type)
        key_col = {"FAB_ID", "PROD_ID_RAW", "STEP", "PARAMETER_ID", "FILE_NAME"}
        update_col = {"usl_val", "lsl_val", "ucl_val", "lcl_val"}
        self.mongo_insert_data(df, "spc_plan_v5", filename, key_col, update_col)
        return df.shape[0]


process_data = SPCPlanv5Idatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)