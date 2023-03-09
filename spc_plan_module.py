import pandas as pd
import query_data_module
from idatamation_module import IdatamationFlow


fab_folder = "S3"
data_source = "SPC_plan"
type_dict = {"Factory": str, "Station": str, "課別": str, "特性別": str, "FileName": str, "CtrlName": str}
data_type = {"FAB_ID": str, "DEPARTMENT": str, "PROPERTY": str, "STATION": str, "FILE_NAME": str,
             "PARAMETER_ID": str, "target_val": float, "usl_val": float, "lsl_val": float, "ucl_val": float,
             "lcl_val": float, "FILE_ID": int, "CTRL_ID": int, "MODULE": str, "sample_size": int, "decimal_places": int,
             "units": str, "DEFAULT_CHART": int, "DEFAULT_CHART_NAME": str, "down_ucl_val": float, "down_cl_val": float,
             "down_lcl_val": float}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["FACTORY", "STATION", "課別", "特性別", "FILENAME", "FILEID", "CTRLNAME", "CTRLID", "管制分類",
                           "規格上限", "規格標準", "規格下限", "UAL", "AL", "LAL", "樣本大小", "小數位數", "單位",
                           "DEFAULTCHART", "預設管制圖", "等級", "CPK", "CIR", "TOPUCL", "TOPLCL", "DOWNUCL", "DOWNCL",
                           "DOWNLCL", "是否啟動自動管制界線計算", "是否啟動落實度警報", "落實度啟動時間", "檢驗頻率",
                           "應KEY組數"]
use_column_list = ["FAB_ID", "DEPARTMENT", "STATION", "PROPERTY", "PARAMETER_ID", "FILE_NAME", "FILE_ID", "CTRL_ID",
                   "target_val", "usl_val", "lsl_val", "ucl_val", "lcl_val", "MODULE", "sample_size", "decimal_places",
                   "units", "DEFAULT_CHART", "DEFAULT_CHART_NAME", "down_ucl_val", "down_cl_val", "down_lcl_val"]

replace_column_list = {"FACTORY": "FAB_ID", "課別": "DEPARTMENT", "特性別": "PROPERTY",
                       "CTRLNAME": "PARAMETER_ID",  "FILENAME": "FILE_NAME", "規格上限": "usl_val",
                       "規格標準": "target_val", "規格下限": "lsl_val", "TOPUCL": "ucl_val", "TOPLCL": "lcl_val",
                       "FILEID": "FILE_ID", "CTRLID": "CTRL_ID", "樣本大小": "sample_size", "小數位數": 'decimal_places',
                       "單位": "units", "DEFAULTCHART": "DEFAULT_CHART", "預設管制圖": "DEFAULT_CHART_NAME", "DOWNUCL": "down_ucl_val",
                       "DOWNCL": "down_cl_val", "DOWNLCL": "down_lcl_val"}


class SPCPlanIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def merge_data(self, df, replace_column_list):
        df = df.rename(columns=replace_column_list)
        df["FAB_ID"] = self.fab_folder
        mongo_conf = query_data_module.ConnectToMongo()
        db = mongo_conf.db
        # merge spc_plan_oqc
        colle_oqc = db["spc_plan_oqc_mapping"]
        query = {"FAB_ID": {"$in": df["FAB_ID"].unique().tolist()}}
        projection = {"_id": 0, "FAB_ID": 1, "FILE_NAME": 1, "PARAMETER_ID": 1, "MODULE": 1, "DEPARTMENT": 1, }
        spc_plan_oqc = pd.DataFrame(list(colle_oqc.find(query, projection)))
        df_merge = pd.merge(df, spc_plan_oqc, on=["FAB_ID", "FILE_NAME", "PARAMETER_ID", "DEPARTMENT"], how="left")
        return df_merge

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        df_merge = self.merge_data(df, replace_column_list)
        df_merge = df_merge[use_column_list]

        # final check column type is correct
        df = self.data_type_check(df_merge, data_type)
        self.log_csv_save_result(df, "spc_plan", filename)
        return df.shape[0]


process_data = SPCPlanIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)