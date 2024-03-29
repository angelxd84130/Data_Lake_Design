import pandas as pd
import query_data_module
from idatamation_module import IdatamationFlow


class SPCPlanIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "SPC_plan"
        self.type_dict = {"Factory": str, "Station": str, "課別": str, "特性別": str, "FileName": str, "CtrlName": str}
        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {
            "FACTORY": "FAB_ID", "課別": "DEPARTMENT", "特性別": "PROPERTY","CTRLNAME": "PARAMETER_ID", "單位": "units",
            "FILENAME": "FILE_NAME", "規格上限": "usl_val","規格標準": "target_val", "規格下限": "lsl_val",
            "TOPUCL": "ucl_val", "TOPLCL": "lcl_val", "FILEID": "FILE_ID", "CTRLID": "CTRL_ID", "樣本大小": "sample_size",
            "小數位數": 'decimal_places', "DEFAULTCHART": "DEFAULT_CHART", "預設管制圖": "DEFAULT_CHART_NAME",
            "DOWNUCL": "down_ucl_val", "DOWNCL": "down_cl_val", "DOWNLCL": "down_lcl_val"}

        self.data_type = {
            "FAB_ID": str, "DEPARTMENT": str, "PROPERTY": str, "STATION": str, "FILE_NAME": str, "PARAMETER_ID": str,
            "target_val": float, "usl_val": float, "lsl_val": float, "ucl_val": float, "lcl_val": float, "FILE_ID": int,
            "CTRL_ID": int, "MODULE": str, "sample_size": int, "decimal_places": int, "units": str, "DEFAULT_CHART": int,
            "DEFAULT_CHART_NAME": str, "down_ucl_val": float, "down_cl_val": float, "down_lcl_val": float}

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

    def data_transformat(self, df, filename):
        df_merge = self.merge_data(df, self.replace_column_list)
        df_merge = df_merge[list(self.data_type.keys())]

        # final check column type is correct
        df = self.data_type_check(df_merge, self.data_type)
        key_col = {"FAB_ID", "DEPARTMENT", "STATION", "PROPERTY", "PARAMETER_ID", "FILE_NAME", "FILE_ID", "CTRL_ID",}
        update_col = {"target_val", "usl_val", "lsl_val", "ucl_val", "lcl_val", "MODULE", "sample_size", "decimal_places",
                   "units", "DEFAULT_CHART", "DEFAULT_CHART_NAME", "down_ucl_val", "down_cl_val", "down_lcl_val"}
        self.mongo_insert_data(df, "spc_plan", filename, key_col, update_col)
        return df.shape[0]


process_data = SPCPlanIdatamation()
process_data.main_function()