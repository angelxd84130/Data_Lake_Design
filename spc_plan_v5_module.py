from idatamation_module import IdatamationFlow


class SPCPlanv5Idatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "SPC_plan_v5"
        self.type_dict = {"Factory": str, "Station": str, "FileName": str, "CtrlName": str, "料號": str}

        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"FACTORY": "FAB_ID", "STATION": "STEP", "料號": "PROD_ID_RAW",
                                    "CTRLNAME": "PARAMETER_ID","USL": "usl_val", "LSL": "lsl_val",
                                    "TOPUCL": "ucl_val", "TOPLCL": "lcl_val","FILENAME": "FILE_NAME"}
        self.data_type = {"FAB_ID": str, "STEP": str, "FILE_NAME": str, "PARAMETER_ID": str,"usl_val": float,
                          "lsl_val": float, "ucl_val": float, "lcl_val": float, "PROD_ID": str, "LOT_TYPE": str}

    def data_transformat(self, df, filename):
        df = df.rename(columns=self.replace_column_list)
        df["FAB_ID"] = self.fab_folder
        df = self.get_prodID_and_lotTYPE(df, 7, "standard")  # spc data has special rules
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df = self.data_type_check(df, self.data_type)
        key_col = {"FAB_ID", "PROD_ID_RAW", "STEP", "PARAMETER_ID", "FILE_NAME"}
        update_col = {"usl_val", "lsl_val", "ucl_val", "lcl_val"}
        self.mongo_insert_data(df, "spc_plan_v5", filename, key_col, update_col)
        return df.shape[0]


process_data = SPCPlanv5Idatamation()
process_data.main_function()