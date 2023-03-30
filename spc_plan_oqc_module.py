from idatamation_module import IdatamationFlow


class SPCPlanOQCIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "SPC_plan_oqc_mapping"
        self.type_dict = {"V用": str, "Station": str, "FileName": str, "CtrlName": str, "模組": str, "課別": str}

        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"FILENAME": "FILE_NAME", "CTRLNAME": "PARAMETER_ID", "模組": "MODULE", "課別": "DEPARTMENT"}
        self.data_type = {"FAB_ID": str, "FILE_NAME": str, "PARAMETER_ID": str, "MODULE": str, "DEPARTMENT": str}

    def data_transformat(self, df, filename):
        df["FAB_ID"] = self.fab_folder
        df = df.rename(columns=self.replace_column_list)
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df = self.data_type_check(df, self.data_type)
        key_col = {"FAB_ID", "FILE_NAME", "PARAMETER_ID", "DEPARTMENT"}
        update_col = {"MODULE"}
        self.mongo_insert_data(df, "spc_plan_oqc_mapping", filename, key_col, update_col)
        return df.shape[0]


process_data = SPCPlanOQCIdatamation()
process_data.main_function()