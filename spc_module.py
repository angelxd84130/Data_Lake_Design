import pandas as pd
from idatamation_module import IdatamationFlow
from data_process.spc_group import SPCCompression


class SPCIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "SPC"
        self.type_dict = {"Fab": str, "Station": str, "Department": str, "Filename": str, "Product": str, "Lot": str,
                          "LAYER": str, "STEP": str, "Parameter": str, "PROPERTY": str, "EquipmentID": str,
                          "Value": float, "Time": str, "DataType": str}
        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"FAB": "FAB_ID", "PRODUCT": "PROD_ID_RAW", "LOT": "LOT_ID", "FILENAME": "FILE_NAME",
                                    "PARAMETER": "PARAMETER_ID", "EQUIPMENTID": "EQP_ID", "FILEID": "FILE_ID",
                                    "CTRLID": "CTRL_ID"}
        self.data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "TIME": object, "STEP": str,
                     "PARAMETER_ID": str, "VALUE": float, "EQP_ID": str, "PROPERTY": str, "STATION": str,
                     "DEPARTMENT": str, "FILE_NAME": str, "FILE_ID": int, "CTRL_ID": int, "PROD_ID": str,
                     "LOT_TYPE": str, "LAYER": str}

    def data_transformat(self, df, filename):
        df["TIME"] = pd.to_datetime(df["TIME"])
        df["TIME"] = df["TIME"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["FAB"] = self.fab_folder
        df["STEP"] = df["LAYER"].astype(str) + "-" + df["STEP"].astype(str)
        df = df.rename(columns=self.replace_column_list)
        df = self.get_prodID_and_lotTYPE(df, 7, "standard")  # spc data has special rules
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])

        df = self.data_type_check(df, self.data_type)
        self.mongo_insert_data(df, "spc_original_lot", filename, set(), set(), overlay_data=True)
        compress_data = SPCCompression(df, self.db, self.mongo_remove, self.mongo_import, self.bulk_write)
        compress_data.main_function()
        return df.shape[0]

process_data = SPCIdatamation()
process_data.main_function()
