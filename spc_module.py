import pandas as pd
from idatamation_module import IdatamationFlow
from data_process.spc_group import SPCCompression

fab_folder = "S3"
data_source = "SPC"
type_dict = {"Fab": str, "Station": str, "Department": str, "Filename": str, "Product": str, "Lot": str, "LAYER": str,
             "STEP": str, "Parameter": str, "PROPERTY": str, "EquipmentID": str}
data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "TIME": object, "STEP": str,
             "PARAMETER_ID": str, "VALUE": int, "EQP_ID": str, "PROPERTY": str, "STATION": str,
             "DEPARTMENT": str, "FILE_NAME": str, "FILE_ID": int, "CTRL_ID": int, "PROD_ID": str, "LOT_TYPE": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["FAB", "STATION", "DEPARTMENT", "FILENAME", "FILEID", "PRODUCT", "LOT", "LAYER", "STEP",
                           "PARAMETER", "CTRLID", "VALUE", "TIME", "PROPERTY", "DATATYPE", "EQUIPMENTID"]
use_column_list = ["FAB_ID", "PROD_ID_RAW", "LOT_ID", "STEP", "PARAMETER_ID", "VALUE", "TIME", "EQP_ID", "PROPERTY",
                   "STATION", "DEPARTMENT", "FILE_NAME", "FILE_ID", "CTRL_ID", "LAYER"]
replace_column_list = {"FAB": "FAB_ID", "PRODUCT": "PROD_ID_RAW", "LOT": "LOT_ID", "PARAMETER": "PARAMETER_ID",
                       "EQUIPMENTID": "EQP_ID", "FILENAME": "FILE_NAME", "FILEID": "FILE_ID", "CTRLID": "CTRL_ID"}


class SPCIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        df["TIME"] = pd.to_datetime(df["TIME"])
        df["TIME"] = df["TIME"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["FAB"] = self.fab_folder
        df["STEP"] = df["LAYER"].astype(str) + "-" + df["STEP"].astype(str)
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.get_prodID_and_lotTYPE(df, 7, "standard")  # spc data has special rules
        df = self.data_type_check(df, data_type)
        self.mongo_insert_data(df, "spc_original_lot", filename, set(), set(), duplicated_data=True)
        self.dataframe = df
        return df.shape[0]

process_data = SPCIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)
compress_data = SPCCompression(process_data.dataframe)
compress_data.main_function()