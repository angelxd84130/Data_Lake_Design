import pandas as pd
from idatamation_module import IdatamationFlow

fab_folder = "S3"
data_source = "IPQC"
type_dict = {"PRDTY": str, "PRTNO": str, "LOTNO": str, "LayerName": str, "LOCATION": str, "DFCOD": str, "DENME": str}
data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "TIME": object, "STEP": str,
             "PARAMETER_ID": str, "VALUE": int, "PROD_ID": str, "LOT_TYPE": str, "LAYER": str, "STATION": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["PRDTY", "STPNO", "PRTNO", "LOTNO", "LAYERNAME", "LOCATION", "KDATE", "DFCOD", "DENME",
                           "QUNTY", "DFTYPE", "DAREA"]
use_column_list = ["FAB_ID", "PROD_ID_RAW", "LOT_ID", "STEP", "TIME", "PARAMETER_ID", "VALUE", "LAYER", "STATION"]
replace_column_list = {"PRDTY": "FAB_ID", "PRTNO": "PROD_ID_RAW", "LOTNO": "LOT_ID", "QUNTY": "VALUE",
                       "KDATE": "TIME", "LAYERNAME": "LAYER", "LOCATION": "STATION"}


class IPQCIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        df["KDATE"] = pd.to_datetime(df["KDATE"])
        df["KDATE"] = df["KDATE"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["STEP"] = df["LAYERNAME"].astype(str).apply(lambda s: s.strip()) + "-" + df["LOCATION"].astype(str)
        df["PARAMETER_ID"] = df["DFCOD"].astype(str) + "-" + df["DENME"].astype(str)
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.get_prodID_and_lotTYPE(df)
        df = self.data_type_check(df, data_type)
        key_col = {'FAB_ID', 'STEP', 'PROD_ID', 'LOT_ID', 'PARAMETER_ID', 'TIME'}
        update_col = {'VALUE'}
        self.mongo_insert_data(df, "ipqc_lot", filename, key_col, update_col)
        return df.shape[0]


process_data = IPQCIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)
