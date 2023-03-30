import pandas as pd
from idatamation_module import IdatamationFlow


class IPQCIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "IPQC"
        self.type_dict = {"PRDTY": str, "PRTNO": str, "LOTNO": str, "LayerName": str, "LOCATION": str, "DFCOD": str,
                          "DENME": str}
        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"PRDTY": "FAB_ID", "PRTNO": "PROD_ID_RAW", "LOTNO": "LOT_ID", "QUNTY": "VALUE",
                                    "KDATE": "TIME", "LAYERNAME": "LAYER", "LOCATION": "STATION"}
        self.data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "TIME": object, "STEP": str, "VALUE": float,
                          "PARAMETER_ID": str, "PROD_ID": str, "LOT_TYPE": str, "LAYER": str, "STATION": str}


    def data_transformat(self, df, filename):
        df["KDATE"] = pd.to_datetime(df["KDATE"])
        df["KDATE"] = df["KDATE"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["STEP"] = df["LAYERNAME"].astype(str).apply(lambda s: s.strip()) + "-" + df["LOCATION"].astype(str)
        df["PARAMETER_ID"] = df["DFCOD"].astype(str) + "-" + df["DENME"].astype(str)
        df = df.rename(columns=self.replace_column_list)
        df = self.get_prodID_and_lotTYPE(df)
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.data_type_check(df, self.data_type)
        key_col = {'FAB_ID', 'STEP',  'PROD_ID_RAW', 'LOT_ID', 'PARAMETER_ID', 'TIME'}
        update_col = {'VALUE', "PROD_ID", "LOT_TYPE", "LAYER", "STATION"}
        self.mongo_insert_data(df, "ipqc_lot", filename, key_col, update_col)
        return df.shape[0]


process_data = IPQCIdatamation()
process_data.main_function()
