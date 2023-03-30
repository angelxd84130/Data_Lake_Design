import pandas as pd
from idatamation_module import IdatamationFlow


class OSTIdatamation(IdatamationFlow):
    def __init__(self):
        self.fab_folder = "S3"
        self.data_source = "OST"
        self.type_dict = {"廠別": str, "料號": str, "批號": str, "Layer": str, "站別": str, "EDID": str, "Parameter": str}
        super().__init__()
        # After import data, the first step is to capitalize column names.
        self.replace_column_list = {"廠別": "FAB_ID", "料號": "PROD_ID_RAW", "批號": "LOT_ID", "站別": "STATION",
                               "PARAMETER": "PARAMETER_ID", "TIMESTAMP": "TIME", }
        self.data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "TIME": object, "STEP": str,
                     "PARAMETER_ID": str, "VALUE": float, "PROD_ID": str, "LOT_TYPE": str, "LAYER": str, "STATION": str}

    def data_transformat(self, df, filename):
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
        df["TIMESTAMP"] = df["TIMESTAMP"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["廠別"] = df["廠別"].apply(lambda x: x.strip().replace("廠", ""))
        df["STEP"] = df["LAYER"].astype(str) + "-" + df["站別"].astype(str)
        df = df.rename(columns=self.replace_column_list)
        df = self.get_prodID_and_lotTYPE(df)
        df = df[list(self.data_type.keys())]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.data_type_check(df, self.data_type)
        key_col = {'FAB_ID', 'STEP', 'PROD_ID', 'LOT_ID', 'PARAMETER_ID', 'TIME'}
        update_col = {'VALUE', "PROD_ID_RAW", "LOT_TYPE", "LAYER", "STATION"}
        self.mongo_insert_data(df, "ost_lot", filename, key_col, update_col)
        return df.shape[0]

process_data = OSTIdatamation()
process_data.main_function()
