import pandas as pd
from idatamation_module import IdatamationFlow


fab_folder = "S3"
data_source = "WPG"
type_dict = {"廠別": str, "料號": str, "批號": str, "Layer": str, "站別": str, "Parameter": str}
data_type = {"FAB_ID": str, "PROD_ID_RAW": str, "LOT_ID": str, "TIME": object, "STEP": str,
             "PARAMETER_ID": str, "VALUE": int, "PROD_ID": str, "LOT_TYPE": str, "LAYER": str, "STATION": str}
# After import data, the first step is to capitalize column names.
column_name_format_list = ["廠別", "料號", "批號", "LAYER", "站別", "EDID", "PARAMETER", "VALUE", "TIMESTAMP"]
use_column_list = ["FAB_ID", "PROD_ID_RAW", "LOT_ID", "STEP", "PARAMETER_ID", "VALUE", "TIME", "LAYER", "STATION"]
replace_column_list = {"廠別": "FAB_ID", "料號": "PROD_ID_RAW", "批號": "LOT_ID", "站別": "STATION",
                       "PARAMETER": "PARAMETER_ID", "TIMESTAMP": "TIME", }


class WPGIdatamation(IdatamationFlow):
    def __init__(self, fab_folder, data_source):
        super().__init__(fab_folder, data_source)
        self.filename = None

    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        # TODO: Changing the time zone is not required.
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
        df["TIMESTAMP"] = df["TIMESTAMP"].dt.tz_localize("Etc/GMT-8").dt.tz_convert("UTC")
        df["廠別"] = df["廠別"].apply(lambda x: x.strip().replace("廠", ""))
        df["批號"] = df["批號"].apply(lambda x: x.strip())
        df["STEP"] = df["LAYER"].astype(str) + "-" + df["站別"].astype(str)
        df = df.rename(columns=replace_column_list)
        df = df[use_column_list]

        # final check column type is correct
        df["TIME"] = pd.to_datetime(df["TIME"])
        df = self.get_prodID_and_lotTYPE(df)
        df = self.data_type_check(df, data_type)
        key_col = {'FAB_ID', 'STEP', 'PROD_ID', 'LOT_ID', 'PARAMETER_ID', 'TIME'}
        update_col = {'VALUE', "PROD_ID_RAW", "LOT_TYPE", "LAYER", "STATION"}
        self.mongo_insert_data(df, "wpg_lot", filename, key_col, update_col)
        return df.shape[0]


process_data = WPGIdatamation(fab_folder, data_source)
process_data.main_function(column_name_format_list, replace_column_list, use_column_list, type_dict)
