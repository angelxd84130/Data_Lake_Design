from data_process.data_mapping import DataMapping
from pymongo import UpdateOne
import pandas as pd
import logging
import math
import copy
import sys


class MSGroup(DataMapping):
    def __init__(self, wip_df: pd.DataFrame, db, mongo_remove, mongo_import, bulk_write):
        super().__init__()
        self.wip_df = wip_df.sort_values(by=['MOVE_IN_TIME'], ascending=True)
        self.move_in_time = self.wip_df.get('MOVE_IN_TIME')[0]
        self.move_out_time = self.wip_df.get('MOVE_OUT_TIME')[0]
        self.time_col = 'TIME'
        self.tbname = 'ms_original_lot'
        self.update_list = []
        self.db = db
        self.mongo_remove = mongo_remove
        self.mongo_import = mongo_import
        self.bulk_write = bulk_write

    def main_function(self):
        df_size = self.get_data()
        if df_size > 0:
            self.source_df[self.time_col] = self.source_df[self.time_col].dt.tz_localize("UTC")
            self.step_mapping()
            self.prod_mapping()
            self.transfer_compression_data()

    def slice_parameters_value(self, d, start, end):
        sliced = {}
        for key in d.keys():
            if end > 0:
                sliced[key] = d[key][start:end]
            else:
                sliced[key] = d[key][start:]
        return sliced

    def collect_signal(self, group):
        DOC_MAX_SIZE = 15728640  # 15MB
        non_signal_columns = [('FAB_ID', ''), ('EQP_ID', ''), ('STEP', ''), ('LOT_ID', ''), ('TIME', ''), ('PROD_ID', ''), ('LOT_TYPE', '')]
        group = group.sort_values(by="TIME").reset_index(drop=True)
        value_d = {}

        for col in group.columns:
            if col in non_signal_columns:
                continue
            value_d[col[1]] = group[col].tolist()

        d = {
            "FAB_ID": group["FAB_ID"][0],
            "STEP": group["STEP"][0],
            "EQP_ID": group["EQP_ID"][0],
            "LOT_ID": group["LOT_ID"][0],
            "PROD_ID": group["PROD_ID"][0],
            "LOT_TYPE": group["LOT_TYPE"][0],
            "TIME": group["TIME"].tolist(),
            "VALUE": value_d,
        }

        if sys.getsizeof(d) >= DOC_MAX_SIZE:
            num_of_copies = math.ceil(sys.getsizeof(d) / float(DOC_MAX_SIZE))
            offset = int(len(d["TIME"]) / num_of_copies)

            sliced_d = copy.deepcopy(d)
            for idx in range(0, len(d["TIME"]), offset):
                if (idx + offset) < len(d["TIME"]):
                    sliced_d["TIME"] = d["TIME"][idx: idx + offset]
                    sliced_d["VALUE"] = self.slice_parameters_value(
                        d["VALUE"], idx, idx + offset
                    )
                else:
                    sliced_d["TIME"] = d["TIME"][idx:]
                    sliced_d["VALUE"] = self.slice_parameters_value(d["VALUE"], idx, -1)

                self.group_document(d)
        else:
            self.group_document(d)

    def transfer_compression_data(self):
        ms_original_df = self.source_df
        if not ms_original_df.empty:
            group_data = ms_original_df.groupby(["FAB_ID", "STEP", "EQP_ID", "LOT_ID", "PROD_ID", "LOT_TYPE"])
            for group_idx in group_data.groups:
                temp = pd.pivot_table(group_data.get_group(group_idx),
                                      index=["FAB_ID", "EQP_ID", "STEP", "LOT_ID", "PROD_ID", "LOT_TYPE", "TIME"],
                                      columns=["PARAMETER_ID"],
                                      values=["VALUE"]).reset_index()

                self.collect_signal(temp)
            self.bulk_write(self.tbname, 'ms_lot', self.update_list)
        else:
            logging.warning("No ms_original_lot !!")

    def group_document(self, d: dict) -> None:
        query = {'FAB_ID': d.get('FAB_ID'),
                 'EQP_ID': d.get('EQP_ID'),
                 'LOT_ID': d.get('LOT_ID'),
                 'STEP': d.get('STEP'),
                 'PROD_ID': d.get('PROD_ID'),
                 'LOT_TYPE': d.get('LOT_TYPE')}
        set = {"$set": {"TIME": d.get('TIME'), "VALUE": d.get('VALUE')}}
        self.update_list.append(UpdateOne(query, set, upsert=True))
