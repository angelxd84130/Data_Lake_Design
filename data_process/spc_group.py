from data_process.data_mapping import DataMapping
import pandas as pd
import logging


class SPCGroup(DataMapping):
    def __init__(self, wip_df: pd.DataFrame):
        super().__init__()
        self.wip_df = wip_df.sort_values(by=['MOVE_IN_TIME'], ascending=True)
        self.move_in_time = self.wip_df.get('MOVE_IN_TIME')[0]
        self.move_out_time = self.wip_df.get('MOVE_OUT_TIME')[0]

    def main_function(self) -> None:
        df_size = self.get_data('spc_original_lot')
        if df_size > 0:
            self.source_df['TIME'] = self.source_df['TIME'].dt.tz_localize("UTC")
            self.step_eqp_mapping()
            self.source_df = self.source_df.drop(['PROD_ID', 'LOT_TYPE'], axis=1)
            self.prod_mapping()
            self.source_df['PROD_ID'] = self.source_df['PROD_ID'].str[:7]
            self.source_df['LOT_TYPE'] = 'standard'
            self.transfer_compression_data()

    def step_eqp_mapping(self) -> None:
        # spc: lot + time --> step + eqp
        mask = (self.source_df['LOT_ID'].notnull())
        target_df = self.source_df[mask]
        target_df = target_df.drop(['STEP', 'EQP_ID'], axis=1)
        wip_df = self.wip_df[['FAB_ID', 'EQP_ID', 'LOT_ID', 'STEP', 'MOVE_IN_TIME', 'MOVE_OUT_TIME']]
        target_df = pd.merge(target_df, wip_df, on=['FAB_ID', 'LOT_ID'])

        mask = (target_df['TIME'] >= target_df['MOVE_IN_TIME']) & (target_df['TIME'] <= target_df['MOVE_OUT_TIME'])
        target_df = target_df[mask].drop(['MOVE_IN_TIME', 'MOVE_OUT_TIME'], axis=1)
        self.source_df = target_df

    def transfer_compression_data(self) -> None:
        self.source_df = self.source_df.dropna(subset=['LOT_ID', 'STEP', 'EQP_ID'])
        if len(self.source_df) < 1:
            log_text = f"no spc_original_lot data to transfer"
            logging.info(log_text)
            return

        target_colle = self.db["spc_lot"]
        checkList = self.source_df.drop_duplicates(
            subset=['FAB_ID', 'STATION', 'DEPARTMENT', 'FILE_ID', 'CTRL_ID', 'TIME'])
        for index, row in checkList.iterrows():
            (value, std, step, eqp, prod, lot, parameter, num) = self._get_value(row)
            target_colle.update_one(
                {"FAB_ID": row["FAB_ID"], "PROD_ID": prod, "STEP": step,
                 "EQP_ID": eqp, "LOT_ID": lot, "PARAMETER_ID": parameter,
                 "STATION": row["STATION"], "PROPERTY": row["PROPERTY"], "DEPARTMENT": row["DEPARTMENT"],
                 "FILE_NAME": row["FILE_NAME"], "FILE_ID": row["FILE_ID"], "CTRL_ID": row["CTRL_ID"],
                 "TIME": row['TIME'], "LOT_TYPE": "standard"},
                {"$set": {"VALUE": value, "STD": std, "VALUES": num}}, upsert=True
            )
        log_text = f"[UPDATE STEP]: the total of rows is {checkList.shape[0]}"
        logging.info(log_text)

    def _get_value(self, row_data: pd.DataFrame) -> (float, float):
        df = row_data
        mean = round(df['VALUE'].mean(), 6)
        std = round(df['VALUE'].std(), 6)
        step = df[['STEP']].sort_values(by=['STEP']).loc[0, 'STEP']
        eqp = df[['EQP_ID']].sort_values(by='EQP_ID').loc[0, 'EQP_ID']
        prod = df[['PROD_ID']].sort_values(by='PROD_ID').loc[0, 'PROD_ID']
        lot = df[['LOT_ID']].sort_values(by='LOT_ID').loc[0, 'LOT_ID']
        parameter = df[['PARAMETER_ID']].sort_values(by='PARAMETER_ID').loc[0, 'PARAMETER_ID']
        num = round(df[['VALUE']], 6).values.flatten().tolist()
        return (mean, std, step, eqp, prod, lot, parameter, num)