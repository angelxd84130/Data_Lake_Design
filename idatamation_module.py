import pandas as pd
import os
from datetime import datetime
from pymongo import UpdateOne
import numpy as np
from query_data_module import ConnectToMongo
import traceback
import sys


class IdatamationFlow(ConnectToMongo):
    def __init__(self):
        super().__init__()
        self.os_path = "/home/mia/PycharmProjects/ETL_TEST/s3/"
        self.raw_data_path = self.os_path + f"/{self.data_source}/"
        self.today_date = datetime.now().strftime("%Y%m%d")

    def _get_data_from_nas(self):
        filename_list = os.listdir(self.raw_data_path)
        filename_list = sorted(filename_list, reverse=False)
        filename_list = filename_list[0:3]  # To do last 3 files of once
        return filename_list

    def data_transformat(self, df, filename):
        pass

    def data_type_check(self, df: pd.DataFrame, data_type: dict) -> pd.DataFrame:
        for col in data_type:
            df[col] = df[col].astype(data_type[col]).where(df[col].notnull(), None)
        return df

    def get_prodID_and_lotTYPE (self, df: pd.DataFrame, limit_size=8, lot_type=None) -> pd.DataFrame:
        """
        function for unimicron special needs
        the front 7 characters mean PROD unique ID.
        the 8th character means the PROD version: {'T', 'X', 'Y', 'Z'} are the test versions, and the others are standard.
        * (spc data don't have the 8th character, so the default lot_type is always standard)
        """
        df["PROD_ID"] = df["PROD_ID_RAW"].str[:limit_size]
        if lot_type == None:
            df['LOT_TYPE'] = np.where(df['PROD_ID'].str[-1].isin(['T', 'X', 'Y', 'Z']), "test", "standard")
        else:
            df['LOT_TYPE'] = lot_type
        return df

    def _package_data(self, df: pd.DataFrame, key_col: set, update_col: set) -> list:
        update_list = []
        for index, row in df.iterrows():
            query = {key: row.get(key) for key in key_col}
            set = {"$set": {key: row.get(key) for key in update_col}}
            update_list.append(UpdateOne(query, set, upsert=True))
        return update_list

    def mongo_insert_data(self, df: pd.DataFrame, collection_name: str, filename: str,
                          key_col: set, update_col: set, overlay_data=False) -> None:
        if overlay_data:  # 大量重複資料
            self.mongo_import(df, collection_name, self.fab_folder, self.data_source, filename)
        else:
            update_list = self._package_data(df, key_col, update_col)
            self.bulk_write(self.fab_folder+filename, collection_name, update_list)

    def _load_data(self, reader, filename):
        df_count = 0
        while True:
            try:
                df = reader.get_chunk(1000000)  # each chunk has 100w row data
                df = df.rename(columns=lambda x: x.strip().upper())
                # transform data & write in mongoDB
                df_rows = self.data_transformat(df, filename)
                df_count += df_rows
                print(df_count)

            except StopIteration:  # end of file
                print('end')
                break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                error_info = {
                    'type': str(exc_type.__name__),
                    'msg': str(exc_value),
                    'info': repr(traceback.format_tb(exc_traceback)),
                }
                print(error_info)
                break
        log_text = f"Accumulation DataFrame Count: {df_count}.\n"
        print(log_text)
        return df_count

    def main_function(self):
        filename_list = self._get_data_from_nas()
        for filename in filename_list:

            # check file name
            filename_pass = True
            date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not filename_pass:
                continue
            try:
                reader = pd.read_csv(self.os_path + self.data_source + '/' + filename, encoding="big5", dtype=self.type_dict, iterator=True)
                df_count = self._load_data(reader, filename)
                log_text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {self.data_source}/{filename} " + \
                           f"The number of data row is {df_count}!\n"

            except UnicodeDecodeError:  # not encode in big5

                log_text = f"[{date_time}][File Encoding Error]: " + \
                           f"{self.data_source}/{filename} is transmitted to Error Folder 'encode'.\n" + \
                           f"Probability encoding is {['encoding']} and confidence is {['confidence']}.\n"


            except pd.errors.ParserError:  # file spilite by comma
                log_text = f"[{date_time}][File Encoding Error]: " + \
                           f"{self.data_source}/{filename} is transmitted to Error Folder the 'encode'.\n" + \
                           "the file has comma or the number of data's column is not equal in every row !\n"


            except ValueError as e:  # column type error
                log_text = f"[{date_time}][Value Error]: Column type is not correct !\n" + \
                           f"[Error]:{e}"


            except Exception as e:  # other error
                date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                log_text = f"[{date_time}][Other Error]: {e}\n"
                exc_type, exc_value, exc_traceback = sys.exc_info()
                error_info = {
                    'type': str(exc_type.__name__),
                    'msg': str(exc_value),
                    'info': repr(traceback.format_tb(exc_traceback)),
                }
                print(error_info)

            print(log_text)
