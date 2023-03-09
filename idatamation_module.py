import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import query_data_module
import traceback
import sys


class IdatamationFlow:
    def __init__(self, fab_folder, data_source):
        self.fab_folder = fab_folder
        self.data_source = data_source
        self.os_path = "/home/mia/PycharmProjects/ETL_TEST/s3/"
        self.raw_data_path = self.os_path + f"/{data_source}/"
        self.today_date = datetime.now().strftime("%Y%m%d")
        self.backup_path = self.os_path + f"/{data_source}/"

    def get_data_from_nas(self):
        filename_list = os.listdir(self.raw_data_path)
        filename_list = sorted(filename_list, reverse=True)
        filename_list = filename_list[0:10]  # To do last 3 files of once
        return filename_list


    def data_transformat(self, df, filename, replace_column_list, use_column_list):
        pass

    def data_type_check(self, df, data_type: dict):
        for col in data_type:
            df[col] = df[col].astype(data_type[col]).where(df[col].notnull(), None)
        return df

    def get_prodID_and_lotTYPE (self, df, limit_size=8, lot_type=None):
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

    def log_csv_save_result(self, df, collection_name, filename):
        mongo_conf = query_data_module.ConnectToMongo()
        log_text = mongo_conf.mongo_import(df, collection_name, self.fab_folder, self.data_source, filename)
        print(log_text)


    def main_function(self, column_format_list, replace_column_list, use_column_list, type_dict):
        mongo_conf = query_data_module.ConnectToMongo()
        filename_list = self.get_data_from_nas()
        df_count = 0
        for filename in filename_list:
            # move data from customer_raw_data folder to backup folder

            # check file name
            filename_pass = filename
            if filename_pass:
                encoding_pass = filename

            if not filename_pass or not encoding_pass:
                continue
            else:
                log_text = ''
                try:
                    df = pd.read_csv(self.raw_data_path + filename, encoding="big5", dtype=type_dict, encoding_errors='ignore')
                    df = df.rename(columns=lambda x: x.strip().upper())
                    log_text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {self.data_source}/{filename} " + \
                               f"The number of data row is {df.shape[0]}!\n"
                    column_format_pass = True
                except ValueError as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    error_info = {
                        'type': str(exc_type.__name__),
                        'msg': str(exc_value),
                        'info': repr(traceback.format_tb(exc_traceback)),
                    }
                    print(error_info)
                    log_text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {self.data_source}/{filename} " + \
                               "column type is not correct!\n \n"
                    column_format_pass = False
                finally:
                    print(log_text)

                if not column_format_pass:
                    continue
                else:
                    try:
                        df_rows = self.data_transformat(df, filename, replace_column_list, use_column_list)
                        df_count += df_rows
                        log_text = f"Accumulation DataFrame Count: {df_count}.\n"
                        print(log_text)

                    except ValueError as e:
                        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        error_info = {
                            'type': str(exc_type.__name__),
                            'msg': str(exc_value),
                            'info': repr(traceback.format_tb(exc_traceback)),
                        }
                        print(error_info)
                        #os.system(f"cp {self.backup_path + filename} {self.error_path + 'other/'}")
                        log_text = f"[{date_time}][Value Error]: Column type is not correct !\n" + \
                                   f"[Error]:{e}"
                        mongo_conf.mongo_insert_log(date_time, self.fab_folder, self.data_source, filename,
                                                    status="Value Error",
                                                    message=f"{self.data_source}/{filename} is transmitted to "
                                                            f"Error Folder 'other'.\n" + f"[Error]:{e}",
                                                    datarows=df.shape[0],
                                                    is_success=False)
                        print(log_text)


                    except Exception as e:
                        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        error_info = {
                            'type': str(exc_type.__name__),
                            'msg': str(exc_value),
                            'info': repr(traceback.format_tb(exc_traceback)),
                        }
                        print(error_info)
                        log_text = f"[{date_time}][Other Error]: {e}\n"
                        mongo_conf.mongo_insert_log(date_time, self.fab_folder, self.data_source, filename,
                                                    status="Other Error",
                                                    message=f"{self.data_source}/{filename} is transmitted to "
                                                            f"Error Folder 'other'.\n" + f"[Error]:{e}",
                                                    datarows=df.shape[0],
                                                    is_success=False)
                        print(log_text)





