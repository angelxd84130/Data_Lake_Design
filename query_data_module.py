import pandas as pd
import logging
from datetime import datetime
from pymongo import MongoClient

class ConnectToMongo:
    def __init__(self):
        self.os_path = "/home/mia/PycharmProjects/ETL_TEST/s3/"
        self.mongo_host = "192.168.8.161"
        self.mongo_port = 27018
        self.mongo_user = "admin"
        self.mongo_password = "admin"
        self.mongo_db = "prd_unimicron"
        self.client = MongoClient(host=self.mongo_host, port=self.mongo_port, username=self.mongo_user,
                                          password=self.mongo_password, authSource="admin", connect=False)
        self.db = self.client[self.mongo_db]

    def mongo_insert_log(self, date_time, fab_folder, data_source, filename, status, message=None, datarows=None,
                         insert_db_rows=None, is_success=None):
        collection_name = "idatamation_log"
        log_dict = {"datetime": datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S"),
                    "fab": fab_folder, "datasource": data_source, "filename": filename,
                    "status": status, "message": message, "datarows": datarows, "isSuccess": is_success,
                    "dbrows": insert_db_rows}
        self.db[collection_name].insert_one(log_dict)

    def mongo_import(self, df: pd.DataFrame, collection_name: str, fab_folder: str, data_source: str, filename: str):
        df_json = df.to_dict("records")
        log_text = None
        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            mongo_insert_result = self.db[collection_name].insert_many(df_json)
            db_rows = len(mongo_insert_result.inserted_ids)
            log_text = f"[{date_time}] {data_source}/{filename} " + \
                       f"A total of {db_rows} insert to MongoDB !!!\n"
            if filename:
                self.mongo_insert_log(date_time, fab_folder, data_source, filename,
                                      status="Insert DB Success", is_success=True,
                                      insert_db_rows=db_rows, datarows=df.shape[0])
        except Exception as e:
            log_text = f"[{date_time}] {data_source}/{filename} " + \
                       "Insert to MongoDB is failed ! The following is except error: " + f"{e}\n"
        finally:
            return log_text

    def mongo_remove(self, collection_name, query):
        log_text = None
        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            mongo_remove_result = self.db[collection_name].delete_many(query)
            db_rows = mongo_remove_result.deleted_count
            log_text = f"[{date_time}] {collection_name} " + \
                       f"A total of {db_rows} delete to MongoDB !!!\n"
        except Exception as e:
            log_text = f"[{date_time}] {collection_name} " + \
                       "Delete to MongoDB is failed ! The following is except error: " + f"{e}\n"
        finally:
            return log_text

    def bulk_write(self, data_source: str, target_collection: str, updates: list) -> None:
        try:
            self.db[target_collection].bulk_write(updates, ordered=False)
            logging.info(f"{data_source}->{target_collection}, Update data Successful !!!")
        except Exception as e:
            logging.info(f"{data_source}->{target_collection}, Update data is failed ! The following is except error: ")
            logging.info(e)

