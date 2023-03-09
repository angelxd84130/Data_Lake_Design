import logging
from datetime import datetime
from pymongo import MongoClient
import traceback
import sys

class ConnectToMongo:
    def __init__(self):
        self.os_path = "/home/mia/PycharmProjects/ETL_TEST/s3/"
        self.mongo_host = "192.168.8.161"
        self.mongo_port = 27018
        self.mongo_user = "admin"
        self.mongo_password = "admin"
        self.mongo_db = "ai"
        self.client = MongoClient(host=self.mongo_host, port=self.mongo_port, username=self.mongo_user,
                                          password=self.mongo_password, authSource="admin")
        self.db = self.client[self.mongo_db]

    def mongo_insert_log(self, date_time, fab_folder, data_source, filename, status, message=None, datarows=None,
                         insert_db_rows=None, is_success=None):
        collection_name = "idatamation_log"
        info_log_path = self.os_path + f"datasource/log/{fab_folder}/{data_source}/"
        log_dict = {"datetime": datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S"),
                    "fab": fab_folder, "datasource": data_source, "filename": filename,
                    "status": status, "message": message, "datarows": datarows, "isSuccess": is_success,
                    "dbrows": insert_db_rows}
        self.db[collection_name].insert_one(log_dict)

    def mongo_import(self, df, collection_name, fab_folder, data_source, filename):
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

    def mongo_update(self, query, new_values, collection_name, data_source, filename):
        try:
            if len(list(self.db[collection_name].find(query))) > 1:
                logging.warning(f"The conditions you query are not unique value !")

            self.db[collection_name].update_one(query, new_values)
            logging.info(f"{data_source}/{filename}, Update data Successful !!!")
            logging.info(f"Update data is {query}.")
        except Exception as e:
            logging.info(f"{data_source}/{filename}, Update data is failed ! The following is except error: ")
            logging.info(f"Failed data is {query}.")
            logging.info(e)

    def mongo_upsert(self, query, new_values, collection_name, data_source, filename):
        try:
            if len(list(self.db[collection_name].find(query))) > 1:
                logging.warning(f"The conditions you query are not unique value !")

            self.db[collection_name].update_many(query, new_values, upsert=True)
            logging.info(f"{data_source}/{filename}, Update data Successful !!!")
            logging.info(f"Update data is {query}.")
        except Exception as e:
            logging.info(f"{data_source}/{filename}, Update data is failed ! The following is except error: ")
            logging.info(f"Failed data is {query}.")
            logging.info(e)

    def bulk_write(self, data_source: str, target_collection: str, updates: list) -> None:
        try:
            self.db[target_collection].bulk_write(updates, ordered=False)
            print('success')
            logging.info(f"{data_source}->{target_collection}, Update data Successful !!!")
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_info = {
                'type': str(exc_type.__name__),
                'msg': str(exc_value),
                'info': repr(traceback.format_tb(exc_traceback)),
            }
            print(error_info)
            logging.info(f"{data_source}->{target_collection}, Update data is failed ! The following is except error: ")
            logging.info(e)