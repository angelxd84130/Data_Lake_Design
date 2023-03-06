import logging
from datetime import datetime
import pymongo


class ConnectToMongo:
    def __init__(self):
        self.os_path = "/home/mia/PycharmProjects/ETL_TEST/s3/"
        self.mongo_host = "192.168.8.161"
        self.mongo_port = 27018
        self.mongo_user = "admin"
        self.mongo_password = "admin"
        self.mongo_db = "ai"
        self.client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port, username=self.mongo_user,
                                          password=self.mongo_password, authSource="admin")
        self.db = self.client[self.mongo_db]

    def mongo_insert_log(self, date_time, fab_folder, data_source, filename, status, message=None, datarows=None,
                         insert_db_rows=None, is_success=None):
        mongo_collection = "idatamation_log"
        info_log_path = self.os_path + f"datasource/log/{fab_folder}/{data_source}/"
        colle = self.db[mongo_collection]

        log_dict = {"datetime": datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S"),
                    "fab": fab_folder, "datasource": data_source, "filename": filename,
                    "status": status, "message": message, "datarows": datarows, "isSuccess": is_success,
                    "dbrows": insert_db_rows}
        # insert to mongo db
        colle.insert_one(log_dict)
        # # output to log file
        # with open(info_log_path + f"{date_time.strftime('%Y%m%d')}.log", "a") as f:
        #     f.write(str(log_dict) + "\n")

    def mongo_import(self, df, collection_name, fab_folder, data_source, filename):
        df_json = df.to_dict("records")
        mongo_collection = collection_name
        colle = self.db[mongo_collection]
        log_text = None
        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            mongo_insert_result = colle.insert_many(df_json)
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
        mongo_collection = collection_name
        colle = self.db[mongo_collection]

        try:
            if len(list(colle.find(query))) > 1:
                logging.warning(f"The conditions you query are not unique value !")

            colle.update_one(query, new_values)
            logging.info(f"{data_source}/{filename}, Update data Successful !!!")
            logging.info(f"Update data is {query}.")
        except Exception as e:
            logging.info(f"{data_source}/{filename}, Update data is failed ! The following is except error: ")
            logging.info(f"Failed data is {query}.")
            logging.info(e)
