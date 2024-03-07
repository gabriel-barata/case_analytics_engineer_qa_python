from gzip import GzipFile
from io import BytesIO 

import boto3
import pandas as pd
import pandasql as ps
import json
import psycopg2
from datetime import datetime

from unidecode import unicode


class MicroServiceQuintoAndar:

    @staticmethod
    def get_connection(db_type, host, user, pwd, db, port, encoding, timeout=0):

        if db_type == 'postgres':
            p = port
            conn = psycopg2.connect(host=host, user=user, password=pwd, database=db, port=p)

            print('setting client encoding to {}'.format(encoding))
            conn.set_client_encoding(encoding)

            if timeout:
                conn.cursor().execute("set statement_timeout = {}".format(timeout))

            return conn

        else:
            return None

    def elt_pipeline(self, table_name, dw_query, bucket, file_path, access_key, secret_key,
        db_type, host, user, pwd, db, port, encoding, timeout=0,
        batch_load=True, incremental_load=False):

        conn = self.get_connection(db_type, host, user, pwd, db, port, encoding, timeout)
        client = MicroServicePostgresCLient(conn)

        if batch_load:
            json_list = client.get_batch_data_from_table_name(table_name)
        elif incremental_load:
            json_list = client.get_incremental_data_from_table_name(table_name)

        for json_ in json_list:
            if json_["password"] == "quintoandar":
                json_["password"] = "*****"

        s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        gz_body = BytesIO()
        for _json in json_list:
            with GzipFile(filebody=gz_body, mode="w") as fp:
                fp.write(json.dumps(_json, ensure_ascii=False, cls=UnidecodeHandler)).encode("utf-8")
                fp.write("/n")

        s3.Bucket(bucket).put_object(Body=gz_body.getvalue(), Key=f"bronze/{file_path}")

        df = pd.DataFrame.from_records(json_list)
        df.to_parquet(f"s3://{bucket}/silver/{file_path}.parquet.gzip", compression="gzip")

        df_dw = ps.sqldf(dw_query, locals())
        df_dw.to_parquet(f's3://{bucket}/dw/{file_path}.parquet.gzip', compression="gzip")