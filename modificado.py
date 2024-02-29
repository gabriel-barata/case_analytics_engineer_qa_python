from unidecode import unidecode
from loguru import logger
import pandasql as ps
import pandas as pd
import psycopg2
import boto3

from functools import wraps
from gzip import GzipFile
from io import BytesIO
import json

from client import MicroServicePostgresClient


class MicroServiceQuintoAndar:

    def __init__(
        self,
        *args,
        **kwargs
    ):

        self.conn = self._get_connection(**kwargs)
        self.s3 = self._get_s3_interface(**kwargs)

    def logger_decorator(func):
        """
        Essa função serve como decorator para outros métodos
        ela captura os logs e os retorna no output padrão
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f'calling function {func.__name__} with args {args}')
            try:
                result = func(*args, **kwargs)
                logger.info(f'function {func.__name__} returned {result}')
                return result
            except Exception as e:
                logger.exception(f'exception in {func.__name__}: {e}')
        return wrapper

    @staticmethod
    @logger_decorator
    def _get_connection(*args, **kwargs):
        """
        Esse método estático gera uma conexão com o banco de dados
        checa se o mesmo é postgres e define um timeout padrão para
        a conexão
        """
        if kwargs.get('db_type') == 'postgres':
            host = kwargs.get('host')
            user = kwargs.get('user')
            pwd = kwargs.get('pwd')
            db = kwargs.get('db')
            port = int(kwargs.get('port'))
            encoding = kwargs.get('encoding')
            timeout = kwargs.get('timeout', 0)

            conn = psycopg2.connect(
                host=host, user=user, password=pwd, database=db, port=port)

            conn.set_client_encoding(encoding)

            if timeout:
                conn.cursor().execute(
                    "set statement_timeout = {}".format(timeout))

            return conn

        else:
            return None

    @staticmethod
    @logger_decorator
    def _get_s3_interface(*agrs, **kwargs):
        """
        Esse método retorna uma interface de comunicação com 
        o Amazon S3 a partir da lib boto3
        """
        s3 = boto3.resource(
            's3', aws_access_key_id=kwargs.get('access_key'),
            aws_secret_access_key=kwargs.get('secret_key'))

        return s3

    @logger_decorator
    def _fetch_data(self, batch_load: True, table_name: str):
        """
        Este método captura os dados da fonte, faz uma pequena tratativa 
        no campo 'password' e os retorna em uma lista
        """
        client = MicroServicePostgresClient(self.conn)

        if batch_load:
            json_list = client.get_batch_data_from_table_name(
                table_name)
        else:
            json_list = client.get_incremental_data_from_table_name(
                table_name)

        for json_unit in json_list:
            if json_unit["password"] == "quintoandar":
                json_unit["password"] = "*****"

        return json_list

    @logger_decorator
    def _insert_to_s3_bronze_layer(
        self,
        json_list: list[str],
        bucket: str,
        file_path: str
    ):
        """
        Este método cria uma variável armazenada em memória com os dados
        recebidos e insere esta mesma variável como um arquivo no bucket s3
        """

        gz_body = BytesIO()
        for json_unit in json_list:
            with GzipFile(filebody=gz_body, mode="w") as fp:
                fp.write(json.dumps(
                    json_unit, ensure_ascii=False, cls=UnidecodeHandler)
                    ).encode("utf-8")
                fp.write("/n")

        self.s3.Bucket(bucket).put_object(
            Body=gz_body.getvalue(), Key=f"bronze/{file_path}")

    @logger_decorator
    def _insert_to_s3_silver_layer(
        self,
        json_list: list[str],
        bucket: str,
        file_path: str
    ):
        """
        Este método cria um dataframe a partir de uma lista strings
        formatadas como json e o insere em formato parquet em um bucket S3
        """
        df = pd.DataFrame.from_records(json_list)
        df.to_parquet(
            f"s3://{bucket}/silver/{file_path}.parquet.gzip",
            compression="gzip"
            )

    @logger_decorator
    def _insert_to_s3_dw_layer(
        dw_query: str,
        bucket: str,
        file_path: str
    ):
        """
        Este método cria um dataframe pandas a partir de uma consulta SQL
        e o insere em um bucket S3
        """

        df_dw = ps.sqldf(dw_query, locals())
        df_dw.to_parquet(
            f's3://{bucket}/dw/{file_path}.parquet.gzip', compression="gzip")

    @logger_decorator
    def elt_pipeline(
        self, json_list: list[str], dw_query: str, table_name: str,
        bucket: str, file_path: str, batch_load: bool = True
    ):
        """
        O método que de fato realiza o processo etl, chamando
        outros métodos internos da classe

        Args:
            json_list: a lista com os dados capturados
            dw_query: a consulta SQL a ser realizada
            bucket: o Bucket em que devem ser inserido o arquivo
            file_path: o caminho para o arquivo no bucket
            batch_load: o tipo de carga de dados,
                batch_load=False faz um processo incremental
            table_name: str
        """

        json_list = self._fetch_data(batch_load, table_name)
        self._insert_to_s3_bronze_layer(json_list, bucket, file_path)
        self._insert_to_s3_silver_layer(json_list, bucket, file_path)
        self._insert_to_s3_dw_layer(dw_query, bucket, file_path)
