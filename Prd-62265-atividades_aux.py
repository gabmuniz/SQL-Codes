from pyspark import SparkContext

spark = SparkContext()

spark.install_pypi_package("pip==21.1.2")
spark.install_pypi_package("pysftp==0.2.9")
spark.install_pypi_package("pandas==1.0.5")
spark.install_pypi_package("csv")
spark.install_pypi_package("datetime==4.3")

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
import pysftp
import pandas as pd
import subprocess
from sqlalchemy import create_engine

sqlc = SQLContext(spark)

centralBankActivityCodeSchema = StructType() \
      .add("id",StringType(),True) \
      .add("central_bank_activity_code_id",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)

central_bank_activity_code = sqlc.read.options(delimiter="\020").schema(centralBankActivityCodeSchema).csv("caminho da tabela")

central_bank_activity_code.createOrReplaceTempView('central_bank_activity_code')

df = sqlc.sql("""SELECT
    `central_bank_activity_code_id` AS `CD_ATIVIDAD`,
    `short_desc` AS `DE_ATIVIDADE`,
    NULL AS `FL_RISCO`,
    `stamp_date_time`
FROM
    `central_bank_activity_code`
where
    `status` = 7""")

dfp = df.toPandas()

# Troca o path
credential_provider_path = "caminho do hdfs e sftp"
credential_name = "nome da credencial"

# Recupera a senha e armazena na variavel "credential_pass"
conf = spark._jsc.hadoopConfiguration()
conf.set("hadoop.security.credential.provider.path",credential_provider_path)
credential_raw = conf.getPassword(credential_name)
credential_pass = "senha da credencial"
for i in range(credential_raw.__len__()):
    credential_pass = credential_pass + str(credential_raw.__getitem__(i))

my_con = create_engine("conexão ao banco de dados + caminho ao ambiente prd".format(credential_pass))
dfp.to_sql(con= my_con, name = "atividades_aux", if_exists ="replace", index = False)

pd.read_sql("select * from atividades_aux", con = my_con)