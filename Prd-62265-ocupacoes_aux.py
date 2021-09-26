from pyspark import SparkContext

spark = SparkContext()

spark.install_pypi_package("pip==21.1.2")
spark.install_pypi_package("pysftp==0.2.9")
spark.install_pypi_package("pandas==1.0.5")
spark.install_pypi_package("csv")
spark.install_pypi_package("datetime==4.3")
spark.install_pypi_package("normalize==2.0.2")

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

occupationCodeSchema = StructType() \
      .add('id',StringType(),True) \
      .add('occupation_code_id',StringType(),True) \
      .add('short_desc',StringType(),True) \
      .add('long_desc',StringType(),True) \
      .add('nemotecnico',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('official_id',StringType(),True)

occupation_code = sqlc.read.options(delimiter="\020").schema(occupationCodeSchema).csv("caminho da tabela")

occupation_code.createOrReplaceTempView('occupation_code')

df = sqlc.sql("""select
    `occupation_code`.`occupation_code_id` as `cd_ocupacao`,
    `occupation_code`.`short_desc` as `de_ocupacao`,
    null as `fl_risco`,
    null as `vl_limite_ordens_mes`,
    `occupation_code`.`stamp_date_time`
from
    `occupation_code`""")

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

from unicodedata import normalize
def removerCaracteresEspeciais (text) :
    if type(text) == str:
        return normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    else:
        return text

for c in dfp:
    dfp[c] = dfp[c].apply(removerCaracteresEspeciais)

my_con = create_engine("conexão ao banco de dados + caminho ao ambiente prd".format(credential_pass))  
dfp.to_sql(con= my_con, name = "ocupacoes_aux", if_exists ="replace", index = False)