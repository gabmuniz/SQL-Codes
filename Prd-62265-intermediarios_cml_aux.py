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

intermediariosCmlAuxSchema = StructType() \
      .add('cd_intermediario',StringType(),True) \
      .add('cd_midia',StringType(),True) \
      .add('cnpj_intermediario',StringType(),True) \
      .add('de_intermediario',StringType(),True) \
      .add('de_cidade',StringType(),True) \
      .add('de_uf',StringType(),True)

intermediarios_cml_aux = sqlc.read.options(delimiter="\020").schema(intermediariosCmlAuxSchema).csv("caminho da tabela")

intermediarios_cml_aux.createOrReplaceTempView('intermediarios_cml_aux')

df = sqlc.sql("""select
    `a`.`cd_intermediario`,
    `a`.`cd_midia`,
    `a`.`cnpj_intermediario`,
    `a`.`de_intermediario`,
    `a`.`de_cidade`,
    `a`.`de_uf`,
    from_unixtime(unix_timestamp('01/01/2020' ,
    'MM/dd/yyyy'),
    'yyyy-MM-dd') as `stamp_date_time`
FROM
    `pld`.`intermediarios_cml_aux` `A`""")

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
dfp.to_sql(con= my_con, name = "intermediarios_cml_aux", if_exists ="replace", index = False)