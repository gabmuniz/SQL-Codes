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

contaPagtoAuxSchema = StructType() \
      .add('cd_titular',StringType(),True) \
      .add('cd_cta_pagto',StringType(),True) \
      .add('cd_contratante',StringType(),True) \
      .add('stamp_date_time',StringType(),True)

view_conta_pagto_aux = sqlc.read.options(delimiter="\020").schema(contaPagtoAuxSchema).csv("caminho da tabela")

dfp = view_conta_pagto_aux.toPandas()

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
dfp.to_sql(con= my_con, name = "conta_pagto_aux", if_exists ="replace", index = False)