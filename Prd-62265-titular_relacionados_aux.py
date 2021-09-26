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

df = sqlc.sql("""SELECT
    '0' as `CD_TITULAR`,
    '0' AS `CD_CPF_CNPJ`,
    '0' AS `NM_RELACIONADO`,
    NULL as `CD_TP_PAPEL`,
    NULL as `FL_PEP`,
    cast('2020-01-01' as timestamp) as `stamp_date_time`
""")

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
dfp.to_sql(con= my_con, name = "titular_relacionados_aux", if_exists ="replace", index = False)