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
import datetime
import pandas as pd
import subprocess

sqlc = SQLContext(spark)

billingSchema = StructType() \
      .add("id",StringType(),True) \
      .add("account",StringType(),True) \
      .add("nsu",StringType(),True) \
      .add("launch_date",StringType(),True) \
      .add("maturity_date",StringType(),True) \
      .add("amount",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("cleared",StringType(),True) \
      .add("payment_date",StringType(),True)

billing = sqlc.read.options(delimiter="\020").schema(billingSchema).csv("caminho da tabela") 
GestaoInadimplencia = sqlc.read.json("caminho da tabela")
GestaoRecebimento = sqlc.read.json("caminho da tabela")

billing.createOrReplaceTempView("billing")
GestaoInadimplencia.createOrReplaceTempView("GestaoInadimplencia")
GestaoRecebimento.createOrReplaceTempView("GestaoRecebimento")

dados_dlgi = sqlc.sql("""SELECT
i.cliente as cliente
,i.conta as conta
from gestaoinadimplencia i
group by
conta
,cliente
order by
cliente
""")

dados_dlgiC = dados_dlgi

dados_dlgr = sqlc.sql("""select 
r.conta as conta
, r.version as version
, r.situacao as situacao
, r.valor as valor
, r.slip as slip
, r.atividade as atividade
, r.codigoautorizacao as codigoautorizacao
, r.idtransacaocyber as idtransacaocyber
, r.datafechamentocorebank as datafechamentocorebank
, r.datavencimentocorebank as datavencimentocorebank
, r.formapagamento as formapagamento
, r.motivo as motivo
, r.billing as billing
, r.datacriacao as datacriacao
, r.comprovantedevenda as comprovantedevenda
, r.id as id
, r.braspagPaymentId as braspagpay
, r.nsu as nsu
from gestaorecebimento r
where cast(substr(r.datavencimentocorebank, 1, 10) as date) < date_add(current_date(), -30)
and r.nsu is not null 
and r.nsu <> 'NOT_APPLIED'
and r.situacao <> 'COMPENSADO'
""")

dados_dlgr.withColumn("nsu", dados_dlgr["nsu"].cast('int'))

dados_dlgrC = dados_dlgr

dados_dlb = sqlc.sql("""select b.nsu as nsu, b.payment_date as payment_date 
from billing b 
where payment_date is not null  
""")

dados_dlbC = dados_dlb

dados_m1 = dados_dlgiC.join(dados_dlgrC, on = 'conta', how = 'left')

dados_m1C = dados_m1

dados_m2 = dados_m1C.join(dados_dlbC, on = 'nsu', how = 'left')

dados_m2C = dados_m2

dados_m2C = dados_m2C.dropna(subset=['situacao'])
dados_m2C = dados_m2C.dropna(subset=['payment_date'])

import datetime

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
#convert to datetime - pandas
yesterday=pd.to_datetime(yesterday)
data = yesterday.strftime('%Y%m%d')

df_filtrado = dados_m2C.toPandas()
filename = "Divergências_em_faturas_GR_x_Cyber_{0}.xlsx".format(data)
df_filtrado.to_excel(filename, index=False, header=True, encoding='utf-8-sig' )

# Troca o path
credential_provider_path = "caminho do hdfs e sftp"
credential_name = "nome da credencial"

# Recupera a senha e armazena na variavel "credential_pass"
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("hadoop.security.credential.provider.path",credential_provider_path)
credential_raw = conf.getPassword(credential_name)
credential_pass = "senha da credencial"
for i in range(credential_raw.__len__()):
    credential_pass = credential_pass + str(credential_raw.__getitem__(i))

myHostname = "nome do host"
myUsername = "seu User"
cnopts = pysftp.CnOpts(knownhosts="known_hosts")
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

remotePath = '/cobranca/out'

conn = pysftp.Connection(host=myHostname, username=myUsername, password=credential_pass,cnopts=cnopts)
with conn.cd(remotePath):
    conn.put(filename)

### PAX ###
subprocess.call(["hdfs", "dfs", "-rm", "-f", filename])