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

subproductSchema = StructType() \
     .add("id" ,StringType(),True) \
     .add("product" ,StringType(),True) \
     .add("subproduct_id" ,StringType(),True) \
     .add("short_desc" ,StringType(),True) \
     .add("long_desc" ,StringType(),True) \
     .add("crm_info" ,StringType(),True) \
     .add("crm_info_expiry_date" ,StringType(),True) \
     .add("currency_code" ,StringType(),True) \
     .add("accept_other_currency" ,StringType(),True) \
     .add("is_for_all_branches" ,StringType(),True) \
     .add("is_interbranch_allowed" ,StringType(),True) \
     .add("is_condition_update_allowed" ,StringType(),True) \
     .add("is_packages_allowed" ,StringType(),True) \
     .add("is_funds_settlement_allowed" ,StringType(),True) \
     .add("business_sector" ,StringType(),True) \
     .add("is_forced_business_sector" ,StringType(),True) \
     .add("valued_date_txn_allowed" ,StringType(),True) \
     .add("valued_date_txn_mode" ,StringType(),True) \
     .add("valued_date_txn_max_days" ,StringType(),True) \
     .add("valued_date_int_mode" ,StringType(),True) \
     .add("valued_date_int_max_days" ,StringType(),True) \
     .add("check_digit" ,StringType(),True) \
     .add("autom_nbr_acct_flag" ,StringType(),True) \
     .add("autom_nbr_acct_mode" ,StringType(),True) \
     .add("stt_issue_flag" ,StringType(),True) \
     .add("stt_period" ,StringType(),True) \
     .add("stt_interval" ,StringType(),True) \
     .add("stt_term_type" ,StringType(),True) \
     .add("stt_day_week_1" ,StringType(),True) \
     .add("stt_day_week_2" ,StringType(),True) \
     .add("stt_day_week_3" ,StringType(),True) \
     .add("stt_day" ,StringType(),True) \
     .add("stt_day_nbr" ,StringType(),True) \
     .add("stt_month_nbr" ,StringType(),True) \
     .add("stt_ordinal" ,StringType(),True) \
     .add("stt_max_lines" ,StringType(),True) \
     .add("stt_order" ,StringType(),True) \
     .add("special_comission_flag" ,StringType(),True) \
     .add("special_cr_interest_flag" ,StringType(),True) \
     .add("special_db_interest_flag" ,StringType(),True) \
     .add("warranty_req_flag" ,StringType(),True) \
     .add("holiday_analisys_type" ,StringType(),True) \
     .add("batch_process_flag" ,StringType(),True) \
     .add("date_from" ,StringType(),True) \
     .add("date_to" ,StringType(),True) \
     .add("status" ,StringType(),True) \
     .add("status_date" ,StringType(),True) \
     .add("status_reason" ,StringType(),True) \
     .add("system_user_core" ,StringType(),True) \
     .add("stamp_additional" ,StringType(),True) \
     .add("stamp_date_time" ,StringType(),True) \
     .add("stamp_user" ,StringType(),True) \
     .add("official_id" ,StringType(),True) \
     .add("nemotecnico" ,StringType(),True) \
     .add("accounting_strip" ,StringType(),True)

productSchema = StructType() \
      .add('id',StringType(),True) \
      .add('institution',StringType(),True) \
      .add('product_id',StringType(),True) \
      .add('short_desc',StringType(),True) \
      .add('long_desc',StringType(),True) \
      .add('detail_table_name',StringType(),True) \
      .add('crm_info',StringType(),True) \
      .add('crm_info_expiry_date',StringType(),True) \
      .add('product_is_own',StringType(),True) \
      .add('iso_product',StringType(),True) \
      .add('date_from',StringType(),True) \
      .add('date_to',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('system_user_core',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('official_id',StringType(),True)

isoProductTypeSchema = StructType() \
      .add('id',StringType(),True) \
      .add('nemotecnico',StringType(),True) \
      .add('short_desc',StringType(),True) \
      .add('long_desc',StringType(),True) \
      .add('iso_product_type_id',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('official_id',StringType(),True)

subproduct = sqlc.read.options(delimiter="\020").schema(subproductSchema).csv("caminho da tabela")
product = sqlc.read.options(delimiter="\020").schema(productSchema).csv("caminho da tabela")
iso_product_type = sqlc.read.options(delimiter="\020").schema(isoProductTypeSchema).csv("caminho da tabela")

subproduct.createOrReplaceTempView('subproduct')
product.createOrReplaceTempView('product')
iso_product_type.createOrReplaceTempView('iso_product_type')

df = sqlc.sql("""SELECT
    `sp`.`subproduct_id` AS `CD_PRODUTO`,
    `sp`.`long_desc` AS `DE_PRODUTO`,
    CASE
        `iso`.`nemotecnico` WHEN 'CLIENTES_PRE' THEN 1
        WHEN 'CLIENTES_POS' THEN 2
        ELSE 0
    END AS `TP_NATUREZA`,
    `p`.`stamp_date_time`
FROM
    `PRODUCT` `P`
INNER JOIN `ISO_PRODUCT_TYPE` `ISO` ON
    ( `p`.`iso_product` = `iso`.`id` )
INNER JOIN `SUBPRODUCT` `SP` ON
    ( `p`.`id` = `sp`.`product` )
WHERE
    `iso`.`nemotecnico` IN ('CLIENTES_PRE', 'CLIENTES_POS')""")

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
dfp.to_sql(con= my_con, name = "produtos_ip_aux", if_exists ="replace", index = False)