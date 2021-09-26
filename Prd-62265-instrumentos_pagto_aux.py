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

customerSchema = StructType() \
      .add("id",StringType(),True) \
      .add("institution",StringType(),True) \
      .add("customer_id",StringType(),True) \
      .add("name",StringType(),True) \
      .add("first_name1",StringType(),True) \
      .add("last_name1",StringType(),True) \
      .add("first_name2",StringType(),True) \
      .add("last_name2",StringType(),True) \
      .add("name_short",StringType(),True) \
      .add("person_type",StringType(),True) \
      .add("customer_type",StringType(),True) \
      .add("crm_info",StringType(),True) \
      .add("expiry_date_crm_info",StringType(),True) \
      .add("originating_branch",StringType(),True) \
      .add("segment",StringType(),True) \
      .add("primary_product",StringType(),True) \
      .add("attention_segment",StringType(),True) \
      .add("responsability_officer",StringType(),True) \
      .add("classification_code",StringType(),True) \
      .add("credit_status",StringType(),True) \
      .add("condition",StringType(),True) \
      .add("origin_customer",StringType(),True) \
      .add("confidential_flag",StringType(),True) \
      .add("bank_link_type",StringType(),True) \
      .add("central_bank_activity_code",StringType(),True) \
      .add("central_bk_activity_second",StringType(),True) \
      .add("bank_activity_code",StringType(),True) \
      .add("bank_activity_second",StringType(),True) \
      .add("business_sector",StringType(),True) \
      .add("residence_code",StringType(),True) \
      .add("addition_date",StringType(),True) \
      .add("purge_reason",StringType(),True) \
      .add("purge_date",StringType(),True) \
      .add("contact_bank",StringType(),True) \
      .add("seniority_bank",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("system_user_core",StringType(),True) \
      .add("identification_type",StringType(),True) \
      .add("identification_number",StringType(),True) \
      .add("department",StringType(),True) \
      .add("epay_person_type",StringType(),True) \
      .add("credit_status_date",StringType(),True) \
      .add("dependents_f",StringType(),True) \
      .add("dependents_m",StringType(),True) \
      .add("is_employee",StringType(),True) \
      .add("primary_subproduct",StringType(),True) \
      .add("pep",StringType(),True) \
      .add("description",StringType(),True) \
      .add("subsidiary",StringType(),True) \
      .add("channel",StringType(),True) \
      .add("is_duplicated",StringType(),True)

accountSchema = StructType() \
      .add("id",StringType(),True) \
      .add("product", StringType(), True) \
      .add("subproduct", StringType(), True) \
      .add("branch", StringType(), True) \
      .add("operation_id", StringType(), True) \
      .add("currency", StringType(), True) \
      .add("iso_product", StringType(), True) \
      .add("name", StringType(), True) \
      .add("short_name", StringType(), True) \
      .add("input_date", StringType(), True) \
      .add("opening_date", StringType(), True) \
      .add("closed_date", StringType(), True) \
      .add("closed_code", StringType(), True) \
      .add("use_of_signature", StringType(), True) \
      .add("situation_code", StringType(), True) \
      .add("interbranch_allowed", StringType(), True) \
      .add("vat_category", StringType(), True) \
      .add("central_bank_activity_code", StringType(), True) \
      .add("business_sector", StringType(), True) \
      .add("bank_activity_code", StringType(), True) \
      .add("available_bal_process_type", StringType(), True) \
      .add("available_bal_process_time", StringType(), True) \
      .add("db_bal_allowed_flag", StringType(), True) \
      .add("verif_hist_bal_flag", StringType(), True) \
      .add("checks_allowed_flag", StringType(), True) \
      .add("min_days_for_checks", StringType(), True) \
      .add("link_to_package", StringType(), True) \
      .add("pack_subproduct", StringType(), True) \
      .add("pack_operation_id", StringType(), True) \
      .add("pack_main_account", StringType(), True) \
      .add("price_list_profile", StringType(), True) \
      .add("special_fees", StringType(), True) \
      .add("special_rates", StringType(), True) \
      .add("db_restriction", StringType(), True) \
      .add("deposit_restriction", StringType(), True) \
      .add("withdrawal_restriction", StringType(), True) \
      .add("cr_restriction", StringType(), True) \
      .add("customer_number_declared", StringType(), True) \
      .add("customer_number_reached", StringType(), True) \
      .add("customer_number_for_insurance", StringType(), True) \
      .add("insurance_code", StringType(), True) \
      .add("update_date" , StringType(), True) \
      .add("last_process_date" , StringType(), True) \
      .add("last_txn_date" , StringType(), True) \
      .add("last_txn_2date" , StringType(), True) \
      .add("last_txn_int_cr_date" , StringType(), True) \
      .add("last_txn_ext_cr_date" , StringType(), True) \
      .add("last_txn_int_db_date" , StringType(), True) \
      .add("last_txn_ext_db_date" , StringType(), True) \
      .add("last_withdrawal_date" , StringType(), True) \
      .add("check_temporary_receivership" , StringType(), True) \
      .add("last_stat_process" , StringType(), True) \
      .add("last_history_process" , StringType(), True) \
      .add("db_days_without_agreement", StringType(), True) \
      .add("db_balance_start_date", StringType(), True) \
      .add("db_balance_total_days", StringType(), True) \
      .add("cnt_chk_formal_reject_mtd", StringType(), True) \
      .add("cnt_chk_formal_reject_ytd", StringType(), True) \
      .add("cnt_chk_reject_mtd", StringType(), True) \
      .add("cnt_chk_reject_ytd", StringType(), True) \
      .add("cnt_chk_justified_ytd", StringType(), True) \
      .add("cnt_onp_ytd", StringType(), True) \
      .add("cnt_stop_payments_ytd", StringType(), True) \
      .add("cnt_withdrawal", StringType(), True) \
      .add("debit_mode_type", StringType(), True) \
      .add("accrual_suspended_date", StringType(), True) \
      .add("total_db_accrued_interest", StringType(), True) \
      .add("oldest_valued_date_txn", StringType(), True) \
      .add("balance_today", StringType(), True) \
      .add("balance_yesterday", StringType(), True) \
      .add("db_balance_to_accrual", StringType(), True) \
      .add("today_cash_deposits", StringType(), True) \
      .add("today_cash_withdrawal", StringType(), True) \
      .add("today_other_cr", StringType(), True) \
      .add("today_other_db", StringType(), True) \
      .add("today24_hrs_db", StringType(), True) \
      .add("tomorrow_cr", StringType(), True) \
      .add("tomorrow_db", StringType(), True) \
      .add("yesterday_cr", StringType(), True) \
      .add("yesterday_db", StringType(), True) \
      .add("total_agreements", StringType(), True) \
      .add("tomorrow_agreements", StringType(), True) \
      .add("blocked_balance", StringType(), True) \
      .add("tomorrow_blocked", StringType(), True) \
      .add("accum_pending_txn", StringType(), True) \
      .add("today_deposits24_hrs", StringType(), True) \
      .add("today_deposits48_hrs", StringType(), True) \
      .add("today_deposits72_hrs", StringType(), True) \
      .add("today_deposits96_hrs", StringType(), True) \
      .add("today_deposits_other_terms", StringType(), True) \
      .add("deposits24_hrs", StringType(), True) \
      .add("deposits48_hrs", StringType(), True) \
      .add("deposits72_hrs", StringType(), True) \
      .add("deposits96_hrs", StringType(), True) \
      .add("deposits_other_terms", StringType(), True) \
      .add("other_cr24_hrs", StringType(), True) \
      .add("other_cr48_hrs", StringType(), True) \
      .add("other_cr72_hrs", StringType(), True) \
      .add("other_cr96_hrs", StringType(), True) \
      .add("other_cr_other_terms", StringType(), True) \
      .add("checks24_hrs", StringType(), True) \
      .add("checks72_hrs", StringType(), True) \
      .add("checks48_hrs", StringType(), True) \
      .add("checks96_hrs", StringType(), True) \
      .add("checks_others_terms", StringType(), True) \
      .add("other_db24_hrs", StringType(), True) \
      .add("other_db48_hrs", StringType(), True) \
      .add("other_db72_hrs", StringType(), True) \
      .add("other_db96_hrs", StringType(), True) \
      .add("other_db_other_terms", StringType(), True) \
      .add("status", StringType(), True) \
      .add("status_date", StringType(), True) \
      .add("status_reason", StringType(), True) \
      .add("system_user_core", StringType(), True) \
      .add("stamp_additional", StringType(), True) \
      .add("stamp_date_time", StringType(), True) \
      .add("stamp_user", StringType(), True) \
      .add("bank", StringType(), True) \
      .add("buk_number", StringType(), True) \
      .add("check_digit_out", StringType(), True) \
      .add("nick_name", StringType(), True) \
      .add("tipology_code", StringType(), True) \
      .add("opening_code_motive", StringType(), True) \
      .add("opening_description_motive" , StringType(), True) \
      .add("closed_description_motive" , StringType(), True) \
      .add("netting_group", StringType(), True) \
      .add("compensation_group", StringType(), True) \
      .add("positioning_group", StringType(), True) \
      .add("last_page_emitted", StringType(), True) \
      .add("last_date_emitted" , StringType(), True) \
      .add("last_page_generated", StringType(), True) \
      .add("line_stt_number", StringType(), True) \
      .add("last_stt_balance", StringType(), True) \
      .add("forced_stt_flag", StringType(), True) \
      .add("cnt_normal_stt", StringType(), True) \
      .add("cnt_special_stt", StringType(), True) \
      .add("stt_type", StringType(), True) \
      .add("stt_period", StringType(), True) \
      .add("sub_currency_is_obligatory", StringType(), True) \
      .add("donot_dormant_flag", StringType(), True) \
      .add("inhibit_checkbook_flag", StringType(), True) \
      .add("activate_form", StringType(), True) \
      .add("notice_of_closure_flag", StringType(), True) \
      .add("date_of_closure" , StringType(), True) \
      .add("data_complete_customer" , StringType(), True) \
      .add("authorization_reason", StringType(), True) \
      .add("reserve_balance", StringType(), True) \
      .add("dep_available_today", StringType(), True) \
      .add("doc_amount", StringType(), True) \
      .add("last_accrual_date" , StringType(), True) \
      .add("shadow_flag", StringType(), True)

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

customerOperationSchema = StructType() \
      .add("id",StringType(),True) \
      .add("subproduct",StringType(),True) \
      .add("branch",StringType(),True) \
      .add("operation_id",StringType(),True) \
      .add("customer",StringType(),True) \
      .add("iso_product",StringType(),True) \
      .add("type",StringType(),True) \
      .add("operating_mode",StringType(),True) \
      .add("holder_flag",StringType(),True) \
      .add("operation_status",StringType(),True) \
      .add("official_account",StringType(),True) \
      .add("date_from",StringType(),True) \
      .add("date_to",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("currency",StringType(),True) \
      .add("participation_percentage",StringType(),True)

subproduct = sqlc.read.options(delimiter="\020").schema(subproductSchema).csv("caminho da tabela")
customer = sqlc.read.options(delimiter="\020").schema(customerSchema).csv("caminho da tabela")
account = sqlc.read.options(delimiter="\020").schema(accountSchema).csv("caminho da tabela")
customer_operation_type = sqlc.read.options(delimiter="\020").schema(customerOperationTypeSchema).csv("caminho da tabela")
Identificador = sqlc.read.json("caminho da tabela")
Proposta = sqlc.read.json("caminho da tabela")
PropostaPF = sqlc.read.json("caminho da tabela")

subproduct.createOrReplaceTempView('subproduct')
customer.createOrReplaceTempView('customer')
account.createOrReplaceTempView('account')
customer_operation.createOrReplaceTempView('customer_operation')
Identificador.createOrReplaceTempView('Identificador')
Proposta.createOrReplaceTempView('Proposta')
PropostaPF.createOrReplaceTempView('PropostaPF')

df = sqlc.sql("""SELECT
    `i`.`idconta`,
    `i`.`nuidentificador`,
    `sp`.`subproduct_id` ,
    `i`.`datainclusao` ,
    IF(`i`.`situacao` = 'ATIVO',
    NULL,
    `i`.`dataatualizacao` ),
    IF(`i`.`situacao` = 'ATIVO',
    1,
    0) ,
    CASE IF(`pj`.`canaldevenda` is NULL,
    `pf`.`canalorigem`,
    `pj`.`canaldevenda`)
    when 'WEB' then '1'
    when 'FERRAMENTABB' then '2'
    when 'FERRAMENTABRA' then '2'
    when 'PARCEIRO_COMERCIAL' then '2'
    when 'BACK_OFFICE' then '3'
END ,
CASE IF(`pj`.`canaldevenda` is NULL,
`pf`.`canalorigem`,
`pj`.`canaldevenda`)
when 'WEB' then '1'
when 'FERRAMENTABB' then '2'
when 'FERRAMENTABRA' then '2'
when 'PARCEIRO_COMERCIAL' then '2'
when 'BACK_OFFICE' then '3'
END ,
`i`.`dataatualizacao` AS `stamp_date_time`
FROM
`Identificador` `i`
Inner join `customer` `c` on
( `c`.`customer_id` = `i`.`idcliente` )
inner join `customer_operation` `co` on
( `co`.`customer` = `c`.`id` )
inner join `account` `a` on
(`a`.`operation_id` = `i`.`idconta`
and `co`.`subproduct` = `a`.`subproduct`
and `co`.`branch` = `a`.`branch`
and `co`.`currency` = `a`.`currency`
and `co`.`operation_id` = `a`.`operation_id` )
INNER JOIN `SUBPRODUCT` `SP` ON
( `a`.`subproduct` = `sp`.`id` )
LEFT JOIN `proposta` `PJ` ON
`pj`.`propostaclientecontaid` = `i`.`idconta`
LEFT JOIN `propostapf` `PF` ON
`pf`.`clienteconta_numeroconta` = `i`.`idconta`
AND `pf`.`clienteconta_numerocliente` = `i`.`idcliente`""")

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
dfp.to_sql(con= my_con, name = "instrumentos_pagto_aux", if_exists ="replace", index = False)