spark = SparkContext()

sqlc = SQLContext(spark)
try:
    spark.install_pypi_package("")
    spark.install_pypi_package("six")
    spark.install_pypi_package("pip==21.1.2")
    spark.install_pypi_package("koalas")
    spark.install_pypi_package("numpy==1.21.0")
    spark.install_pypi_package("pandas==1.0.5")
    spark.install_pypi_package("csv")
    spark.install_pypi_package("datetime==4.3")
    
except:
    pass

import databricks.koalas as pd
import lxml
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
import pysftp
import pandas
import subprocess
import csv

customerOperationsSchema = StructType() \
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
      
provinceCodeSchema = StructType() \
      .add("id",StringType(),True) \
      .add("province_code_id",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("country",StringType(),True) \
      .add("official_id",StringType(),True)

electronicsContactSchema = StructType() \
      .add("id",StringType(),True) \
      .add("customer",StringType(),True) \
      .add("email_type",StringType(),True) \
      .add("email_order",StringType(),True) \
      .add("email_use",StringType(),True) \
      .add("email_address_complete",StringType(),True) \
      .add("integrity_structure",StringType(),True) \
      .add("email_last_use",StringType(),True) \
      .add("addition_date",StringType(),True) \
      .add("purge_reason",StringType(),True) \
      .add("purge_date",StringType(),True) \
      .add("comments",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("system_user_core",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("priority",StringType(),True) \
      .add("electronics_contact_type",StringType(),True) \
      .add("electronics_contact_id",StringType(),True) \
      .add("email_for_comu",StringType(),True)

addressSchema = StructType() \
      .add("id",StringType(),True) \
      .add('address_id', StringType(), True) \
      .add('customer', StringType(), True) \
      .add('address_type', StringType(), True) \
      .add('address_allocate', StringType(), True) \
      .add('geographic_zone', StringType(), True) \
      .add('street_type', StringType(), True) \
      .add('street', StringType(), True) \
      .add('street_number', StringType(), True) \
      .add('without_number_street', StringType(), True) \
      .add('street_extension', StringType(), True) \
      .add('area', StringType(), True) \
      .add('postal_office_box', StringType(), True) \
      .add('branch_postal_office_box', StringType(), True) \
      .add('town_zone', StringType(), True) \
      .add('internal_street', StringType(), True) \
      .add('internal_street_number', StringType(), True) \
      .add('floor', StringType(), True) \
      .add('apartment', StringType(), True) \
      .add('office', StringType(), True) \
      .add('highway', StringType(), True) \
      .add('highway_exit', StringType(), True) \
      .add('route', StringType(), True) \
      .add('route_kilometre', StringType(), True) \
      .add('rural_zone', StringType(), True) \
      .add('train_station_name', StringType(), True) \
      .add('locality', StringType(), True) \
      .add('zip_code', StringType(), True) \
      .add('new_zip_code', StringType(), True) \
      .add('right_street', StringType(), True) \
      .add('left_street', StringType(), True) \
      .add('property_register', StringType(), True) \
      .add('province', StringType(), True) \
      .add('province_code', StringType(), True) \
      .add('region', StringType(), True) \
      .add('country', StringType(), True) \
      .add('country_zip_code', StringType(), True) \
      .add('integrity_code', StringType(), True) \
      .add('post_destination_branch', StringType(), True) \
      .add('maps_relations', StringType(), True) \
      .add('reside_begin_date', StringType(), True) \
      .add('address_last_use_date' , StringType(), True) \
      .add('return_mail_type' , StringType(), True) \
      .add('last_return_mail_date' , StringType(), True) \
      .add('address_expiry_date' , StringType(), True) \
      .add('comments' , StringType(), True) \
      .add('adition_date' , StringType(), True) \
      .add('purge_date' , StringType(), True) \
      .add('purge_reason' , StringType(), True) \
      .add('status' , StringType(), True) \
      .add('status_date' , StringType(), True) \
      .add('stamp_user' , StringType(), True) \
      .add('status_reason' , StringType(), True) \
      .add('system_user_core', StringType(), True) \
      .add('stamp_additional', StringType(), True) \
      .add('stamp_date_time', StringType(), True) \
      .add('stamp_user', StringType(), True) \
      .add('exact_address', StringType(), True) \
      .add('postal_box', StringType(), True) \
      .add('postal_box_branch', StringType(), True) \
      .add('parcell', StringType(), True) \
      .add('block', StringType(), True) \
      .add('house_number', StringType(), True) \
      .add('house_type', StringType(), True) \
      .add('sector', StringType(), True) \
      .add('canton', StringType(), True) \
      .add('district', StringType(), True) \
      .add('neighborhood', StringType(), True) \
      .add('locality_zip_code', StringType(), True) \
      .add('residence_type', StringType(), True) \
      .add('residence_condition', StringType(), True) \
      .add('is_for_correspondence', StringType(), True)

localitySchema = StructType() \
      .add("id",StringType(),True) \
      .add("locality_code_id",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("zip_code",StringType(),True) \
      .add("official_id",StringType(),True) \
      .add("province_code",StringType(),True)

contactPhoneSchema = StructType() \
      .add("id",StringType(),True) \
      .add("communication_id",StringType(),True) \
      .add("customer",StringType(),True) \
      .add("address",StringType(),True) \
      .add("order_phone",StringType(),True) \
      .add("communication_type",StringType(),True) \
      .add("country_phone",StringType(),True) \
      .add("telephone_type",StringType(),True) \
      .add("phone_characteristic",StringType(),True) \
      .add("area_country",StringType(),True) \
      .add("area_code",StringType(),True) \
      .add("cellular_identification",StringType(),True) \
      .add("distribution_area",StringType(),True) \
      .add("phone_number",StringType(),True) \
      .add("phone_extension",StringType(),True) \
      .add("phone_use_code",StringType(),True) \
      .add("phone_expiry_date",StringType(),True) \
      .add("phone_last_use_date",StringType(),True) \
      .add("integrity_structure",StringType(),True) \
      .add("addition_date",StringType(),True) \
      .add("purge_date",StringType(),True) \
      .add("purge_reason",StringType(),True) \
      .add("comments",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("system_user_core",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("country",StringType(),True) \
      .add("tel_for_comu",StringType(),True)

isoProductTypeSchema = StructType() \
      .add("id",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("iso_product_type_id",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)

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

legalEntitySchema = StructType() \
      .add("id",StringType(),True) \
      .add("customer",StringType(),True) \
      .add("central_bank_reporting_name",StringType(),True) \
      .add("company_legal_type",StringType(),True) \
      .add("legal_start_date",StringType(),True) \
      .add("legal_expiry_date",StringType(),True) \
      .add("activity_start_date",StringType(),True) \
      .add("fiscal_balance_ending_month",StringType(),True) \
      .add("fiscal_ending_balance_date",StringType(),True) \
      .add("exporting_code",StringType(),True) \
      .add("country_of_origin",StringType(),True) \
      .add("quotation_code",StringType(),True) \
      .add("board_of_directors_expiry",StringType(),True) \
      .add("management_change_date",StringType(),True) \
      .add("system_user_core",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("activity_description",StringType(),True) \
      .add("incorporation_date",StringType(),True) \
      .add("commercial_name",StringType(),True) \
      .add("other_business_sector",StringType(),True) \
      .add("other_company_category",StringType(),True) \
      .add("other_company_type",StringType(),True) \
      .add("other_int_business_detail",StringType(),True) \
      .add("company_size",StringType(),True) \
      .add("underactivity",StringType(),True) \
      .add("business_environment",StringType(),True) \
      .add("company_business",StringType(),True) \
      .add("company_category",StringType(),True) \
      .add("int_business_detail",StringType(),True) \
      .add("folio_number",StringType(),True) \
      .add("country_legal_adrress",StringType(),True) \
      .add("company_size_points",StringType(),True) \
      .add("reason_for_change",StringType(),True)

customer = sqlc.read.options(delimiter="\020").schema(customerSchema).csv("caminho da tabela")
legal_entity = sqlc.read.options(delimiter="\020").schema(legalEntitySchema).csv("caminho da tabela")
customer_operation = sqlc.read.options(delimiter="\020").schema(customerOperationSchema).csv("caminho da tabela")
address = sqlc.read.options(delimiter="\020").schema(addressSchema).csv("caminho da tabela")
locality = sqlc.read.options(delimiter="\020").schema(localitySchema).csv("caminho da tabela")
province_code = sqlc.read.options(delimiter="\020").schema(provinceCodeSchema).csv("caminho da tabela")
electronics_contact = sqlc.read.options(delimiter="\020").schema(electronicsContactSchema).csv("caminho da tabela")
contact_phone = sqlc.read.options(delimiter="\020").schema(contactPhoneSchema).csv("caminho da tabela")
account = sqlc.read.options(delimiter="\020").schema(accountSchema).csv("caminho da tabela")
iso_product_type = sqlc.read.options(delimiter="\020").schema(isoProductTypeSchema).csv("caminho da tabela")

customer.createOrReplaceTempView("customer")
legal_entity.createOrReplaceTempView("legal_entity")
customer_operation.createOrReplaceTempView("customer_operation")
address.createOrReplaceTempView("address")
locality.createOrReplaceTempView("locality")
province_code.createOrReplaceTempView("province_code")
electronics_contact.createOrReplaceTempView("electronics_contact")
contact_phone.createOrReplaceTempView("contact_phone")
account.createOrReplaceTempView("account")
iso_product_type.createOrReplaceTempView("iso_product_type")

df1 = sqlc.sql("""select 
'0100' as REG,
c.id as COD_CLIENTE,
case when c.person_type = 2 then identification_number
end as CNPJ,
0 as CPF,
case when le.commercial_name is null then c.name
else le.commercial_name
end as N_FANT,
adrs.STREET AS ENDERECO,
adrs.WITHOUT_NUMBER_STREET AS NUMERO,
adrs.EXACT_ADDRESS AS COMPLEMENTO,
D.SHORT_DESC AS CIDADE,
adrs.COUNTRY_ZIP_CODE as CEP,
pc.short_desc as UF,
NULL as NOME_RESP,
0 as telefone,
null as EMAIL_CONT,
--CURRENT_TIMESTAMP() as DT_EXTRACAO
D.short_desc as MUNICIPIOS,
date_format(c.addition_date,'yyyyMMdd') as DT_CREDEN,
CONCAT(CONCAT(CONCAT_WS(", ", adrs.STREET, adrs.WITHOUT_NUMBER_STREET, adrs.EXACT_ADDRESS, D.SHORT_DESC))) as address
from customer c
left join legal_entity le on le.customer = c.id
left JOIN CUSTOMER_OPERATION CO ON C.ID = CO.CUSTOMER
left join address adrs on ( C.ID = adrs.customer AND CO.CUSTOMER = adrs.CUSTOMER )
LEFT JOIN LOCALITY D ON ( adrs.LOCALITY_ZIP_CODE = D.ID )
left join province_code pc on pc.id = adrs.province_code
LEFT JOIN ELECTRONICS_CONTACT EC ON ( CO.CUSTOMER = EC.CUSTOMER )
LEFT JOIN CONTACT_PHONE CP ON ( CP.CUSTOMER = EC.CUSTOMER )
left JOIN account A ON A.OPERATION_ID = CO.OPERATION_ID
left JOIN iso_product_type IPT ON IPT.ID = A.ISO_PRODUCT
WHERE IPT.nemotecnico = 'COMER_EC'
--and c.addition_date between date_format(date_add(last_day(add_months(current_date, -2)),1), 'yyyyMMdd') 
--and date_format(last_day(add_months((current_date), -1)), 'yyyyMMdd')
group by
--'0100' as REG,
c.id,
case when c.person_type = 2 then identification_number end,
case when le.commercial_name is null then c.name
else le.commercial_name end,
adrs.STREET,
adrs.WITHOUT_NUMBER_STREET,
adrs.EXACT_ADDRESS,
D.SHORT_DESC,
adrs.COUNTRY_ZIP_CODE,
pc.short_desc,
c.name,
D.short_desc,
c.addition_date
""")

#df1 = spark.sql(query)

df1.withColumn("cod_cliente", df1["cod_cliente"].cast('int'))

df1.dropDuplicates()

df1C = pd.DataFrame(df1)

x = df1C.MUNICIPIOS
y = df1C.UF
chave = x + ' - ' + y
df1C['chave'] = chave

df = pandas.read_html("https://www.ibge.gov.br/explica/codigos-dos-municipios.php", header=None)

df_bra2 = pandas.DataFrame(columns= ["Municipios", "Codigos", "Estados"])

for i in range(len(df)):
    try:
        list_header = list(df[i+1].columns)
        df_new = df[i+1].rename(columns={"{0}".format(list_header[0]):"Municipios",
                                     "Códigos": "Codigos"})
        df_new["Estados"] = df[i+1].columns[0]
    except:
        pass
    df_bra2 = df_bra2.append(df_new)

antes = [
    'Municípios do Acre',
'Municípios de Alagoas',
'Municípios do Amapá',
'Municípios do Amazonas',
'Municípios da Bahia',
'Municípios do Ceará',
'Distrito Federal',
'Municípios do Espírito Santo',
'Municípios de Goiás',
'Municípios do Maranhão',
'Municípios de Mato Grosso',
'Municípios de Mato Grosso do Sul',
'Municípios de Minas Gerais',
'Municípios do Pará',
'Municípios da Paraíba',
'Municípios do Paraná',
'Municípios de Pernambuco',
'Municípios do Piauí',
'Municípios do Rio de Janeiro',
'Municípios do Rio Grande do Norte',
'Municípios do Rio Grande do Sul',
'Municípios de Rondônia',
'Municípios de Roraima',
'Municípios de Santa Catarina',
'Municípios de São Paulo',
'Municípios do Sergipe',
'Municípios de Tocantins'

]

depois = [
 'AC',
'AL',
'AP',
'AM',
'BA',
'CE',
'DF',
'ES',
'GO',
'MA',
'MT',
'MS',
'MG',
'PA',
'PB',
'PR',
'PE',
'PI',
'RJ',
'RN',
'RS',
'RO',
'RR',
'SC',
'SP',
'SE',
'TO'
]

df_bra2["Estados"] = df_bra2["Estados"].replace(to_replace = antes, value = depois)

df_bra2["chave"] = df_bra2["Municipios"] +" - "+df_bra2["Estados"]

df_braC = df_bra2

df2 = df1C.to_spark().toPandas()

dfM1 = df2.merge(df_braC[['chave', 'Codigos']], on = 'chave', how = 'left')

pandas.set_option('display.max_columns', None)

dfM1C = dfM1

dfM1C = dfM1C.rename(columns={'Codigos': 'COD_MUN'})

dfM1C = dfM1C.drop(['ENDERECO', 'NUMERO', 'COMPLEMENTO', 'CIDADE'], axis = 1)

dfM1C.columns = dfM1C.columns.str.lower()

dfM1C = dfM1C[['reg',
 'cod_cliente',
 'cnpj',
 'cpf',
 'n_fant',
 'address',
 'cep',
 'cod_mun',
 'uf',
 'nome_resp',
 'telefone',
 'email_cont',
 'dt_creden',
 'municipios',
 'chave'
 ]]

dfM1C = dfM1C.rename(columns={'address': 'END'})

dfM1C.columns = dfM1C.columns.str.upper()

dfM1C = dfM1C.drop_duplicates()

dfM1C = dfM1C.drop(['MUNICIPIOS', 'CHAVE'], axis = 1)

dfM1C["CEP"] = dfM1C["CEP"].apply(lambda x: x.replace("-", ""))

dfM1C = dfM1C.rename(columns={'TELEFONE': 'FONE_CONT'})

filename = "Arquivo_0100_Cadastro_de_Cliente.csv"
dfM1C.to_csv(filename, index=False, encoding='utf-8-sig', sep=";")

credential_provider_path = "caminho do hdfs e sftp"
credential_name = "nome da credencial"

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

remotePath = '/ELOPAR_SEFAZ/out'

conn = pysftp.Connection(host=myHostname, username=myUsername, password=credential_pass,cnopts=cnopts)
with conn.cd(remotePath):
    conn.put(filename)

subprocess.call(["hdfs", "dfs", "-rm", "-f", filename])