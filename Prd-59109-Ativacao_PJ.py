from pyspark import SparkContext

spark = SparkContext()

try:
    spark.install_pypi_package("six")
    spark.install_pypi_package("pip==21.1.2")
    spark.install_pypi_package("koalas")
    spark.install_pypi_package("numpy==1.21.0")
    spark.install_pypi_package("datetime==4.3")
    spark.install_pypi_package("openpyxl==3.0.7")
    spark.install_pypi_package("pysftp==0.2.9")
except:
    pass

import databricks.koalas as ks
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import numpy as np
from datetime import datetime, date, timedelta
import pysftp
import subprocess

sqlc = SQLContext(spark)

# Schemas das tabelas
iso_product_typeSchema = StructType() \
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
      
      
productSchema = StructType() \
      .add("id",StringType(),True) \
      .add("institution",StringType(),True) \
      .add("product_id",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("detail_table_name",StringType(),True) \
      .add("crm_info",StringType(),True) \
      .add("crm_info_expiry_date",StringType(),True) \
      .add("product_is_own",StringType(),True) \
      .add("iso_product",StringType(),True) \
      .add("date_from",StringType(),True) \
      .add("date_to",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("system_user_core",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("official_id",StringType(),True)
      
accountExtSchema = StructType() \
      .add("id",StringType(),True) \
      .add("account",StringType(),True) \
      .add("is_send_flag_ccs",StringType(),True) \
      .add("send_date_ccs",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("due_date",StringType(),True) \
      .add("cut_date",StringType(),True) \
      .add("channel",StringType(),True) \
      .add("bank_external_account",StringType(),True) \
      .add("agencia_external_account",StringType(),True) \
      .add("codigo_external_account",StringType(),True) \
      .add("type_external_account",StringType(),True) \
      .add("payment_day_ec",StringType(),True) \
      .add("last_txn_number",StringType(),True) \
      .add("charge_result",StringType(),True) \
      .add("subproduct_account_recharge",StringType(),True) \
      .add("dig_agencia_external_account",StringType(),True) \
      .add("last_process_payment",StringType(),True) \
      .add("flag_reversal_billing",StringType(),True) \
      .add("cut_days",StringType(),True)
      
      
      
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
     
     
statusSchema = StructType() \
      .add("id",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("status_id",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)
      
      
bankSchema = StructType() \
	.add("id",StringType(),True) \
	.add("bank_id",StringType(),True) \
	.add("short_desc",StringType(),True) \
	.add("long_desc",StringType(),True) \
	.add("nemotecnico",StringType(),True) \
	.add("status",StringType(),True) \
	.add("status_date",StringType(),True) \
	.add("status_reason",StringType(),True) \
	.add("stamp_user",StringType(),True) \
	.add("stamp_additional",StringType(),True) \
	.add("stamp_date_time",StringType(),True) \
	.add("official_id",StringType(),True) \
	.add("institution",StringType(),True) \
	.add("swift_code",StringType(),True) \
	.add("central_bank_code",StringType(),True) \
	.add("customer",StringType(),True) \
	.add("internal_correspondent",StringType(),True) \
	.add("external_correspondent",StringType(),True) \
	.add("corr_account",StringType(),True) \
	.add("central_bank_account",StringType(),True) \
	.add("country_swift",StringType(),True) \
	.add("finantial_institution_type",StringType(),True) \
	.add("central_bank_reporting_name",StringType(),True)
      
      
payment_typeSchema = StructType() \
    .add('id',StringType(),True) \
    .add('nemotecnico',StringType(),True) \
    .add('short_desc',StringType(),True) \
    .add('long_desc',StringType(),True) \
    .add('payment_type_id',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('official_id',StringType(),True)
    
    
    
legal_entitySchema = StructType() \
    .add('id',StringType(),True) \
    .add('customer',StringType(),True) \
    .add('central_bank_reporting_name',StringType(),True) \
    .add('company_legal_type',StringType(),True) \
    .add('legal_start_date',StringType(),True) \
    .add('legal_expiry_date',StringType(),True) \
    .add('activity_start_date',StringType(),True) \
    .add('fiscal_balance_ending_month',StringType(),True) \
    .add('fiscal_ending_balance_date',StringType(),True) \
    .add('exporting_code',StringType(),True) \
    .add('country_of_origin',StringType(),True) \
    .add('quotation_code',StringType(),True) \
    .add('board_of_directors_expiry',StringType(),True) \
    .add('management_change_date',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('activity_description',StringType(),True) \
    .add('incorporation_date',StringType(),True) \
    .add('commercial_name',StringType(),True) \
    .add('other_business_sector',StringType(),True) \
    .add('other_company_category',StringType(),True) \
    .add('other_company_type',StringType(),True) \
    .add('other_int_business_detail',StringType(),True) \
    .add('company_size',StringType(),True) \
    .add('underactivity',StringType(),True) \
    .add('business_environment',StringType(),True) \
    .add('company_business',StringType(),True) \
    .add('company_category',StringType(),True) \
    .add('int_business_detail',StringType(),True) \
    .add('folio_number',StringType(),True) \
    .add('country_legal_adrress',StringType(),True) \
    .add('company_size_points',StringType(),True) \
    .add('reason_for_change',StringType(),True)

contact_phoneSchema = StructType() \
    .add('id',StringType(),True) \
    .add('communication_id',StringType(),True) \
    .add('customer',StringType(),True) \
    .add('address',StringType(),True) \
    .add('order_phone',StringType(),True) \
    .add('communication_type',StringType(),True) \
    .add('country_phone',StringType(),True) \
    .add('telephone_type',StringType(),True) \
    .add('phone_characteristic',StringType(),True) \
    .add('area_country',StringType(),True) \
    .add('area_code',StringType(),True) \
    .add('cellular_identification',StringType(),True) \
    .add('distribution_area',StringType(),True) \
    .add('phone_number',StringType(),True) \
    .add('phone_extension',StringType(),True) \
    .add('phone_use_code',StringType(),True) \
    .add('phone_expiry_date',StringType(),True) \
    .add('phone_last_use_date',StringType(),True) \
    .add('integrity_structure',StringType(),True) \
    .add('addition_date',StringType(),True) \
    .add('purge_date',StringType(),True) \
    .add('purge_reason',StringType(),True) \
    .add('comments',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('country',StringType(),True) \
    .add('tel_for_comu',StringType(),True)



electronics_contactSchema = StructType() \
    .add('id',StringType(),True) \
    .add('customer',StringType(),True) \
    .add('email_type',StringType(),True) \
    .add('email_order',StringType(),True) \
    .add('email_use',StringType(),True) \
    .add('email_address_complete',StringType(),True) \
    .add('integrity_structure',StringType(),True) \
    .add('email_last_use',StringType(),True) \
    .add('addition_date',StringType(),True) \
    .add('purge_reason',StringType(),True) \
    .add('purge_date',StringType(),True) \
    .add('comments',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('priority',StringType(),True) \
    .add('electronics_contact_type',StringType(),True) \
    .add('electronics_contact_id',StringType(),True) \
    .add('email_for_comu',StringType(),True)

    
relation_customer_customerSchema = StructType() \
    .add('id',StringType(),True) \
    .add('customer',StringType(),True) \
    .add('customer_link',StringType(),True) \
    .add('relation_type',StringType(),True) \
    .add('date_from',StringType(),True) \
    .add('date_to',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('description',StringType(),True) \
    .add('porcentaje',StringType(),True) \
    .add('external_customer',StringType(),True) \
    .add('bank_customer_or_supplier',StringType(),True) \
    .add('notification',StringType(),True) \
    .add('address',StringType(),True) \
    .add('eletronics_contact',StringType(),True) \
    .add('contact_phone',StringType(),True) 


relation_typeSchema = StructType() \
    .add('id',StringType(),True) \
    .add('relation_type_id',StringType(),True) \
    .add('short_desc',StringType(),True) \
    .add('long_desc',StringType(),True) \
    .add('nemotecnico',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('official_id',StringType(),True) \
    .add('relation_group',StringType(),True)


addressSchema = StructType() \
    .add('id',StringType(),True) \
    .add('address_id',StringType(),True) \
    .add('customer',StringType(),True) \
    .add('address_type',StringType(),True) \
    .add('address_allocate',StringType(),True) \
    .add('geographic_zone',StringType(),True) \
    .add('street_type',StringType(),True) \
    .add('street',StringType(),True) \
    .add('street_number',StringType(),True) \
    .add('without_number_street',StringType(),True) \
    .add('street_extension',StringType(),True) \
    .add('area',StringType(),True) \
    .add('postal_office_box',StringType(),True) \
    .add('branch_postal_office_box',StringType(),True) \
    .add('town_zone',StringType(),True) \
    .add('internal_street',StringType(),True) \
    .add('internal_street_number',StringType(),True) \
    .add('floor',StringType(),True) \
    .add('apartment',StringType(),True) \
    .add('office',StringType(),True) \
    .add('highway',StringType(),True) \
    .add('highway_exit',StringType(),True) \
    .add('route',StringType(),True) \
    .add('route_kilometre',StringType(),True) \
    .add('rural_zone',StringType(),True) \
    .add('train_station_name',StringType(),True) \
    .add('locality',StringType(),True) \
    .add('zip_code',StringType(),True) \
    .add('new_zip_code',StringType(),True) \
    .add('right_street',StringType(),True) \
    .add('left_street',StringType(),True) \
    .add('property_register',StringType(),True) \
    .add('province',StringType(),True) \
    .add('province_code',StringType(),True) \
    .add('region',StringType(),True) \
    .add('country',StringType(),True) \
    .add('country_zip_code',StringType(),True) \
    .add('integrity_code',StringType(),True) \
    .add('post_destination_branch',StringType(),True) \
    .add('maps_relations',StringType(),True) \
    .add('reside_begin_date',StringType(),True) \
    .add('address_last_use_date',StringType(),True) \
    .add('return_mail_type',StringType(),True) \
    .add('last_return_mail_date',StringType(),True) \
    .add('address_expiry_date',StringType(),True) \
    .add('comments',StringType(),True) \
    .add('adition_date',StringType(),True) \
    .add('purge_date',StringType(),True) \
    .add('purge_reason',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('exact_address',StringType(),True) \
    .add('postal_box',StringType(),True) \
    .add('postal_box_branch',StringType(),True) \
    .add('parcell',StringType(),True) \
    .add('block',StringType(),True) \
    .add('house_number',StringType(),True) \
    .add('house_type',StringType(),True) \
    .add('sector',StringType(),True) \
    .add('canton',StringType(),True) \
    .add('district',StringType(),True) \
    .add('neighborhood',StringType(),True) \
    .add('locality_zip_code',StringType(),True) \
    .add('residence_type',StringType(),True) \
    .add('residence_condition',StringType(),True) \
    .add('is_for_correspondence',StringType(),True) 


province_codeSchema = StructType() \
    .add('id',StringType(),True) \
    .add('province_code_id',StringType(),True) \
    .add('short_desc',StringType(),True) \
    .add('long_desc',StringType(),True) \
    .add('nemotecnico',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('country',StringType(),True) \
    .add('official_id',StringType(),True) 


subproduct_chargeSchema = StructType() \
    .add('id',StringType(),True) \
    .add('subproduct',StringType(),True) \
    .add('charge',StringType(),True) \
    .add('amount_type',StringType(),True) \
    .add('amount_type_dest',StringType(),True) \
    .add('minimal_base',StringType(),True) \
    .add('maximum_limit',StringType(),True) \
    .add('minimal_price',StringType(),True) \
    .add('maximum_price',StringType(),True) \
    .add('fixed_price',StringType(),True) \
    .add('currency_charge',StringType(),True) \
    .add('backward_days_price',StringType(),True) \
    .add('quotation_type',StringType(),True) \
    .add('additional_price',StringType(),True) \
    .add('rate',StringType(),True) \
    .add('rate_value',StringType(),True) \
    .add('interest_spread_type',StringType(),True) \
    .add('spread_value',StringType(),True) \
    .add('algorithm',StringType(),True) \
    .add('rounded_code',StringType(),True) \
    .add('date_from',StringType(),True) \
    .add('date_to',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('transaction_reason',StringType(),True) \
    .add('currency_price',StringType(),True) \
    .add('quotation_type_price',StringType(),True) \
    .add('backward_days',StringType(),True) \
    .add('customer_behaviour_type',StringType(),True) \
    .add('channel',StringType(),True) \
    .add('has_range',StringType(),True) \
    .add('charge_result',StringType(),True) \
    .add('repass_type',StringType(),True) 



chargeSchema = StructType() \
    .add('id',StringType(),True) \
    .add('institution',StringType(),True) \
    .add('charge_id',StringType(),True) \
    .add('short_desc',StringType(),True) \
    .add('long_desc',StringType(),True) \
    .add('amount_type',StringType(),True) \
    .add('date_from',StringType(),True) \
    .add('date_to',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('system_user_core',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('status',StringType(),True) \
    .add('record_type',StringType(),True) \
    .add('stamp_user',StringType(),True) 



bonification_accountSchema = StructType() \
    .add('id',StringType(),True) \
    .add('account',StringType(),True) \
    .add('charge',StringType(),True) \
    .add('date_from',StringType(),True) \
    .add('date_to',StringType(),True) \
    .add('subproduct_package',StringType(),True) \
    .add('date_relation',StringType(),True) \
    .add('percentage_value',StringType(),True) \
    .add('fix_value',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) 



subproduct_packageSchema = StructType() \
    .add('id',StringType(),True) \
    .add('subproduct',StringType(),True) \
    .add('package_type',StringType(),True) \
    .add('allow_changes',StringType(),True) \
    .add('payment_term',StringType(),True) \
    .add('term_type',StringType(),True) \
    .add('currency',StringType(),True) \
    .add('package_price',StringType(),True) \
    .add('subproduct_package_id',StringType(),True) \
    .add('charge',StringType(),True) \
    .add('short_desc',StringType(),True) \
    .add('long_desc',StringType(),True) \
    .add('person_type',StringType(),True) \
    .add('status',StringType(),True) \
    .add('status_date',StringType(),True) \
    .add('status_reason',StringType(),True) \
    .add('stamp_user',StringType(),True) \
    .add('stamp_additional',StringType(),True) \
    .add('stamp_date_time',StringType(),True) \
    .add('is_default_package',StringType(),True) \
    .add('grace_period_term',StringType(),True) \
    .add('grace_period_type',StringType(),True) \
    .add('customer_behaviour_type',StringType(),True) \
    .add('payment_in_debit_date',StringType(),True) \
    .add('is_essential',StringType(),True) \
    .add('retry_term',StringType(),True) \
    .add('retry_type',StringType(),True) \
    .add('date_from',StringType(),True) \
    .add('date_to',StringType(),True) \
    .add('promotional_code',StringType(),True) \
    .add('allowed_accounts_qty',StringType(),True) \
    .add('customer',StringType(),True) \
    .add('transaction_reason',StringType(),True) \
    .add('account',StringType(),True) 





#leitura das tabelas
customer = sqlc.read.options(delimiter="\020").schema(customerSchema).csv("caminho da tabela")
account = sqlc.read.options(delimiter="\020").schema(accountSchema).csv("caminho da tabela")
product = sqlc.read.options(delimiter="\020").schema(productSchema).csv("caminho da tabela")
account_ext = sqlc.read.options(delimiter="\020").schema(accountExtSchema).csv("caminho da tabela")
customer_operation = sqlc.read.options(delimiter="\020").schema(customerOperationSchema).csv("caminho da tabela")
subproduct = sqlc.read.options(delimiter="\020").schema(subproductSchema).csv("caminho da tabela")
status = sqlc.read.options(delimiter="\020").schema(statusSchema).csv("caminho da tabela")
bank = sqlc.read.options(delimiter="\020").schema(bankSchema).csv("caminho da tabela")
payment_type = sqlc.read.options(delimiter="\020").schema(payment_typeSchema).csv("caminho da tabela")
legal_entity = sqlc.read.options(delimiter="\020").schema(legal_entitySchema).csv("caminho da tabela")
contact_phone = sqlc.read.options(delimiter="\020").schema(contact_phoneSchema).csv("caminho da tabela")
iso_product_type = sqlc.read.options(delimiter="\020").schema(iso_product_typeSchema).csv("caminho da tabela")
electronics_contact = sqlc.read.options(delimiter="\020").schema(electronics_contactSchema).csv("caminho da tabela")
relation_customer_customer = sqlc.read.options(delimiter="\020").schema(relation_customer_customerSchema).csv("caminho da tabela")
relation_type = sqlc.read.options(delimiter="\020").schema(relation_typeSchema).csv("caminho da tabela")
address = sqlc.read.options(delimiter="\020").schema(addressSchema).csv("caminho da tabela")
province_code = sqlc.read.options(delimiter="\020").schema(province_codeSchema).csv("caminho da tabela")
subproduct_charge = sqlc.read.options(delimiter="\020").schema(subproduct_chargeSchema).csv("caminho da tabela")
charge = sqlc.read.options(delimiter="\020").schema(chargeSchema).csv("caminho da tabela")
bonification_account = sqlc.read.options(delimiter="\020").schema(bonification_accountSchema).csv("caminho da tabela")
subproduct_package = sqlc.read.options(delimiter="\020").schema(subproduct_packageSchema).csv("caminho da tabela")
identificador = sqlc.read.json("caminho da tabela")
proposta = sqlc.read.json("caminho da tabela")





#criando views
customer.createOrReplaceTempView("customer")
account.createOrReplaceTempView("account")
product.createOrReplaceTempView("product")
account_ext.createOrReplaceTempView("account_ext")
customer_operation.createOrReplaceTempView("customer_operation")
subproduct.createOrReplaceTempView("subproduct")
status.createOrReplaceTempView("status")
bank.createOrReplaceTempView("bank")
address.createOrReplaceTempView("address")
charge.createOrReplaceTempView("charge")
payment_type.createOrReplaceTempView("payment_type")
legal_entity.createOrReplaceTempView("legal_entity")
contact_phone.createOrReplaceTempView("contact_phone")
electronics_contact.createOrReplaceTempView("electronics_contact")
relation_customer_customer.createOrReplaceTempView("relation_customer_customer")
relation_type.createOrReplaceTempView("relation_type")
province_code.createOrReplaceTempView("province_code")
subproduct_charge.createOrReplaceTempView("subproduct_charge")
bonification_account.createOrReplaceTempView("bonification_account")
subproduct_package.createOrReplaceTempView("subproduct_package")
iso_product_type.createOrReplaceTempView("iso_product_type")
proposta.createOrReplaceTempView("proposta")
identificador.createOrReplaceTempView("identificador")



#gerando dataframe principal
dados_dlc = sqlc.sql("""select
c.identification_number as cnpj
,c.id as id_cliente
,a.operation_id as id_conta
,a.opening_date as data_aprovacao
,max(p.datasituacaopropostaonline) as dataproposta
,c.name as razao_social
,c.name as nome_filial_contratante
,le.commercial_name as nomefantasia
,le.other_business_sector as nomegrupoeconomico
--,p.statusproposta as statusproposta
,REPLACE(cast(a.total_agreements  as decimal(20,2)), '.', ',') as limite_de_credito
,pt.long_desc as forma_pagamento
,case 
	when ISO.NEMOTECNICO = 'PRE PAGO' then REPLACE(cast(p.valorrecargaprepago as decimal(20,2)), '.', ',')
	else REPLACE(cast(a.total_agreements  as decimal(20,2)), '.', ',') 
end as valor_limite_ou_recarga
,REPLACE(cast(p.faturamentomediomensal as decimal(20,2)), '.', ',') as faturamento_mensal
--
,substring(p.telefoneprincipalproposta,1,2) as DDD
,substring(p.telefoneprincipalproposta,3,15) as telefonefixo
,substring(p.telefonesecundarioproposta,1,2) as DDD2
,substring(p.telefonesecundarioproposta,3,15) as telefonefixo2
,cp.area_code as DDD3
,cp.phone_number as telefonefixo3
--
,pf.name as representante_legal
,pf.cargo as nivel_cargo_representante_legal
,substring(p.telefoneprospect,1,2) as DDD_representantelegal
,substring(p.telefoneprospect,3,15) as telefone_representantelegal
,p.emailprospect as email_representantelegal
--
,case 
	when adr.street is not Null then adr.street
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.logradourosecundario
			else p.logradouroprincipal 
		end)
	end as endereco
--numero
,case 
	when adr.without_number_street is not Null then adr.without_number_street
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.numerosecundario
			else p.numeroprincipal 
		end)
	end as numero
--complemento
,case 
	when adr.exact_address is not Null then adr.exact_address
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.complementosecundario
			else p.complementoprincipal 
		end)
	end as complemento
--bairro
,case 
	when adr.town_zone is not Null then adr.town_zone
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.bairrosecundario
			else p.bairroprincipal 
		end)
	end as bairro
--cep
,case 
	when adr.country_zip_code is not Null then adr.country_zip_code
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cepsecundario
			else p.cepprincipal
		end)
	end as cep
--cidade
,case 
	when adr.locality is not Null then adr.locality
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cidadesecundario
			else p.cidadeprincipal 
		end)
	end as cidade
--UF
,case 
	when adr.short_desc is not Null then adr.short_desc
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.ufsecundario
			else p.ufprincipal 
		end)
	end as UF
--
/*,adr.street as endereco
,adr.without_number_street as numero
,adr.exact_address as complemento
,adr.town_zone as bairro
,adr.country_zip_code as cep
,adr.locality as cidade
,adr.short_desc as UF
--
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.logradourosecundario else p.logradouroprincipal end as endereco_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.numerosecundario else p.numeroprincipal end as numero_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.complementosecundario else p.complementoprincipal end as complemento_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.bairrosecundario else p.bairroprincipal end as bairro_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cepsecundario else p.cepprincipal end as cep_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cidadesecundario else p.cidadeprincipal end as cidade_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.ufsecundario else p.ufprincipal end as uf_enviotag*/
--
,p.nomecontato as nome_operador
,substring(p.telefoneprincipalproposta,1,2) as DDD_operador
,substring(p.telefoneprincipalproposta,3,15) as telefone_operador
,p.emailproposta as email_operador
,em.email_address_complete as email_customer
--
,ISO.NEMOTECNICO as plano_pre_pos
,count (i.veiculo_placa) as qtd_TAGS_ativo_inativo
,p.qtdveiculos as qtd_TAGS_proposta
--
--,REPLACE(cast(p.valorrecargaprepago as decimal(20,2)), '.', ',') as valorrecargaprepago
,case when ISO.NEMOTECNICO = 'CLIENTES_POS' then p.datadebito end as datavencimento 
--
,bk.bank_id as bank_id_pagamento
,bk.long_desc as banco_pagamento
,p.agencia as agencia_pagamento
,p.conta as conta_pagamento
,p.codigoagenciavendedor as codigoagenciavendedor
,case
    when p.codigobancovendedor RLIKE '.*(1|01|001|0001|00001).*' then '001'
    when p.codigobancovendedor RLIKE '.*(237|0237|00237).*' then '237'
    else null 
end as codigobancovendedor
,CASE 
    when p.canaldevenda = 'BACK_OFFICE' and p.codigousuario RLIKE '.*(ATENTO|DPONTE2).*' then 'TELEVENDAS'
    when p.canaldevenda = 'BACK_OFFICE' and p.codigobancovendedor RLIKE '.*(1|01|001|0001|00001).*' or p.canaldevenda RLIKE '.*(FERRAMENTABB|SEGURADORABB).*' then 'BANCO DO BRASIL'
    when p.canaldevenda = 'BACK_OFFICE' and p.codigobancovendedor RLIKE '.*(237|0237|00237).*' or p.canaldevenda RLIKE '.*(FERRAMENTABRA|SEGURADORABRA).*' then 'BRADESCO'
    when p.canaldevenda = 'WEB' then 'WEB'
    when p.canaldevenda = 'BACK_OFFICE' then 'MAR ABERTO' 
end as canaldevenda_final
,p.canaldevenda as canaldevenda_original
,p.nomeusuario as nomevendedor
,p.codigousuario as codigovendedor
--
--,p.numeroproposta as numeroproposta
--,p.datasituacaopropostaonline as data_proposta
--
,s.short_desc as status_conta
,a.operation_id as chave
--
from customer c
--
left join customer_operation co on c.id = co.customer
left join account a 
	on a.operation_id = co.operation_id
	and a.branch = co.branch
	and a.subproduct = co.subproduct
left join legal_entity le on c.id = le.customer
left join payment_type as pt on a.cnt_stop_payments_ytd = pt.id
left join proposta p on p.cnpj = c.identification_number
left join contact_phone cp on c.id = cp.customer
left join electronics_contact em on c.id = em.customer
left join status s on s.id = a.status
left join subproduct sp on ( a.subproduct = sp.id )
left join identificador i on a.operation_id = i.idconta
left join account_ext ae on ae.account = a.operation_id
left join bank bk on ae.bank_external_account = bk.id
left join product pr ON A.PRODUCT = Pr.ID
left join iso_product_type ISO ON Pr.ISO_PRODUCT = ISO.ID
--
left join(SELECT
	c.identification_number as identification_number
	,rcc.id as id
	,rcc.customer as customer
	,rcc.customer_link as customer_link
	,c.name as name
	,rt.long_desc as cargo
	--
	from customer c
	--
	left join relation_customer_customer rcc on rcc.customer_link = c.id
	left join relation_type rt on rt.id = rcc.relation_type ) pf on pf.customer = c.id
left join(SELECT
	c.identification_number
	,ad.street
	,ad.without_number_street
	,ad.exact_address
	,ad.town_zone
	,ad.country_zip_code
	,ad.locality
	,pcd.short_desc
	--
	from customer c
	--
	left join address ad on ad.customer = c.id
	left join province_code pcd on ad.province_code = pcd.id
	--
	where ad.is_for_correspondence = 1) adr on adr.identification_number = c.identification_number
--
where ISO.NEMOTECNICO IN ('CLIENTES_PRE', 'CLIENTES_POS') 
and c.identification_type = 1
and a.opening_date > date_add(current_date(), -2)
and c.identification_number is not null
--
group by
c.identification_number
,c.id
,a.operation_id
,a.opening_date
,c.name
,le.commercial_name
,le.other_business_sector
--,p.statusproposta
,REPLACE(cast(a.total_agreements  as decimal(20,2)), '.', ',')
,pt.long_desc
,case 
	when ISO.NEMOTECNICO = 'PRE PAGO' then REPLACE(cast(p.valorrecargaprepago as decimal(20,2)), '.', ',')
	else REPLACE(cast(a.total_agreements  as decimal(20,2)), '.', ',') 
end
,REPLACE(cast(p.faturamentomediomensal as decimal(20,2)), '.', ',')
--
,substring(p.telefoneprincipalproposta,1,2)
,substring(p.telefoneprincipalproposta,3,15)
,substring(p.telefonesecundarioproposta,1,2)
,substring(p.telefonesecundarioproposta,3,15)
,cp.area_code
,cp.phone_number
--
,pf.name
,pf.cargo
,substring(p.telefoneprospect,1,2)
,substring(p.telefoneprospect,3,15)
,p.emailprospect
--
,case 
	when adr.street is not Null then adr.street
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.logradourosecundario
			else p.logradouroprincipal 
		end)
	end
--numero
,case 
	when adr.without_number_street is not Null then adr.without_number_street
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.numerosecundario
			else p.numeroprincipal 
		end)
	end
--complemento
,case 
	when adr.exact_address is not Null then adr.exact_address
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.complementosecundario
			else p.complementoprincipal 
		end)
	end
--bairro
,case 
	when adr.town_zone is not Null then adr.town_zone
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.bairrosecundario
			else p.bairroprincipal 
		end)
	end
--cep
,case 
	when adr.country_zip_code is not Null then adr.country_zip_code
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cepsecundario
			else p.cepprincipal
		end)
	end
--cidade
,case 
	when adr.locality is not Null then adr.locality
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cidadesecundario
			else p.cidadeprincipal 
		end)
	end
--UF
,case 
	when adr.short_desc is not Null then adr.short_desc
	else 
		(case 
			when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.ufsecundario
			else p.ufprincipal 
		end)
	end
--
/*,adr.street as endereco
,adr.without_number_street as numero
,adr.exact_address as complemento
,adr.town_zone as bairro
,adr.country_zip_code as cep
,adr.locality as cidade
,adr.short_desc as UF
--
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.logradourosecundario else p.logradouroprincipal end as endereco_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.numerosecundario else p.numeroprincipal end as numero_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.complementosecundario else p.complementoprincipal end as complemento_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.bairrosecundario else p.bairroprincipal end as bairro_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cepsecundario else p.cepprincipal end as cep_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.cidadesecundario else p.cidadeprincipal end as cidade_enviotag
,case when p.enderecocorrespondencia in ('OUTRO', 'Outro endereço') then p.ufsecundario else p.ufprincipal end as uf_enviotag*/
--
,p.nomecontato
,substring(p.telefoneprincipalproposta,1,2)
,substring(p.telefoneprincipalproposta,3,15)
,p.emailproposta
,em.email_address_complete
--
,ISO.NEMOTECNICO
,p.qtdveiculos
--
--,REPLACE(cast(p.valorrecargaprepago as decimal(20,2)), '.', ',') as valorrecargaprepago
,case when ISO.NEMOTECNICO = 'CLIENTES_POS' then p.datadebito end
--
,bk.bank_id
,bk.long_desc
,p.agencia
,p.conta
,p.codigoagenciavendedor
,case
    when p.codigobancovendedor RLIKE '.*(1|01|001|0001|00001).*' then '001'
    when p.codigobancovendedor RLIKE '.*(237|0237|00237).*' then '237'
    else null 
end
,CASE 
    when p.canaldevenda = 'BACK_OFFICE' and p.codigousuario RLIKE '.*(ATENTO|DPONTE2).*' then 'TELEVENDAS'
    when p.canaldevenda = 'BACK_OFFICE' and p.codigobancovendedor RLIKE '.*(1|01|001|0001|00001).*' or p.canaldevenda RLIKE '.*(FERRAMENTABB|SEGURADORABB).*' then 'BANCO DO BRASIL'
    when p.canaldevenda = 'BACK_OFFICE' and p.codigobancovendedor RLIKE '.*(237|0237|00237).*' or p.canaldevenda RLIKE '.*(FERRAMENTABRA|SEGURADORABRA).*' then 'BRADESCO'
    when p.canaldevenda = 'WEB' then 'WEB'
    when p.canaldevenda = 'BACK_OFFICE' then 'MAR ABERTO' 
end
,p.canaldevenda
,p.nomeusuario
,p.codigousuario 
--
--,p.numeroproposta as numeroproposta
,p.datasituacaopropostaonline
--
,s.short_desc
order by p.datasituacaopropostaonline desc, a.opening_date desc
""")


dados_dlb = sqlc.sql("""select 
--c.identification_number
a.operation_id as key
--a.operation_id as id_conta
,ba.date_from as data_de
,ba.date_to as data_ate
,max(ba.status_date) as status_date_bon
,pk.promotional_code as codigo_promocional
--,pk.long_desc as descricao_pacote
,REPLACE(cast(ba.fix_value as decimal(20,2)), '.', ',') as valor_desconto
,CASE 
WHEN ba.percentage_value = '100' THEN 'BONIFICACAO'
WHEN ba.fix_value is not null THEN 'DESCONTO' 
else null end As tipo_bon
,CASE WHEN ba.percentage_value = '100' THEN 0.00 ELSE REPLACE(cast((sch.fixed_price - ba.fix_value) as decimal(20,2)), '.', ',') end as tarifa_com_desconto
,CASE WHEN ba.percentage_value = '100' THEN cast(months_between(last_day(ba.date_to), last_day(ba.date_from)) as int) end as meses_isencao 
,ba.percentage_value as porcentagem_desconto
,a.operation_id as chave1
--,sch.fixed_price as tarifa_mensal_sem_desconto
--
from account a
--
left join product pr on (pr.id = a.product)
left join subproduct sp on ( a.subproduct = sp.id )
left join subproduct_charge sch on (sp.id = sch.subproduct)
left join charge ch on (ch.ID = sch.charge)
left JOIN BONIFICATION_ACCOUNT BA 
	ON A.ID = BA.ACCOUNT 
	and sch.charge = BA.charge
left JOIN subproduct_package pk ON ba.subproduct_package = pk.id
left join status st on ba.status = st.id
where pk.long_desc <> 'ISENÇÃO INATIVAÇÃO'
	and ba.date_to >= CURRENT_DATE
	AND ba.date_from <= CURRENT_DATE
	and ba.status <> 22
group by
--c.identification_number
a.operation_id
,ba.date_from
,ba.date_to
,pk.promotional_code
,pk.long_desc
,ba.fix_value
,REPLACE(cast(ba.fix_value as decimal(20,2)), '.', ',')
,CASE 
WHEN ba.percentage_value = '100' THEN 'BONIFICACAO'
WHEN ba.fix_value is not null THEN 'DESCONTO' 
else null end
,CASE WHEN ba.percentage_value = '100' THEN 0.00 ELSE REPLACE(cast((sch.fixed_price - ba.fix_value) as decimal(20,2)), '.', ',') end
,CASE WHEN ba.percentage_value = '100' THEN cast(months_between(last_day(ba.date_to), last_day(ba.date_from)) as int) end 
,ba.percentage_value
""")


dados_dlt = sqlc.sql("""select distinct
--c.identification_number as cnpj,
a.operation_id as key1,
--a.operation_id as id_conta,
pr.short_desc as produto,
sp.short_desc as plano,
ch.short_desc as encargo,
REPLACE(cast(sch.fixed_price as decimal(20,2)), '.', ',') as tarifa_mensal_sem_desconto
--
from account a
--
left join product pr on (pr.id = a.product)
left join subproduct sp on ( a.subproduct = sp.id )
left join subproduct_charge sch on (sp.id = sch.subproduct)
left join charge ch on (ch.ID = sch.charge)
left JOIN BONIFICATION_ACCOUNT BA 
	ON A.ID = BA.ACCOUNT 
	and sch.charge = BA.charge
where ba.charge in (182, 163, 102, 21, 162)
""")


# remove duplicates according to a.opening_date - filters the most recent account

w_c=Window.partitionBy(dados_dlc.cnpj,dados_dlc.id_cliente,dados_dlc.id_conta, dados_dlc.data_aprovacao).orderBy(dados_dlc.dataproposta, dados_dlc.data_aprovacao.desc()) 

dados_dlcC = dados_dlc.withColumn("row_number",f.row_number().over(w_c)).filter(f.col("row_number")==1)


# merge dfs

df = dados_dlcC.join(dados_dlb, dados_dlcC.chave==dados_dlb.key, how='left')

df2 = df.join(dados_dlt, df.chave1==dados_dlt.key1, how='left')


# drop supporting columns

df2 = df2.drop("chave1")

df2 = df2.drop("key1")

df2 = df2.drop("chave")

df2 = df2.drop("key")

df2 = df2.drop("row_number")

df2 = df2.drop("produto")


# reorder by a.opening_date asc

df2.createOrReplaceTempView("df2")

df2_order = sqlc.sql("""select * from df2 order by df2.data_aprovacao""")



#tratando valores nulos

df3 = df2_order.replace('\\N','')

# reorder columns

columnsTitles = ['cnpj',
'id_cliente',
'id_conta',
'data_aprovacao',
'status_conta',
'razao_social',
'nome_filial_contratante',
'nomefantasia',
'nomegrupoeconomico',
'faturamento_mensal',
'representante_legal',
'nivel_cargo_representante_legal',
'DDD_representantelegal',
'telefone_representantelegal',
'email_representantelegal',
'nome_operador',
'DDD_operador',
'telefone_operador',
'email_operador',
'email_customer',
'DDD',
'telefonefixo',
'DDD2',
'telefonefixo2',
'DDD3',
'telefonefixo3',
'endereco',
'numero',
'complemento',
'bairro',
'cep',
'cidade',
'UF',
'qtd_TAGS_ativo_inativo',
'qtd_TAGS_proposta',
'datavencimento',
'plano_pre_pos',
'limite_de_credito',
'forma_pagamento',
'valor_limite_ou_recarga',
'data_de',
'data_ate',
'status_date_bon',
'codigo_promocional',
'valor_desconto',
'tipo_bon',
'tarifa_com_desconto',
'meses_isencao',
'porcentagem_desconto',
'tarifa_mensal_sem_desconto',
'plano',
'encargo',
'bank_id_pagamento',
'banco_pagamento',
'agencia_pagamento',
'conta_pagamento',
'codigoagenciavendedor',
'codigobancovendedor',
'canaldevenda_final',
'canaldevenda_original',
'nomevendedor',
'codigovendedor']

df3 = df3.select(columnsTitles)



### PAX ###


filedata = datetime.now() + timedelta(days=-1) 
filedata = pd.to_datetime(filedata) 
data = filedata.strftime('%Y%m%d') 

filename = "Ativacao_PJ_{}.xlsx".format(data)
df3.toPandas().to_excel(filename, index=False, encoding='utf-8-sig', header=True)

# Troca o path
credential_provider_path = "caminho do hdfs e sftp"
credential_name = "nome da credencial"


# Recupera a senha e armazena na variavel "credential_pass"
#conf = spark.sparkContext._jsc.hadoopConfiguration()
conf = spark._jsc.hadoopConfiguration()
conf.set("hadoop.security.credential.provider.path",credential_provider_path)
credential_raw = conf.getPassword(credential_name)
credential_pass = "senha da credencial"
for i in range(credential_raw.__len__()):
    credential_pass = credential_pass + str(credential_raw.__getitem__(i))

#Setando credenciais SFTP
myHostname = "nome do host"
myUsername = "seu User"
cnopts = pysftp.CnOpts(knownhosts="known_hosts")
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


remotePath = "/ativacao_pj_backoffice/out"

conn = pysftp.Connection(host=myHostname, username=myUsername, password=credential_pass,cnopts=cnopts)
with conn.cd(remotePath):
    conn.put(filename)

### PAX ###
subprocess.call(["hdfs", "dfs", "-rm", "-f", filename])
