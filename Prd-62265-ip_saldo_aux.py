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

txnHistSchema = StructType() \
      .add("id",StringType(),True) \
      .add("account",StringType(),True) \
      .add("imputation_date",StringType(),True) \
      .add("imputation_time",StringType(),True) \
      .add("iso_product",StringType(),True) \
      .add("product_is_own",StringType(),True) \
      .add("business_sector",StringType(),True) \
      .add("source_iso_product",StringType(),True) \
      .add("msg_type_id",StringType(),True) \
      .add("transaction_reason",StringType(),True) \
      .add("pure_action",StringType(),True) \
      .add("txn_action_code",StringType(),True) \
      .add("txn_description",StringType(),True) \
      .add("reference_number",StringType(),True) \
      .add("child_sequence_nbr",StringType(),True) \
      .add("original_reference_number",StringType(),True) \
      .add("trace_number",StringType(),True) \
      .add("check_number",StringType(),True) \
      .add("summary_detail",StringType(),True) \
      .add("amount",StringType(),True) \
      .add("business_date",StringType(),True) \
      .add("issue_business_date",StringType(),True) \
      .add("source_date",StringType(),True) \
      .add("source_time",StringType(),True) \
      .add("valued_date",StringType(),True) \
      .add("user_id",StringType(),True) \
      .add("supervisor_user",StringType(),True) \
      .add("terminal_unit_id",StringType(),True) \
      .add("terminal",StringType(),True) \
      .add("terminal_type",StringType(),True) \
      .add("channel",StringType(),True) \
      .add("origin_branch",StringType(),True) \
      .add("service",StringType(),True) \
      .add("quot_applied",StringType(),True) \
      .add("converted_amount",StringType(),True) \
      .add("exchange_ticket_id",StringType(),True) \
      .add("currency",StringType(),True) \
      .add("currency_code_out",StringType(),True) \
      .add("is_cash",StringType(),True) \
      .add("is_siter",StringType(),True) \
      .add("post_pending",StringType(),True) \
      .add("off_line_form_id",StringType(),True) \
      .add("amount_type",StringType(),True) \
      .add("moment_code",StringType(),True) \
      .add("term_code",StringType(),True) \
      .add("key_other_system",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("source_subproduct",StringType(),True) \
      .add("source_branch",StringType(),True) \
      .add("source_operation_id",StringType(),True) \
      .add("source_currency_code",StringType(),True) \
      .add("last_page_generated",StringType(),True) \
      .add("line_stt_number",StringType(),True) \
      .add("last_stt_balance",StringType(),True) \
      .add("mov_customer",StringType(),True) \
      .add("team",StringType(),True) \
      .add("prty",StringType(),True) \
      .add("comments",StringType(),True) \
      .add("category",StringType(),True) \
      .add("date_from",StringType(),True) \
      .add("date_to",StringType(),True) \
      .add("root_subproduct",StringType(),True) \
      .add("root_branch",StringType(),True) \
      .add("root_operation_id",StringType(),True) \
      .add("root_iso_product",StringType(),True) \
      .add("root_currency",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("processed_by_rewards",StringType(),True) \
      .add("mov_responder",StringType(),True)

txnHistExtSchema = StructType() \
      .add("id",StringType(),True) \
      .add("txn_hist",StringType(),True) \
      .add("event_date",StringType(),True) \
      .add("repasse_date",StringType(),True) \
      .add("origen_transaction",StringType(),True) \
      .add("fast_id",StringType(),True) \
      .add("dependentes",StringType(),True) \
      .add("comments2",StringType(),True) \
      .add("comments3",StringType(),True) \
      .add("processing_date",StringType(),True) \
      .add("lot_hist",StringType(),True) \
      .add("txn_group",StringType(),True) \
      .add("team",StringType(),True) \
      .add("lot_process_date",StringType(),True) \
      .add("nsu",StringType(),True) \
      .add("process_flag",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("nsu_origem",StringType(),True) \
      .add("end_event_date",StringType(),True) \
      .add("moviment_fast",StringType(),True) \
      .add("billing",StringType(),True)

transactionReasonSchema = StructType() \
      .add('id',StringType(),True) \
      .add('transaction_core',StringType(),True) \
      .add('reason',StringType(),True) \
      .add('long_desc',StringType(),True) \
      .add('short_desc',StringType(),True) \
      .add('pure_action',StringType(),True) \
      .add('txn_action',StringType(),True) \
      .add('statement_desc',StringType(),True) \
      .add('atm_desc',StringType(),True) \
      .add('network_desc',StringType(),True) \
      .add('interbranch_allowed',StringType(),True) \
      .add('account_required',StringType(),True) \
      .add('is_cash',StringType(),True) \
      .add('txn_central_bank_code',StringType(),True) \
      .add('pin_pad_mode',StringType(),True) \
      .add('cmc7_id',StringType(),True) \
      .add('on_line_form_id',StringType(),True) \
      .add('off_line_form_id',StringType(),True) \
      .add('panel_id',StringType(),True) \
      .add('max_iteration',StringType(),True) \
      .add('overdraft_mode',StringType(),True) \
      .add('genuine_balance',StringType(),True) \
      .add('origin_mode',StringType(),True) \
      .add('opportunity',StringType(),True) \
      .add('allowed_date',StringType(),True) \
      .add('amount_type',StringType(),True) \
      .add('other_currency',StringType(),True) \
      .add('history_pass',StringType(),True) \
      .add('is_siter',StringType(),True) \
      .add('update_txn_2_update',StringType(),True) \
      .add('update_int_db_date',StringType(),True) \
      .add('update_ext_db_date',StringType(),True) \
      .add('update_int_cr_date',StringType(),True) \
      .add('update_ext_cr_date',StringType(),True) \
      .add('update_withdrawal_date',StringType(),True) \
      .add('update_today_cash_withdrawal',StringType(),True) \
      .add('update_today_cash_deposits',StringType(),True) \
      .add('update_today_other_db',StringType(),True) \
      .add('update_today_other_cr',StringType(),True) \
      .add('imp_deb_code',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('has_roe',StringType(),True) \
      .add('reserve_balance_flag',StringType(),True) \
      .add('unavailable_balance_flag',StringType(),True) \
      .add('block_adm_cr_flag',StringType(),True) \
      .add('block_adm_db_flag',StringType(),True) \
      .add('allows_retry_flag',StringType(),True) \
      .add('apply_current_balance_flag',StringType(),True) \
      .add('issues_insurance_flag',StringType(),True) \
      .add('team',StringType(),True) \
      .add('prty',StringType(),True) \
      .add('accounting_code',StringType(),True) \
      .add('txn_group',StringType(),True) \
      .add('send_txn_warn_flag',StringType(),True) \
      .add('shadow_limit_flag',StringType(),True) \
      .add('release_category',StringType(),True) \
      .add('release_form',StringType(),True) \
      .add('rwd_transaction_detail',StringType(),True) \
      .add('txn_category',StringType(),True) \
      .add('txn_nature',StringType(),True)

movimentFastSchema = StructType() \
      .add('id',StringType(),True) \
      .add('secuencia',StringType(),True) \
      .add('fast_id',StringType(),True) \
      .add('cyber_id',StringType(),True) \
      .add('compesation_date',StringType(),True) \
      .add('origin_system',StringType(),True) \
      .add('recarga_amount',StringType(),True) \
      .add('operation_id_custom',StringType(),True) \
      .add('balance_operational',StringType(),True) \
      .add('balance_today',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('error_code',StringType(),True) \
      .add('error_description',StringType(),True) \
      .add('nsu',StringType(),True) \
      .add('activity',StringType(),True) \
      .add('status_extrato',StringType(),True) \
      .add('operation_id_comer',StringType(),True) \
      .add('amount',StringType(),True) \
      .add('process_date_time',StringType(),True) \
      .add('event_date_time',StringType(),True) \
      .add('comments1',StringType(),True) \
      .add('comments2',StringType(),True) \
      .add('comments3',StringType(),True) \
      .add('end_event_date',StringType(),True) \
      .add('id_transaction_ec',StringType(),True) \
      .add('repasse_date',StringType(),True) \
      .add('dependente_id',StringType(),True) \
      .add('estorno_flag',StringType(),True) \
      .add('reversal_flag',StringType(),True) \
      .add('estorno_eft_flag',StringType(),True) \
      .add('cyber_id_estorno',StringType(),True) \
      .add('factor',StringType(),True)

transactionReasonActivitySchema = StructType() \
      .add('id',StringType(),True) \
      .add('transaction_reason',StringType(),True) \
      .add('activity',StringType(),True) \
      .add('system_user_core',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True)

activitySchema = StructType() \
      .add('id',StringType(),True) \
      .add('institution',StringType(),True) \
      .add('activity_code_id',StringType(),True) \
      .add('nemotecnico',StringType(),True) \
      .add('activity_type',StringType(),True) \
      .add('fare_flag',StringType(),True) \
      .add('agenda_flag',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('system_user_core',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('short_desc',StringType(),True) \
      .add('long_desc',StringType(),True) \
      .add('official_id',StringType(),True) \
      .add('nsu_flag',StringType(),True)	

txnGroupSchema = StructType() \
      .add('id',StringType(),True) \
      .add('nemotecnico',StringType(),True) \
      .add('short_desc',StringType(),True) \
      .add('long_desc',StringType(),True) \
      .add('txn_group_id',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('official_id',StringType(),True) \
      .add('team',StringType(),True)

activityRelatedSchema = StructType() \
      .add('id',StringType(),True) \
      .add('activity',StringType(),True) \
      .add('product',StringType(),True) \
      .add('subproduct',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('system_user_core',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True)

accountHistorySchema = StructType() \
      .add('id',StringType(),True) \
      .add('account',StringType(),True) \
      .add('hist_date',StringType(),True) \
      .add('currency',StringType(),True) \
      .add('last_bal_chg',StringType(),True) \
      .add('adjusted_balance',StringType(),True) \
      .add('debit_mode_type',StringType(),True) \
      .add('accrual_suspended_flag',StringType(),True) \
      .add('db_days_without_agreement',StringType(),True) \
      .add('db_balance_start_date',StringType(),True) \
      .add('db_balance_total_days',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('unavailable_balance',StringType(),True) \
      .add('deposits24_hrs',StringType(),True) \
      .add('deposits48_hrs',StringType(),True) \
      .add('reserve_balance',StringType(),True) \
      .add('amount_block_db',StringType(),True) \
      .add('amount_block_cr',StringType(),True) \
      .add('available_balance',StringType(),True) \
      .add('available_balance_without_agr',StringType(),True) \
      .add('db_balance_to_accrual',StringType(),True) \
      .add('total_agreements',StringType(),True) \
      .add('blocked_balance',StringType(),True) \
      .add('dep_available_today',StringType(),True) \
      .add('doc_amount',StringType(),True) \
      .add('checks24_hrs',StringType(),True)

iso_product_type = sqlc.read.options(delimiter="\020").schema(isoProductTypeSchema).csv("caminho da tabela")
subproduct = sqlc.read.options(delimiter="\020").schema(transactionReasonSchema).csv("caminho da tabela")
product = sqlc.read.options(delimiter="\020").schema(productSchema).csv("caminho da tabela")
account = sqlc.read.options(delimiter="\020").schema(accountSchema).csv("caminho da tabela")
customer_operation = sqlc.read.options(delimiter="\020").schema(customerOperationSchema).csv("caminho da tabela")
customer = sqlc.read.options(delimiter="\020").schema(customerSchema).csv("caminho da tabela")
txn_hist = sqlc.read.options(delimiter="\020").schema(txnHistSchema).csv("caminho da tabela")
txn_hist_ext = sqlc.read.options(delimiter="\020").schema(txnHistExtSchema).csv("caminho da tabela")
transaction_reason = sqlc.read.options(delimiter="\020").schema(transactionReasonSchema).csv("caminho da tabela")
moviment_fast = sqlc.read.options(delimiter="\020").schema(movimentFastSchema).csv("caminho da tabela")
transaction_reason_activity = sqlc.read.options(delimiter="\020").schema(transactionReasonActivitySchema).csv("caminho da tabela")
activity = sqlc.read.options(delimiter="\020").schema(activitySchema).csv("caminho da tabela")
txn_group = sqlc.read.options(delimiter="\020").schema(txnGroupSchema).csv("caminho da tabela")
activity_related = sqlc.read.options(delimiter="\020").schema(activityRelatedSchema).csv("caminho da tabela")
account_history = sqlc.read.options(delimiter="\020").schema(accountHistorySchema).csv("caminho da tabela")

iso_product_type.createOrReplaceTempView('iso_product_type')
subproduct.createOrReplaceTempView('subproduct')
product.createOrReplaceTempView('product')
account.createOrReplaceTempView('account')
customer_operation.createOrReplaceTempView('customer_operation')
customer.createOrReplaceTempView('customer')
txn_hist.createOrReplaceTempView('txn_hist')
txn_hist_ext.createOrReplaceTempView('txn_hist_ext')
transaction_reason.createOrReplaceTempView('transaction_reason')
moviment_fast.createOrReplaceTempView('moviment_fast')
transaction_reason_activity.createOrReplaceTempView('transaction_reason_activity')
activity.createOrReplaceTempView('activity')
txn_group.createOrReplaceTempView('txn_group')
activity_related.createOrReplaceTempView('activity_related')
account_history.createOrReplaceTempView('account_history')

df = sqlc.sql("""select
    `a`.`operation_id` as `cd_cta_pagto`,
    date_Format(`mv`.`event_date_time`, 'YYYYMM') as `dt_mes_ano`,
    CURRENT_DATE as `dt_base`,
    sum(`ah`.`available_balance_without_agr`) as `vl_saldo_reais`,
    0 as `vl_saque_reais`,
    sum(if(`txg`.`nemotecnico` = 'CONSUMOPOS', `th`.`amount`, 0)) as `vl_despesas_reais`,
    sum(if(`txg`.`nemotecnico` = 'RECARGA', `th`.`amount`, 0)) as `vl_aportes_reais`,
    sum(if(`txg`.`nemotecnico` = 'DEBITO', `th`.`amount`, IF (`txg`.`nemotecnico` = 'CREDITO', `th`.`amount`, 0))) as `vl_pagto_reais`
from
    `iso_product_type` `iso`
inner join `product` `p` on
    ( `iso`.`id` = `p`.`iso_product` )
inner join`account` `a` on
    ( `p`.`id` = `a`.`product` )
inner join `txn_hist` `th` on
    ( `a`.`id` = `th`.`account` )
inner join `transaction_reason` `tr` on
    ( `tr`.`id` = `th`.`transaction_reason` )
inner join `txn_hist_ext` `te` on
    ( `th`.`id` = `te`.`txn_hist` )
inner join `moviment_fast` `mv` on
    ( `te`.`moviment_fast` = `mv`.`id` )
inner join `customer_operation` `co` on
    ( `a`.`subproduct` = `co`.`subproduct`
    and `a`.`branch` = `co`.`branch`
    and `a`.`currency` = `co`.`currency`
    and `a`.`operation_id` = `co`.`operation_id` )
inner join `customer` `c` on
    ( `co`.`customer` = `c`.`id` )
inner join `subproduct` `sp` on
    ( `a`.`subproduct` = `sp`.`id` )
inner join `transaction_reason_activity` `tra` on
    ( `tra`.`transaction_reason` = `tr`.`id` )
inner join `activity` `act` on
    ( `tra`.`activity` = `act`.`id` )
inner join `txn_group` `txg` on
    (`te`.`txn_group` = `txg`.`id`)
left join `activity_related` `acr` on
    ( `acr`.`activity` = `act`.`id` )
left join `account` `a2` on
    ( `a2`.`operation_id` = `mv`.`operation_id_comer` )
left join `customer_operation` `co2` on
    ( `a2`.`subproduct` = `co2`.`subproduct`
    and `a2`.`branch` = `co2`.`branch`
    and `a2`.`currency` = `co2`.`currency`
    and `a2`.`operation_id` = `co2`.`operation_id` )
left join `customer` `c2` on
    ( `co2`.`customer` = `c2`.`id` )
left join `subproduct` `sb2` on
    ( `acr`.`subproduct` = `sb2`.`id` )
left join `account_history` `ah` on
    (`ah`.`account` = `a`.`id`
    and `ah`.`hist_date` = last_day(`mv`.`event_date_time`))
where
    `iso`.`nemotecnico` in ('CLIENTES_POS', 'CLIENTES_PRE')
    and `txg`.`nemotecnico` in ('RECARGA', 'DEBITO', 'CREDITO', 'CONSUMOPOS')
group by
    `a`.`operation_id`,
    date_Format(`mv`.`event_date_time`, 'YYYYMM')""")

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
dfp.to_sql(con= my_con, name = "ip_saldo_aux", if_exists ="replace", index = False)