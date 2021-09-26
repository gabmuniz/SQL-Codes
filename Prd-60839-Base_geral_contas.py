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

customerDetailSchema = StructType() \
      .add('id',StringType(),True) \
      .add('customer',StringType(),True) \
      .add('is_related',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('provider_service_type',StringType(),True) \
      .add('sugef_position_type',StringType(),True) \
      .add('sugef_position',StringType(),True) \
      .add('staff_opening_hours',StringType(),True) \
      .add('staff_closing_hours',StringType(),True) \
      .add('official_economic_sector',StringType(),True) \
      .add('branch_opening_hours',StringType(),True) \
      .add('branch_closing_hours',StringType(),True) \
      .add('cooperation_level',StringType(),True) \
      .add('eml_sector',StringType(),True) \
      .add('cetip',StringType(),True) \
      .add('selic',StringType(),True) \
      .add('adherence_to_register',StringType(),True) \
      .add('authorize_bacen',StringType(),True) \
      .add('authorize_pcam',StringType(),True) \
      .add('date_from_financial_system',StringType(),True) \
      .add('tax_document_type',StringType(),True) \
      .add('cod_oper_cvm',StringType(),True) \
      .add('communication_type',StringType(),True) \
      .add('treatment_type',StringType(),True) \
      .add('treatment_name',StringType(),True) \
      .add('dda',StringType(),True) \
      .add('digital_certificate',StringType(),True) \
      .add('opportunity_type',StringType(),True) \
      .add('visit_information',StringType(),True) \
      .add('segment',StringType(),True) \
      .add('date_renovacion',StringType(),True) \
      .add('date_update_segment_legal',StringType(),True) \
      .add('segment_legal',StringType(),True) \
      .add('user_update_segment_legal',StringType(),True) \
      .add('campaign_origination',StringType(),True) \
      .add('business_segment',StringType(),True) \
      .add('overdue_renovation_date',StringType(),True) \
      .add('fiscal_note_indicator',StringType(),True)

paymentDaySchema = StructType() \
      .add("id",StringType(),True) \
      .add("payment_day_id",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)

paymentTypeSchema = StructType() \
      .add("id",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("payment_type_id",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)

subproductPackageSchema = StructType() \
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

bonificationAccountSchema = StructType() \
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

basicIndividualDataSchema = StructType() \
      .add('id',StringType(),True) \
      .add('customer',StringType(),True) \
      .add('country_of_birth',StringType(),True) \
      .add('birth_date',StringType(),True) \
      .add('marital_status',StringType(),True) \
      .add('sex_code',StringType(),True) \
      .add('education_level',StringType(),True) \
      .add('education_ending_date',StringType(),True) \
      .add('institution_profession',StringType(),True) \
      .add('non_commercial_activity',StringType(),True) \
      .add('family_size',StringType(),True) \
      .add('decease_date',StringType(),True) \
      .add('system_user_core',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('province_of_birth',StringType(),True) \
      .add('number_of_children',StringType(),True) \
      .add('school_name',StringType(),True) \
      .add('foreign_type',StringType(),True) \
      .add('profession',StringType(),True) \
      .add('occupation',StringType(),True) \
      .add('place_of_birth',StringType(),True) \
      .add('nationality',StringType(),True) \
      .add('mother_name',StringType(),True) \
      .add('father_name',StringType(),True) \
      .add('status_education',StringType(),True) \
      .add('locality_zip_code',StringType(),True)

identificationTypeSchema = StructType() \
      .add("id",StringType(),True) \
      .add("identification_type_id",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)

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

customerOperationTypeSchema = StructType() \
      .add("id",StringType(),True) \
      .add("customer_operation_type_id",StringType(),True) \
      .add("short_desc",StringType(),True) \
      .add("long_desc",StringType(),True) \
      .add("nemotecnico",StringType(),True) \
      .add("status",StringType(),True) \
      .add("status_date",StringType(),True) \
      .add("status_reason",StringType(),True) \
      .add("stamp_user",StringType(),True) \
      .add("stamp_additional",StringType(),True) \
      .add("stamp_date_time",StringType(),True) \
      .add("official_id",StringType(),True)

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
identification_type = sqlc.read.options(delimiter="\020").schema(identificationTypeSchema).csv("caminho da tabela")
customer_operation = sqlc.read.options(delimiter="\020").schema(customerOperationSchema).csv("caminho da tabela")
account = sqlc.read.options(delimiter="\020").schema(accountSchema).csv("caminho da tabela")
customer_operation_type = sqlc.read.options(delimiter="\020").schema(customerOperationTypeSchema).csv("caminho da tabela")
status = sqlc.read.options(delimiter="\020").schema(statusSchema).csv("caminho da tabela")
basic_individual_data = sqlc.read.options(delimiter="\020").schema(basicIndividualDataSchema).csv("caminho da tabela")
address = sqlc.read.options(delimiter="\020").schema(addressSchema).csv("caminho da tabela")
electronics_contact = sqlc.read.options(delimiter="\020").schema(electronicsContactSchema).csv("caminho da tabela")
contact_phone = sqlc.read.options(delimiter="\020").schema(contactPhoneSchema).csv("caminho da tabela")
locality = sqlc.read.options(delimiter="\020").schema(localitySchema).csv("caminho da tabela")
province_code = sqlc.read.options(delimiter="\020").schema(provinceCodeSchema).csv("caminho da tabela")
product = sqlc.read.options(delimiter="\020").schema(productSchema).csv("caminho da tabela")
iso_product_type = sqlc.read.options(delimiter="\020").schema(isoProductTypeSchema).csv("caminho da tabela")
subproduct = sqlc.read.options(delimiter="\020").schema(subproductSchema).csv("caminho da tabela")
payment_type = sqlc.read.options(delimiter="\020").schema(paymentTypeSchema).csv("caminho da tabela")
payment_day = sqlc.read.options(delimiter="\020").schema(paymentDaySchema).csv("caminho da tabela")
customer_detail = sqlc.read.options(delimiter="\020").schema(customerDetailSchema).csv("caminho da tabela")
bonification_account = sqlc.read.options(delimiter="\020").schema(bonificationAccountSchema).csv("caminho da tabela")
subproduct_package = sqlc.read.options(delimiter="\020").schema(subproductPackageSchema).csv("caminho da tabela")
legal_entity = sqlc.read.options(delimiter="\020").schema(legalEntitySchema).csv("caminho da tabela")

customer.createOrReplaceTempView("customer")
identification_type.createOrReplaceTempView("identification_type")
customer_operation.createOrReplaceTempView("customer_operation")
account.createOrReplaceTempView("account")
customer_operation_type.createOrReplaceTempView("customer_operation_type")
status.createOrReplaceTempView("status")
basic_individual_data.createOrReplaceTempView("basic_individual_data")
address.createOrReplaceTempView("address")
electronics_contact.createOrReplaceTempView("electronics_contact")
contact_phone.createOrReplaceTempView("contact_phone")
subproduct.createOrReplaceTempView("subproduct")
locality.createOrReplaceTempView("locality")
province_code.createOrReplaceTempView("province_code")
product.createOrReplaceTempView("product")
iso_product_type.createOrReplaceTempView("iso_product_type")
payment_type.createOrReplaceTempView("payment_type")
payment_day.createOrReplaceTempView("payment_day")
customer_detail.createOrReplaceTempView("customer_detail")
bonification_account.createOrReplaceTempView("bonification_account")
subproduct_package.createOrReplaceTempView("subproduct_package")
legal_entity.createOrReplaceTempView("legal_entity")

dados_dlc = sqlc.sql("""
SELECT DISTINCT 
	c.identification_number AS `id_cliente`,
	a.operation_id 			AS `conta`,
	c.customer_id 			AS `codigo_cliente`, 
	P.SHORT_DESC 			AS `produto`,
	SP.SHORT_DESC 			AS `plano`,
	PT.SHORT_DESC 			AS `forma_pagamento`,
	sa.short_desc 			AS `status_conta`,
	a.closed_date 			AS `data_encerramento`,
	PD.PAYMENT_DAY_ID 		AS `dia_vencimento`,
	ISO.SHORT_DESC 			AS `tipo_produto`,
	PK.PROMOTIONAL_CODE 	AS `codigo_promocional`,
	a.opening_date 			AS `data_criacao`
FROM customer C 
	INNER JOIN identification_type 			IT 	ON ( C.identification_type 	 = IT.id )
	INNER JOIN customer_operation				CO 	ON ( C.id 					 = CO.customer )
	INNER JOIN account 						A 	ON ( CO.subproduct  		 = A.subproduct AND
															 CO.branch 				 = A.branch 	AND 
															 CO.currency 			 = A.currency 	AND 
															 CO.operation_id		 = A.operation_id )
	INNER JOIN customer_operation_type 		CT 	ON ( CO.type 				 = CT.id )
	INNER JOIN status 						SA 	ON ( A.status 				 = SA.id )
	LEFT JOIN basic_individual_data 			BID	ON ( C.id 					 = BID.customer )
	LEFT JOIN address 						B 	ON ( CO.customer 			 = B.customer )
	LEFT JOIN electronics_contact 			EC 	ON ( CO.customer 			 = EC.customer )
	LEFT JOIN contact_phone 					CP 	ON ( CP.customer 			 = EC.customer )
	LEFT JOIN locality 						D 	ON ( B.LOCALITY_ZIP_CODE 	 = D.ID )
	LEFT JOIN province_code 					E  	ON ( B.PROVINCE_CODE 		 = E.ID )
	INNER JOIN product 						P 	ON ( A.PRODUCT 				 = P.ID )
	INNER JOIN iso_product_type 				ISO ON ( P.ISO_PRODUCT 			 = ISO.ID )
	INNER JOIN subproduct 					SP 	ON ( A.subproduct 			 = SP.ID )
	LEFT JOIN payment_type 					PT 	ON ( A.CNT_STOP_PAYMENTS_YTD = PT.ID )
	LEFT JOIN payment_day 					PD 	ON ( PD.ID 					 = A.customer_number_for_insurance)
	LEFT JOIN customer_detail 				CD 	ON ( CD.customer 			 = C.ID) 
	LEFT JOIN bonification_account 			BA 	ON ( BA.account 			 = A.ID)
	LEFT JOIN subproduct_package 				PK 	ON ( BA.subproduct_package	 = PK.ID )
	LEFT JOIN legal_entity 					LE 	ON ( C.ID 					 = LE.customer )
WHERE CT.nemotecnico = 'FIRST_HOLDER_PR'
AND B.IS_FOR_CORRESPONDENCE = 1 
AND EC.EMAIL_FOR_COMU = 1
AND CP.TEL_FOR_COMU = 1
""")

#dados_dlc = spark.sql(query)

dados_dlc.withColumn("dia_vencimento", dados_dlc["dia_vencimento"].cast(IntegerType()))

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
yesterday=pd.to_datetime(yesterday)

dados_dlc = dados_dlc.drop("data_criacao")

data = yesterday.strftime('%Y%m%d')
filename = "Base_geral_contas_{0}.csv".format(data)
dados_dlc.toPandas().to_csv(filename, index=False, header=True, encoding='utf-8-sig' )

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

myHostname = "nome do host"
myUsername = "seu User"
cnopts = pysftp.CnOpts(knownhosts="known_hosts")
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

remotePath = "/Import"

conn = pysftp.Connection(host=myHostname, username=myUsername, password=credential_pass,cnopts=cnopts)
with conn.cd(remotePath):
    conn.put(filename)

### PAX ###
subprocess.call(["hdfs", "dfs", "-rm", "-f", filename])