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

accountSchema = StructType() \
      .add("id",StringType(),True) \
      .add('product', StringType(), True) \
      .add('subproduct', StringType(), True) \
      .add('branch', StringType(), True) \
      .add('operation_id', StringType(), True) \
      .add('currency', StringType(), True) \
      .add('iso_product', StringType(), True) \
      .add('name', StringType(), True) \
      .add('short_name', StringType(), True) \
      .add('input_date', StringType(), True) \
      .add('opening_date', StringType(), True) \
      .add('closed_date', StringType(), True) \
      .add('closed_code', StringType(), True) \
      .add('use_of_signature', StringType(), True) \
      .add('situation_code', StringType(), True) \
      .add('interbranch_allowed', StringType(), True) \
      .add('vat_category', StringType(), True) \
      .add('central_bank_activity_code', StringType(), True) \
      .add('business_sector', StringType(), True) \
      .add('bank_activity_code', StringType(), True) \
      .add('available_bal_process_type', StringType(), True) \
      .add('available_bal_process_time', StringType(), True) \
      .add('db_bal_allowed_flag', StringType(), True) \
      .add('verif_hist_bal_flag', StringType(), True) \
      .add('checks_allowed_flag', StringType(), True) \
      .add('min_days_for_checks', StringType(), True) \
      .add('link_to_package', StringType(), True) \
      .add('pack_subproduct', StringType(), True) \
      .add('pack_operation_id', StringType(), True) \
      .add('pack_main_account', StringType(), True) \
      .add('price_list_profile', StringType(), True) \
      .add('special_fees', StringType(), True) \
      .add('special_rates', StringType(), True) \
      .add('db_restriction', StringType(), True) \
      .add('deposit_restriction', StringType(), True) \
      .add('withdrawal_restriction', StringType(), True) \
      .add('cr_restriction', StringType(), True) \
      .add('customer_number_declared', StringType(), True) \
      .add('customer_number_reached', StringType(), True) \
      .add('customer_number_for_insurance', StringType(), True) \
      .add('insurance_code', StringType(), True) \
      .add('update_date' , StringType(), True) \
      .add('last_process_date' , StringType(), True) \
      .add('last_txn_date' , StringType(), True) \
      .add('last_txn_2date' , StringType(), True) \
      .add('last_txn_int_cr_date' , StringType(), True) \
      .add('last_txn_ext_cr_date' , StringType(), True) \
      .add('last_txn_int_db_date' , StringType(), True) \
      .add('last_txn_ext_db_date' , StringType(), True) \
      .add('last_withdrawal_date' , StringType(), True) \
      .add('check_temporary_receivership' , StringType(), True) \
      .add('last_stat_process' , StringType(), True) \
      .add('last_history_process' , StringType(), True) \
      .add('db_days_without_agreement', StringType(), True) \
      .add('db_balance_start_date', StringType(), True) \
      .add('db_balance_total_days', StringType(), True) \
      .add('cnt_chk_formal_reject_mtd', StringType(), True) \
      .add('cnt_chk_formal_reject_ytd', StringType(), True) \
      .add('cnt_chk_reject_mtd', StringType(), True) \
      .add('cnt_chk_reject_ytd', StringType(), True) \
      .add('cnt_chk_justified_ytd', StringType(), True) \
      .add('cnt_onp_ytd', StringType(), True) \
      .add('cnt_stop_payments_ytd', StringType(), True) \
      .add('cnt_withdrawal', StringType(), True) \
      .add('debit_mode_type', StringType(), True) \
      .add('accrual_suspended_date', StringType(), True) \
      .add('total_db_accrued_interest', StringType(), True) \
      .add('oldest_valued_date_txn', StringType(), True) \
      .add('balance_today', StringType(), True) \
      .add('balance_yesterday', StringType(), True) \
      .add('db_balance_to_accrual', StringType(), True) \
      .add('today_cash_deposits', StringType(), True) \
      .add('today_cash_withdrawal', StringType(), True) \
      .add('today_other_cr', StringType(), True) \
      .add('today_other_db', StringType(), True) \
      .add('today24_hrs_db', StringType(), True) \
      .add('tomorrow_cr', StringType(), True) \
      .add('tomorrow_db', StringType(), True) \
      .add('yesterday_cr', StringType(), True) \
      .add('yesterday_db', StringType(), True) \
      .add('total_agreements', StringType(), True) \
      .add('tomorrow_agreements', StringType(), True) \
      .add('blocked_balance', StringType(), True) \
      .add('tomorrow_blocked', StringType(), True) \
      .add('accum_pending_txn', StringType(), True) \
      .add('today_deposits24_hrs', StringType(), True) \
      .add('today_deposits48_hrs', StringType(), True) \
      .add('today_deposits72_hrs', StringType(), True) \
      .add('today_deposits96_hrs', StringType(), True) \
      .add('today_deposits_other_terms', StringType(), True) \
      .add('deposits24_hrs', StringType(), True) \
      .add('deposits48_hrs', StringType(), True) \
      .add('deposits72_hrs', StringType(), True) \
      .add('deposits96_hrs', StringType(), True) \
      .add('deposits_other_terms', StringType(), True) \
      .add('other_cr24_hrs', StringType(), True) \
      .add('other_cr48_hrs', StringType(), True) \
      .add('other_cr72_hrs', StringType(), True) \
      .add('other_cr96_hrs', StringType(), True) \
      .add('other_cr_other_terms', StringType(), True) \
      .add('checks24_hrs', StringType(), True) \
      .add('checks72_hrs', StringType(), True) \
      .add('checks48_hrs', StringType(), True) \
      .add('checks96_hrs', StringType(), True) \
      .add('checks_others_terms', StringType(), True) \
      .add('other_db24_hrs', StringType(), True) \
      .add('other_db48_hrs', StringType(), True) \
      .add('other_db72_hrs', StringType(), True) \
      .add('other_db96_hrs', StringType(), True) \
      .add('other_db_other_terms', StringType(), True) \
      .add('status', StringType(), True) \
      .add('status_date', StringType(), True) \
      .add('status_reason', StringType(), True) \
      .add('system_user_core', StringType(), True) \
      .add('stamp_additional', StringType(), True) \
      .add('stamp_date_time', StringType(), True) \
      .add('stamp_user', StringType(), True) \
      .add('bank', StringType(), True) \
      .add('buk_number', StringType(), True) \
      .add('check_digit_out', StringType(), True) \
      .add('nick_name', StringType(), True) \
      .add('tipology_code', StringType(), True) \
      .add('opening_code_motive', StringType(), True) \
      .add('opening_description_motive' , StringType(), True) \
      .add('closed_description_motive' , StringType(), True) \
      .add('netting_group', StringType(), True) \
      .add('compensation_group', StringType(), True) \
      .add('positioning_group', StringType(), True) \
      .add('last_page_emitted', StringType(), True) \
      .add('last_date_emitted' , StringType(), True) \
      .add('last_page_generated', StringType(), True) \
      .add('line_stt_number', StringType(), True) \
      .add('last_stt_balance', StringType(), True) \
      .add('forced_stt_flag', StringType(), True) \
      .add('cnt_normal_stt', StringType(), True) \
      .add('cnt_special_stt', StringType(), True) \
      .add('stt_type', StringType(), True) \
      .add('stt_period', StringType(), True) \
      .add('sub_currency_is_obligatory', StringType(), True) \
      .add('donot_dormant_flag', StringType(), True) \
      .add('inhibit_checkbook_flag', StringType(), True) \
      .add('activate_form', StringType(), True) \
      .add('notice_of_closure_flag', StringType(), True) \
      .add('date_of_closure' , StringType(), True) \
      .add('data_complete_customer' , StringType(), True) \
      .add('authorization_reason', StringType(), True) \
      .add('reserve_balance', StringType(), True) \
      .add('dep_available_today', StringType(), True) \
      .add('doc_amount', StringType(), True) \
      .add('last_accrual_date' , StringType(), True) \
      .add('shadow_flag', StringType(), True)

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
      .add('id',StringType(),True) \
      .add('institution',StringType(),True) \
      .add('customer_id',StringType(),True) \
      .add('name',StringType(),True) \
      .add('first_name1',StringType(),True) \
      .add('last_name1',StringType(),True) \
      .add('first_name2',StringType(),True) \
      .add('last_name2',StringType(),True) \
      .add('name_short',StringType(),True) \
      .add('person_type',StringType(),True) \
      .add('customer_type',StringType(),True) \
      .add('crm_info',StringType(),True) \
      .add('expiry_date_crm_info',StringType(),True) \
      .add('originating_branch',StringType(),True) \
      .add('segment',StringType(),True) \
      .add('primary_product',StringType(),True) \
      .add('attention_segment',StringType(),True) \
      .add('responsability_officer',StringType(),True) \
      .add('classification_code',StringType(),True) \
      .add('credit_status',StringType(),True) \
      .add('condition',StringType(),True) \
      .add('origin_customer',StringType(),True) \
      .add('confidential_flag',StringType(),True) \
      .add('bank_link_type',StringType(),True) \
      .add('central_bank_activity_code',StringType(),True) \
      .add('central_bk_activity_second',StringType(),True) \
      .add('bank_activity_code',StringType(),True) \
      .add('bank_activity_second',StringType(),True) \
      .add('business_sector',StringType(),True) \
      .add('residence_code',StringType(),True) \
      .add('addition_date',StringType(),True) \
      .add('purge_reason',StringType(),True) \
      .add('purge_date',StringType(),True) \
      .add('contact_bank',StringType(),True) \
      .add('seniority_bank',StringType(),True) \
      .add('status',StringType(),True) \
      .add('status_date',StringType(),True) \
      .add('status_reason',StringType(),True) \
      .add('stamp_additional',StringType(),True) \
      .add('stamp_date_time',StringType(),True) \
      .add('stamp_user',StringType(),True) \
      .add('system_user_core',StringType(),True) \
      .add('identification_type',StringType(),True) \
      .add('identification_number',StringType(),True) \
      .add('department',StringType(),True) \
      .add('epay_person_type',StringType(),True) \
      .add('credit_status_date',StringType(),True) \
      .add('dependents_f',StringType(),True) \
      .add('dependents_m',StringType(),True) \
      .add('is_employee',StringType(),True) \
      .add('primary_subproduct',StringType(),True) \
      .add('pep',StringType(),True) \
      .add('description',StringType(),True) \
      .add('subsidiary',StringType(),True) \
      .add('channel',StringType(),True) \
      .add('is_duplicated',StringType(),True)
      
      
armestrepedidoentregadetalheSchema = StructType() \
      .add('dataatualizacao',StringType(),True) \
      .add('codrastreio',StringType(),True) \
      .add('conta',StringType(),True) \
      .add('identificador',StringType(),True) \
      .add('codigocliente',StringType(),True) \
      .add('tipoproduto',StringType(),True) \
      .add('eventname',StringType(),True)
      
identificadorSchema = StructType() \
      .add('codidentificador',StringType(),True) \
      .add('bloqueio',StringType(),True) \
      .add('situacao',StringType(),True) \
      .add('datainclusao',StringType(),True) \
      .add('clienteconta_conta_utilizalimite',StringType(),True) \
      .add('clienteconta_conta_saldo',StringType(),True) \
      .add('clienteconta_conta_limiteoperacional',StringType(),True) \
      .add('clienteconta_conta_tipoproduto',StringType(),True) \
      .add('clienteconta_conta_id',StringType(),True) \
      .add('clienteconta_cliente_id',StringType(),True) \
      .add('placa',StringType(),True) \
      .add('motivobloqueio',StringType(),True) \
      .add('motivoinativacao',StringType(),True) \
      .add('idconta',StringType(),True) \
      .add('tempassagem',StringType(),True) \
      .add('nuidentificador',StringType(),True) \
      .add('idcliente',StringType(),True) \
      .add('veiculo_categoria',StringType(),True) \
      .add('veiculo_placa',StringType(),True) \
      .add('veiculo_grupo',StringType(),True) \
      .add('veiculo_categoriavalidada',StringType(),True) \
      .add('id',StringType(),True) \
      .add('dataatualizacao',StringType(),True) \
      .add('eventname',StringType(),True)
      
armestrepedidoentregaSchema = StructType() \
      .add('qtdeidentificadores',StringType(),True) \
      .add('valor',StringType(),True) \
      .add('statuspedido',StringType(),True) \
      .add('dataentrega',StringType(),True) \
      .add('codrastreio',StringType(),True) \
      .add('cnpjcpf',StringType(),True) \
      .add('numeroregistro',StringType(),True) \
      .add('tipoproduto',StringType(),True) \
      .add('codigopedido',StringType(),True) \
      .add('peso',StringType(),True) \
      .add('conta',StringType(),True) \
      .add('logradouro_estado',StringType(),True) \
      .add('logradouro_cidade',StringType(),True) \
      .add('logradouro_complemento',StringType(),True) \
      .add('logradouro_logradouro',StringType(),True) \
      .add('logradouro_bairro',StringType(),True) \
      .add('logradouro_numlogradouro',StringType(),True) \
      .add('logradouro_cep',StringType(),True) \
      .add('dataprevistaentrega',StringType(),True) \
      .add('codigocliente',StringType(),True) \
      .add('idtransportadora',StringType(),True) \
      .add('nome',StringType(),True) \
      .add('tiporegistro',StringType(),True) \
      .add('tipokit',StringType(),True) \
      .add('eventname',StringType(),True) 
      
propostapfSchema = StructType() \
      .add('id',StringType(),True) \
      .add('codigo',StringType(),True) \
      .add('canalorigem',StringType(),True) \
      .add('datacriacao',StringType(),True) \
      .add('dataetapafluxoproposta',StringType(),True) \
      .add('situacaoproposta',StringType(),True) \
      .add('contato_nome',StringType(),True) \
      .add('dadospessoais_cpf',StringType(),True) \
      .add('dadospessoais_datanascimento',StringType(),True) \
      .add('dadospessoais_estadocivilid',StringType(),True) \
      .add('dadospessoais_nomemae',StringType(),True) \
      .add('dadospessoais_sexoid',StringType(),True) \
      .add('contato_email',StringType(),True) \
      .add('contato_recebimentoinformacoesemail',StringType(),True) \
      .add('contato_telefones_telefonecelular',StringType(),True) \
      .add('contato_telefones_telefonefixo',StringType(),True) \
      .add('endereco_bairro',StringType(),True) \
      .add('endereco_cep',StringType(),True) \
      .add('endereco_cidade',StringType(),True) \
      .add('endereco_cidadeid',StringType(),True) \
      .add('endereco_complemento',StringType(),True) \
      .add('endereco_logradouro',StringType(),True) \
      .add('endereco_numero',StringType(),True) \
      .add('endereco_uf',StringType(),True) \
      .add('plano_produtoid',StringType(),True) \
      .add('plano_subprodutoid',StringType(),True) \
      .add('plano_valor',StringType(),True) \
      .add('plano_valorrecargaprepago',StringType(),True) \
      .add('plano_quantidadeidentificadores',StringType(),True) \
      .add('plano_codigopromocional',StringType(),True) \
      .add('entrega_endereco_bairro',StringType(),True) \
      .add('entrega_endereco_cep',StringType(),True) \
      .add('entrega_endereco_cidade',StringType(),True) \
      .add('entrega_endereco_cidadeid',StringType(),True) \
      .add('entrega_endereco_complemento',StringType(),True) \
      .add('entrega_endereco_enderecodentroareacobertura',StringType(),True) \
      .add('entrega_endereco_logradouro',StringType(),True) \
      .add('entrega_endereco_numero',StringType(),True) \
      .add('entrega_endereco_uf',StringType(),True) \
      .add('entrega_tipoentrega',StringType(),True) \
      .add('analisecreditoretorno_codevento',StringType(),True) \
      .add('analisecreditoretorno_codmensagem',StringType(),True) \
      .add('analisecreditoretorno_codoperacao',StringType(),True) \
      .add('analisecreditoretorno_descricaomensagem',StringType(),True) \
      .add('analisecreditoretorno_numeroproposta',StringType(),True) \
      .add('analisecreditoretorno_resultado',StringType(),True) \
      .add('analisecreditoretorno_sucesso',StringType(),True) \
      .add('dadospagamento_bandeira',StringType(),True) \
      .add('dadospagamento_nome',StringType(),True) \
      .add('dadospagamento_numeromascarado',StringType(),True) \
      .add('dadospagamento_validade',StringType(),True) \
      .add('versaotermoaceite',StringType(),True) \
      .add('clienteconta_numerocliente',StringType(),True) \
      .add('clienteconta_numeroconta',StringType(),True) \
      .add('numeropropostaanalisecredito',StringType(),True) \
      .add('enderecodentroareacobertura',StringType(),True) \
      .add('tipoparceirocomercial',StringType(),True) \
      .add('parceirocomercialid',StringType(),True) \
      .add('subprodutodesc',StringType(),True) \
      .add('tipoplano',StringType(),True) \
      .add('calcobservacao',StringType(),True) \
      .add('datahorainicio',StringType(),True) \
      .add('datahorafim',StringType(),True) \
      .add('formapagamento',StringType(),True) \
      .add('version',StringType(),True) \
      .add('isnovofluxocompras',StringType(),True) \
      .add('fluxocanal',StringType(),True) \
      .add('eventname',StringType(),True)
      
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
      
      
sub_packageSchema = StructType() \
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

account = sqlc.read.options(delimiter='\020').schema(accountSchema).csv('caminho da tabela')
customer_operation = sqlc.read.options(delimiter='\020').schema(customerOperationsSchema).csv('caminho da tabela')
customer = sqlc.read.options(delimiter='\020').schema(customerSchema).csv('caminho da tabela')
identificador = sqlc.read.json('caminho da tabela')
clienteconta = sqlc.read.json('caminho da tabela')
bonification_account = sqlc.read.options(delimiter='\020').schema(bonification_accountSchema).csv('caminho da tabela')
sub_package = sqlc.read.options(delimiter='\020').schema(sub_packageSchema).csv('caminho da tabela')
armestrepedidoentrega = sqlc.read.json('caminho da tabela')
armestrepedidoentregadetalhe = sqlc.read.json('caminho da tabela')
pedidoentregaidentificador = sqlc.read.json('caminho da tabela')
propostapf = sqlc.read.json('caminho da tabela')

account.createOrReplaceTempView("account")
customer_operation.createOrReplaceTempView("customer_operation")
identificador.createOrReplaceTempView("identificador")
bonification_account.createOrReplaceTempView("bonification_account")
sub_package.createOrReplaceTempView("sub_package")
armestrepedidoentrega.createOrReplaceTempView("armestrepedidoentrega")
armestrepedidoentregadetalhe.createOrReplaceTempView("armestrepedidoentregadetalhe")
customer.createOrReplaceTempView("customer")
pedidoentregaidentificador.createOrReplaceTempView("pedidoentregaidentificador")
propostapf.createOrReplaceTempView("propostapf")

df = spark.sql("""
select p.codrastreio as codigo_rastreio,
c.name as nome,
c.identification_number as cpf,
p.codigopedido as numero_pedido,
cast(concat(SUBSTR(pi.datapedido, 1,10), ' ', SUBSTR(pi.datapedido, 12,8))as timestamp) as data_pedido,
case when
	p.statuspedido in ('CANCELADO', 'SEPARACAO', 'TRANSPORTE', 'ENVIADO') then 'EM ANDAMENTO'
	else p.statuspedido
	end as status_entrega_identificadores,
pd.identificador as serial_number,
case when
    i.situacao = 'ATIVO' OR i.situacao = 'INATIVO' then i.situacao
    else 'TAG NAO ATIVADA'
    end as status_identificador,
case when
	pk.promotional_code = 'BTGPOSDZ' then 'COBRANDED'
	when pk.promotional_code = 'VELOEBTG24POS' or pk.promotional_code = 'VELOEBTG24PRE' then 'LP'
	else 'N/A'
	end as origem,
--pk.promotional_code as codigo_promo
case
    when statuspedido = 'ENTREGUE' then cast(concat(SUBSTR(p.dataentrega, 1,10), ' ', SUBSTR(p.dataentrega, 12,8))as timestamp)
    else Null
end as Data_TAG_Entregue
from pedidoentregaidentificador pi
left join armestrepedidoentrega p on p.codigopedido = pi.id 
left join armestrepedidoentregadetalhe pd on pd.codrastreio = p.codrastreio 
left join identificador i on pd.identificador = i.codidentificador
left join bonification_account ba on ba.account = p.conta
left join sub_package pk on ba.subproduct_package = pk.id
left join customer c on c.customer_id = pd.codigocliente
where pk.promotional_code in ('BTGPOSDZ', 'VELOEBTG24POS', 'VELOEBTG24PRE')
and (c.person_type = 1 or c.person_type = '1')
and p.statuspedido not in ('EXTRAVIADO', 'ERRO_PEDIDO')
""")

df2 = spark.sql("""
select p.codrastreio as codigo_rastreio,
c.name as nome,
c.identification_number as cpf,
p.codigopedido as numero_pedido,
cast(concat(SUBSTR(pi.datapedido, 1,10), ' ', SUBSTR(pi.datapedido, 12,8))as timestamp) as data_pedido,
case when
	p.statuspedido in ('CANCELADO', 'SEPARACAO', 'TRANSPORTE', 'ENVIADO') then 'EM ANDAMENTO'
	else p.statuspedido
	end as status_entrega_identificadores,
pd.identificador as serial_number,
case when
    i.situacao = 'ATIVO' OR i.situacao = 'INATIVO' then i.situacao
    else 'TAG NAO ATIVADA'
    end as status_identificador,
case when
	pf.tipoparceirocomercial = 'COBRANDED' and pf.parceirocomercialid = '1873610' then 'COBRANDED'
	when pf.codigo = 'VELOEBTG24POS' or pf.codigo = 'VELOEBTG24PRE' then 'LP'
	else 'N/A'
	end as origem ,
case
    when statuspedido = 'ENTREGUE' then cast(concat(SUBSTR(p.dataentrega, 1,10), ' ', SUBSTR(p.dataentrega, 12,8))as timestamp)
    else Null
end as Data_TAG_Entregue
from pedidoentregaidentificador pi
left join armestrepedidoentrega p on p.codigopedido = pi.id 
left join armestrepedidoentregadetalhe pd on pd.codrastreio = p.codrastreio 
left join identificador i on pd.identificador = i.codidentificador
left join customer c on c.customer_id = pd.codigocliente
inner join propostapf pf on pf.clienteconta_numeroconta = pi.conta
where pf.codigo in ('VELOEBTG24POS', 'VELOEBTG24PRE') or pf.parceirocomercialid = '1873610'
and p.statuspedido not in ('EXTRAVIADO', 'ERRO_PEDIDO')
""")

df_concat = df.union(df2)

df_concat = df_concat.dropDuplicates()

import datetime
today = (datetime.date.today())

filename = 'PedidosBTG{}.csv'.format(today)
df_concat.toPandas().to_csv(filename, sep=';', index = False,encoding='utf-8-sig')

import pysftp

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

#Setando credenciais SFTP
myHostname = "nome do host"
myUsername = "seu User"
cnopts = pysftp.CnOpts(knownhosts="known_hosts")
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

remotePath = '/comercial/out'

conn = pysftp.Connection(host=myHostname, username=myUsername, password=credential_pass,cnopts=cnopts)
with conn.cd(remotePath):
    conn.put(filename)

### PAX ###
subprocess.call(["hdfs", "dfs", "-rm", "-f", filename])