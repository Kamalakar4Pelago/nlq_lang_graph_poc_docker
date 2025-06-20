CREATE TABLE IF NOT EXISTS core.t1_bookings_all
(
	activity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,booking_currency VARCHAR(65535)   ENCODE lzo
	,booking_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,booking_gross_total DOUBLE PRECISION   ENCODE RAW
	,intent_id VARCHAR(65535)   ENCODE lzo
	,booking_id VARCHAR(65535)   ENCODE RAW
	,booking_state VARCHAR(65535)   ENCODE lzo
	,cancellation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cancellation_type VARCHAR(65535)   ENCODE lzo
	,comments VARCHAR(65535)   ENCODE lzo
	,confirmation_type VARCHAR(65535)   ENCODE lzo
	,country_id VARCHAR(65535)   ENCODE lzo
	,currency VARCHAR(65535)   ENCODE lzo
	,customer_id VARCHAR(65535)   ENCODE lzo
	,date_created TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,date_modified TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,destination_id VARCHAR(65535)   ENCODE lzo
	,discount DOUBLE PRECISION   ENCODE RAW
	,ds_user_id VARCHAR(65535)   ENCODE lzo
	,expiry_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,gross_total DOUBLE PRECISION   ENCODE RAW
	,inventory_type VARCHAR(65535)   ENCODE lzo
	,is_guest_booking BOOLEAN   ENCODE RAW
	,is_voucher_generated BOOLEAN   ENCODE RAW
	,reward_created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,loyalty_rewards__date_modified TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reward_credited_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,earned_reward DOUBLE PRECISION   ENCODE RAW
	,loyalty_id VARCHAR(65535)   ENCODE lzo
	,reward_state VARCHAR(65535)   ENCODE lzo
	,reward_type VARCHAR(65535)   ENCODE lzo
	,net_total DOUBLE PRECISION   ENCODE RAW
	,payout_state VARCHAR(65535)   ENCODE lzo
	,review_mail_sent VARCHAR(65535)   ENCODE lzo
	,review_mail_sent_date VARCHAR(65535)   ENCODE lzo
	,provider_account_id VARCHAR(65535)   ENCODE lzo
	,promo_code VARCHAR(65535)   ENCODE lzo
	,redemption_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,redemption_type VARCHAR(65535)   ENCODE lzo
	,refund_amount DOUBLE PRECISION   ENCODE RAW
	,refund_comments VARCHAR(65535)   ENCODE lzo
	,refund_currency VARCHAR(65535)   ENCODE lzo
	,refund_created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refunds__date_modified TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,external_refund_id VARCHAR(65535)   ENCODE lzo
	,refund_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refund_id VARCHAR(65535)   ENCODE lzo
	,refund_reasons VARCHAR(65535)   ENCODE lzo
	,refund_state VARCHAR(65535)   ENCODE lzo
	,reject_reason VARCHAR(65535)   ENCODE lzo
	,sub_total DOUBLE PRECISION   ENCODE RAW
	,user_isd_code VARCHAR(65535)   ENCODE lzo
	,locale VARCHAR(65535)   ENCODE lzo
	,utm_campaign VARCHAR(65535)   ENCODE lzo
	,utm_content VARCHAR(65535)   ENCODE lzo
	,utm_medium VARCHAR(65535)   ENCODE lzo
	,utm_term VARCHAR(65535)   ENCODE lzo
	,utm_source VARCHAR(65535)   ENCODE lzo
	,voucher_id VARCHAR(65535)   ENCODE lzo
	,voucher_type VARCHAR(65535)   ENCODE lzo
	,workflow_type VARCHAR(65535)   ENCODE lzo
	,cancellation VARCHAR(65535)   ENCODE lzo
	,max_redemption_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,partner_booking_id VARCHAR(65535)   ENCODE lzo
	,partner_id VARCHAR(65535)   ENCODE lzo
	,partner_meta_data VARCHAR(65535)   ENCODE lzo
	,payment_type VARCHAR(65535)   ENCODE lzo
	,order_id VARCHAR(65535)   ENCODE lzo
	,item_quantity BIGINT   ENCODE az64
	,option_id VARCHAR(255)   ENCODE lzo
	,option_name VARCHAR(1000)   ENCODE lzo
	,product_id VARCHAR(255)   ENCODE lzo
	,product_name VARCHAR(1000)   ENCODE lzo
	,cancellation_window BIGINT   ENCODE az64
	,ds_session_id VARCHAR(65535)   ENCODE lzo
	,ref VARCHAR(65535)   ENCODE lzo
	,booking_date_utc8 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,booking_lead_day BIGINT   ENCODE az64
	,is_confirmed_booking INTEGER   ENCODE az64
	,confirmed_booking_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exrate_currency_sgd DOUBLE PRECISION   ENCODE RAW
	,exrate_booking_currency_sgd DOUBLE PRECISION   ENCODE RAW
	,refund_currency_rate_sgd DOUBLE PRECISION   ENCODE RAW
	,gross_total_sgd DOUBLE PRECISION   ENCODE RAW
	,net_total_sgd DOUBLE PRECISION   ENCODE RAW
	,booking_gross_total_sgd DOUBLE PRECISION   ENCODE RAW
	,sub_total_sgd DOUBLE PRECISION   ENCODE RAW
	,discount_sgd DOUBLE PRECISION   ENCODE RAW
	,commission_sgd DOUBLE PRECISION   ENCODE RAW
	,refund_amount_sgd DOUBLE PRECISION   ENCODE RAW
	,promo_type VARCHAR(500)   ENCODE lzo
	,promo_channel VARCHAR(256)   ENCODE lzo
	,iso_of_booking_isd CHAR(2)   ENCODE lzo
	,sys_process_date DATE   ENCODE RAW
	,sys_process_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE KEY
 DISTKEY (booking_id)
 SORTKEY (
	booking_id
	, date_created
	, sys_process_date
	)
;