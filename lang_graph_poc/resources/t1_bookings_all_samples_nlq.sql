-- User: Show me top 5 products by revenue last month
-- Intent: Product Performance
-- SQL:
SELECT product_name, SUM(gross_total_sgd) AS revenue
FROM core.t1_bookings_all
WHERE booking_date_utc8 >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND booking_date_utc8 < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 5;

-- User: What’s the average booking value by country this month?
-- Intent: Revenue and Booking Analysis
-- SQL:
SELECT
    iso_of_booking_isd AS country,
    AVG(gross_total_sgd) AS avg_booking_value_sgd,
    COUNT(*) AS booking_count
FROM core.t1_bookings_all
WHERE DATE_TRUNC('month', booking_date_utc8) = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY iso_of_booking_isd
ORDER BY avg_booking_value_sgd DESC;

-- User: Show me total bookings and revenue for the last 30 days
-- Intent: Revenue and Booking Analysis
-- SQL:
SELECT
    COUNT(*) AS total_bookings,
    SUM(gross_total_sgd) AS total_revenue_sgd
FROM core.t1_bookings_all
WHERE booking_date_utc8 >= CURRENT_DATE - INTERVAL '30 days';

-- User: Show me confirmed bookings by customer segment for Q1 2025
-- Intent: Customer Behavior Analysis
-- SQL:
SELECT
    CASE
        WHEN is_guest_booking = true THEN 'Guest'
        ELSE 'Registered'
    END AS customer_segment,
    COUNT(*) AS confirmed_bookings,
    SUM(gross_total_sgd) AS total_revenue_sgd
FROM core.t1_bookings_all
WHERE is_confirmed_booking = 1
    AND booking_date_utc8 BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY customer_segment;

-- User: Find customers with multiple bookings and their total spend
-- Intent: Customer Behavior Analysis
-- SQL:
SELECT
    customer_id,
    COUNT(*) AS booking_count,
    SUM(gross_total_sgd) AS total_spend_sgd
FROM core.t1_bookings_all
WHERE is_confirmed_booking = 1
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY total_spend_sgd DESC;

-- User: Which products have the highest cancellation rates?
-- Intent: Product Performance
-- SQL:
SELECT
    product_name,
    COUNT(*) AS total_bookings,
    SUM(CASE WHEN booking_state = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_bookings,
    ROUND(100.0 * SUM(CASE WHEN booking_state = 'CANCELLED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate_pct
FROM core.t1_bookings_all
GROUP BY product_name
HAVING COUNT(*) >= 10
ORDER BY cancellation_rate_pct DESC;

-- User: Show me top selling products by revenue last quarter
-- Intent: Product Performance
-- SQL:
SELECT
    product_name,
    COUNT(*) AS bookings,
    SUM(gross_total_sgd) AS revenue_sgd
FROM core.t1_bookings_all
WHERE booking_date_utc8 >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')
    AND booking_date_utc8 < DATE_TRUNC('quarter', CURRENT_DATE)
    AND is_confirmed_booking = 1
GROUP BY product_name
ORDER BY revenue_sgd DESC
LIMIT 10;

-- User: What’s the conversion rate by UTM source this month?
-- Intent: Marketing Attribution
-- SQL:
SELECT
    utm_source,
    COUNT(*) AS total_bookings,
    SUM(is_confirmed_booking) AS confirmed_bookings,
    ROUND(100.0 * SUM(is_confirmed_booking) / COUNT(*), 2) AS conversion_rate_pct,
    SUM(gross_total_sgd) AS revenue_sgd
FROM core.t1_bookings_all
WHERE DATE_TRUNC('month', booking_date_utc8) = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY utm_source
ORDER BY conversion_rate_pct DESC;

-- User: Show me promo code performance with discount amounts
-- Intent: Marketing Attribution
-- SQL:
SELECT
    COALESCE(promo_code,'NON_PROMO') AS promo_code,
    promo_type,
    COUNT(*) AS usage_count,
    SUM(discount_sgd) AS total_discount_sgd,
    AVG(discount_sgd) AS avg_discount_sgd,
    SUM(gross_total_sgd) AS revenue_generated_sgd
FROM core.t1_bookings_all
WHERE promo_code IS NOT NULL
    AND is_confirmed_booking = 1
GROUP BY promo_code, promo_type
ORDER BY usage_count DESC;

-- User: Show me refund trends by month for the past year
-- Intent: Refund Analysis
-- SQL:
SELECT
    DATE_TRUNC('month', refund_date) AS refund_month,
    COUNT(*) AS refund_count,
    SUM(refund_amount_sgd) AS total_refund_sgd,
    AVG(refund_amount_sgd) AS avg_refund_sgd
FROM core.t1_bookings_all
WHERE refund_date IS NOT NULL
    AND refund_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', refund_date)
ORDER BY refund_month;

-- User: What’s the average booking lead time by destination?
-- Intent: Booking Lead Time Analysis
-- SQL:
SELECT
    destination_id,
    AVG(booking_lead_day) AS avg_lead_days,
    COUNT(*) AS booking_count
FROM core.t1_bookings_all
WHERE is_confirmed_booking = 1
    AND booking_lead_day >= 0
GROUP BY destination_id
HAVING COUNT(*) >= 20
ORDER BY avg_lead_days DESC;

-- User: Show me commission earned by product name this year
-- Intent: Commission Analysis
-- SQL:
SELECT
    product_name,
    COUNT(*) AS bookings,
    SUM(commission_sgd) AS total_commission_sgd,
    AVG(commission_sgd) AS avg_commission_sgd
FROM core.t1_bookings_all
WHERE DATE_TRUNC('year', booking_date_utc8) = DATE_TRUNC('year', CURRENT_DATE)
    AND is_confirmed_booking = 1
GROUP BY product_name
ORDER BY total_commission_sgd DESC;

-- User: How many loyalty rewards were earned and their total value?
-- Intent: Loyalty Program Analysis
-- SQL:
SELECT
    reward_type,
    COUNT(*) AS rewards_earned,
    SUM(earned_reward) AS total_reward_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM core.t1_bookings_all
WHERE earned_reward IS NOT NULL
    AND reward_state = 'CREDITED'
GROUP BY reward_type
ORDER BY total_reward_value DESC;

-- User: What was the total number of bookings yesterday?
-- Intent: Revenue and Booking Analysis
-- SQL:
SELECT
  COUNT(booking_id)
FROM t1_bookings_all_data_dev
WHERE
  booking_date_utc8 :: DATE = '2025-06-17'
  AND booking_state = 'CONFIRMED';

-- User: What was the total Gross Merchandise Value (GMV) in SGD yesterday?
-- Intent: Revenue and Booking Analysis
-- SQL:
SELECT
  SUM(booking_gross_total_sgd)
FROM t1_bookings_all_data_dev
WHERE
  booking_date_utc8 :: DATE = '2025-06-17'
  AND booking_state = 'CONFIRMED';

-- User: How many confirmed bookings are there in total?
-- Intent: Revenue and Booking Analysis
-- SQL:
SELECT
  COUNT(booking_id)
FROM t1_bookings_all_data_dev
WHERE
  booking_state = 'CONFIRMED';

-- User: What is the total Gross Merchandise Value (GMV) in SGD for all confirmed bookings?
-- Intent: Revenue and Booking Analysis
-- SQL:
SELECT
  SUM(booking_gross_total_sgd)
FROM t1_bookings_all_data_dev
WHERE
  booking_state = 'CONFIRMED';

-- User: List the top 5 products by GMV yesterday.
-- Intent: Product Performance
-- SQL:
SELECT
  product_name,
  SUM(booking_gross_total_sgd) AS total_gmv
FROM t1_bookings_all_data_dev
WHERE
  booking_date_utc8 :: DATE = '2025-06-17'
  AND booking_state = 'CONFIRMED'
GROUP BY
  product_name
ORDER BY
  total_gmv DESC
LIMIT 5;

-- User: What are the different booking states and their counts?
-- Intent: Booking Analysis
-- SQL:
SELECT
  booking_state,
  COUNT(booking_id) AS count
FROM t1_bookings_all_data_dev
GROUP BY
  booking_state
ORDER BY
  count DESC;

-- User: How many bookings were cancelled by the customer?
-- Intent: Booking Analysis
-- SQL:
SELECT
  COUNT(booking_id)
FROM t1_bookings_all_data_dev
WHERE
  booking_state = 'CANCELLED_BY_CUSTOMER';

-- User: What is the total refund amount in SGD?
-- Intent: Refund Analysis
-- SQL:
SELECT
  SUM(refund_amount_sgd)
FROM t1_bookings_all_data_dev
WHERE
  refund_state = 'REFUND_ISSUED';

-- User: Show me the most redeemed promo codes.
-- Intent: Marketing Attribution
-- SQL:
SELECT
  promo_code,
  COUNT(booking_id) AS redemption_count
FROM t1_bookings_all_data_dev
WHERE
  promo_code IS NOT NULL
GROUP BY
  promo_code
ORDER BY
  redemption_count DESC
LIMIT 10;

-- User: What is the total net total in SGD for all bookings?
-- Intent: Revenue Analysis
-- SQL:
SELECT
  SUM(net_total_sgd)
FROM t1_bookings_all_data_dev;