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

--> user intent: Revenue and Booking Analysis
--> use query; What’s the average booking value by country this month?”
---->> sql query:
        SELECT 
            iso_of_booking_isd as country,
            AVG(gross_total_sgd) as avg_booking_value_sgd,
            COUNT(*) as booking_count
        FROM core.t1_bookings_all 
        WHERE DATE_TRUNC('month', booking_date_utc8) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY iso_of_booking_isd
        ORDER BY avg_booking_value_sgd DESC;

-->user intent: Revenue and Booking Analysis
--> use query; Show me total bookings and revenue for the last 30 days
---->> sql query:
SELECT 
    COUNT(*) as total_bookings,
    SUM(gross_total_sgd) as total_revenue_sgd
FROM core.t1_bookings_all 
WHERE booking_date_utc8 >= CURRENT_DATE - INTERVAL '30 days';

--Customer Behavior Analysis
--> use query; Show me confirmed bookings by customer segment for Q1 2025
---->> sql query:
        SELECT 
            CASE 
                WHEN is_guest_booking = true THEN 'Guest'
                ELSE 'Registered'
            END as customer_segment,
            COUNT(*) as confirmed_bookings,
            SUM(gross_total_sgd) as total_revenue_sgd
        FROM core.t1_bookings_all 
        WHERE is_confirmed_booking = 1 
            AND booking_date_utc8 BETWEEN '2025-01-01' AND '2025-03-31'
        GROUP BY customer_segment;

--. Customer Behavior Analysis
--> use query; Find customers with multiple bookings and their total spend”
---->> sql query:
        SELECT 
            customer_id,
            COUNT(*) as booking_count,
            SUM(gross_total_sgd) as total_spend_sgd
        FROM core.t1_bookings_all 
        WHERE is_confirmed_booking = 1
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        ORDER BY total_spend_sgd DESC;
--. Product Performance
--> use query; Which products have the highest cancellation rates?
---->> sql query:
        SELECT 
            product_name,
            COUNT(*) as total_bookings,
            SUM(CASE WHEN booking_state = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_bookings,
            ROUND(100.0 * SUM(CASE WHEN booking_state = 'CANCELLED' THEN 1 ELSE 0 END) / COUNT(*), 2) as cancellation_rate_pct
        FROM core.t1_bookings_all 
        GROUP BY product_name
        HAVING COUNT(*) >= 10
        ORDER BY cancellation_rate_pct DESC;

--. Product Performance
--> use query; Show me top selling products by revenue last quarter
---->> sql query: 
        SELECT 
            product_name,
            COUNT(*) as bookings,
            SUM(gross_total_sgd) as revenue_sgd
        FROM core.t1_bookings_all 
        WHERE booking_date_utc8 >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')
            AND booking_date_utc8 < DATE_TRUNC('quarter', CURRENT_DATE)
            AND is_confirmed_booking = 1
        GROUP BY product_name
        ORDER BY revenue_sgd DESC
        LIMIT 10;

-->Marketing Attribution
--> use query; What’s the conversion rate by UTM source this month?
---->> sql query: 
        SELECT 
            utm_source,
            COUNT(*) as total_bookings,
            SUM(is_confirmed_booking) as confirmed_bookings,
            ROUND(100.0 * SUM(is_confirmed_booking) / COUNT(*), 2) as conversion_rate_pct,
            SUM(gross_total_sgd) as revenue_sgd
        FROM core.t1_bookings_all 
        WHERE DATE_TRUNC('month', booking_date_utc8) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY utm_source
        ORDER BY conversion_rate_pct DESC;

--Marketing Attribution
--> use query; Show me promo code performance with discount amounts
---->> sql query:  
        SELECT 
            COALESCE(promo_code,'NON_PROMO') as promo_code,
            promo_type,
            COUNT(*) as usage_count,
            SUM(discount_sgd) as total_discount_sgd,
            AVG(discount_sgd) as avg_discount_sgd,
            SUM(gross_total_sgd) as revenue_generated_sgd
        FROM core.t1_bookings_all 
        WHERE promo_code IS NOT NULL 
            AND is_confirmed_booking = 1
        GROUP BY promo_code, promo_type
        ORDER BY usage_count DESC;

--Refund Analysis
--> use query;Show me refund trends by month for the past year
---->> sql query: 
        SELECT 
            DATE_TRUNC('month', refund_date) as refund_month,
            COUNT(*) as refund_count,
            SUM(refund_amount_sgd) as total_refund_sgd,
            AVG(refund_amount_sgd) as avg_refund_sgd
        FROM core.t1_bookings_all 
        WHERE refund_date IS NOT NULL 
            AND refund_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY DATE_TRUNC('month', refund_date)
        ORDER BY refund_month;

--Booking Lead Time Analysis
--> use query; What’s the average booking lead time by destination?
---->> sql query:
        SELECT 
            destination_id,
            AVG(booking_lead_day) as avg_lead_days,
            COUNT(*) as booking_count
        FROM core.t1_bookings_all 
        WHERE is_confirmed_booking = 1 
            AND booking_lead_day >= 0
        GROUP BY destination_id
        HAVING COUNT(*) >= 20
        ORDER BY avg_lead_days DESC;

---Commission Analysis
--> use query; Show me commission earned by product name this year
---->> sql query:
        SELECT 
            product_name,
            COUNT(*) as bookings,
            SUM(commission_sgd) as total_commission_sgd,
            AVG(commission_sgd) as avg_commission_sgd
        FROM core.t1_bookings_all 
        WHERE DATE_TRUNC('year', booking_date_utc8) = DATE_TRUNC('year', CURRENT_DATE)
            AND is_confirmed_booking = 1
        GROUP BY product_name
        ORDER BY total_commission_sgd DESC;
---Loyalty Program Analysis
--> use query; How many loyalty rewards were earned and their total value?
---->> sql query: 
            SELECT 
                reward_type,
                COUNT(*) as rewards_earned,
                SUM(earned_reward) as total_reward_value,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM core.t1_bookings_all 
            WHERE earned_reward IS NOT NULL 
                AND reward_state = 'CREDITED'
            GROUP BY reward_type
            ORDER BY total_reward_value DESC;
