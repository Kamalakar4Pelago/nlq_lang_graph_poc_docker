## Sample NLQ Queries and SQL Translations for Booking Sessions

### 1. Geographic Performance Analysis

**NLQ:** "Show me booking performance by continent for the last 30 days"
```sql
SELECT 
    continent,
    SUM(fulfillable_bookings) as total_bookings,
    SUM(gross_total_sgd) as total_revenue_sgd,
    SUM(commission_sgd) as total_commission_sgd,
    SUM(units_sold) as total_units_sold
FROM core.t2_bi_booking_sessions 
WHERE booking_date_utc8 >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY continent
ORDER BY total_revenue_sgd DESC;
```

**NLQ:** "Which countries have the highest booking completion rates this month?"
```sql
SELECT 
    country_name,
    SUM(completed_bookings) as completed_bookings,
    SUM(failed_bookings) as failed_bookings,
    ROUND(100.0 * SUM(completed_bookings) / NULLIF(SUM(completed_bookings) + SUM(failed_bookings), 0), 2) as completion_rate_pct
FROM core.t2_bi_booking_sessions 
WHERE DATE_TRUNC('month', booking_date_utc8) = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY country_name
HAVING SUM(completed_bookings) + SUM(failed_bookings) >= 10
ORDER BY completion_rate_pct DESC;
```

### 2. KrisFlyer Miles Analysis

**NLQ:** "Show me KrisFlyer miles earned vs redeemed by destination last quarter"
```sql
SELECT 
    destination_id,
    SUM(kf_earn_bookings) as miles_earning_bookings,
    SUM(kf_burn_bookings) as miles_redemption_bookings,
    SUM(kf_miles_accrued) as total_miles_earned,
    SUM(kf_miles_redeemed) as total_miles_redeemed
FROM core.t2_bi_booking_sessions 
WHERE booking_date_utc8 >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')
    AND booking_date_utc8 < DATE_TRUNC('quarter', CURRENT_DATE)
GROUP BY destination_id
HAVING SUM(kf_earn_bookings) > 0 OR SUM(kf_burn_bookings) > 0
ORDER BY total_miles_earned DESC;
```

**NLQ:** "What's the KrisFlyer miles burn rate by country?"
```sql
SELECT 
    country_name,
    SUM(kf_burn_bookings) as redemption_bookings,
    SUM(kf_miles_redeemed) as miles_redeemed,
    AVG(kf_miles_redeemed) as avg_miles_per_redemption
FROM core.t2_bi_booking_sessions 
WHERE kf_burn_bookings > 0
GROUP BY country_name
ORDER BY miles_redeemed DESC;
```

### 3. Payment Method Analysis

**NLQ:** "Show me KrisPay adoption rates by continent"
```sql
SELECT 
    continent,
    SUM(krispay_bookings) as krispay_bookings,
    SUM(fulfillable_bookings) as total_bookings,
    ROUND(100.0 * SUM(krispay_bookings) / NULLIF(SUM(fulfillable_bookings), 0), 2) as krispay_adoption_rate_pct
FROM core.t2_bi_booking_sessions 
GROUP BY continent
ORDER BY krispay_adoption_rate_pct DESC;
```

### 4. Booking Failure Analysis

**NLQ:** "What are the main reasons for booking failures by country?"
```sql
SELECT 
    country_name,
    SUM(failed_bookings) as failed_bookings,
    SUM(rejected_by_provider_bookings) as provider_rejections,
    SUM(rejected_by_system_bookings) as system_rejections,
    SUM(cancelled_by_provider_bookings) as provider_cancellations,
    SUM(cancelled_by_customer_bookings) as customer_cancellations,
    SUM(completed_bookings) as successful_bookings
FROM core.t2_bi_booking_sessions 
WHERE booking_date_utc8 >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY country_name
ORDER BY failed_bookings DESC;
```

### 5. Customer Segmentation Analysis

**NLQ:** "Show me new vs returning customer performance this month"
```sql
SELECT 
    CASE 
        WHEN booking_date_utc8 = first_booking_date THEN 'New Customer'
        ELSE 'Returning Customer'
    END as customer_segment,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(fulfillable_bookings) as total_bookings,
    SUM(gross_total_sgd) as total_revenue_sgd,
    AVG(gross_total_sgd) as avg_revenue_per_session
FROM core.t2_bi_booking_sessions 
WHERE DATE_TRUNC('month', booking_date_utc8) = DATE_TRUNC('month', CURRENT_DATE)
    AND customer_id IS NOT NULL
GROUP BY customer_segment;
```

### 6. Marketing Cost Analysis

**NLQ:** "What's our marketing promo cost vs revenue by destination?"
```sql
SELECT 
    destination_id,
    SUM(marketing_promo_cost_sgd) as total_promo_cost_sgd,
    SUM(gross_total_sgd) as total_revenue_sgd,
    ROUND(100.0 * SUM(marketing_promo_cost_sgd) / NULLIF(SUM(gross_total_sgd), 0), 2) as promo_cost_percentage,
    SUM(fulfillable_bookings) as bookings_with_promos
FROM core.t2_bi_booking_sessions 
WHERE marketing_promo_cost_sgd > 0
GROUP BY destination_id
ORDER BY total_promo_cost_sgd DESC;
```

### 7. Session-Level Performance

**NLQ:** "Show me average bookings per session by country"
```sql
SELECT 
    country_name,
    COUNT(DISTINCT ds_session_id) as unique_sessions,
    SUM(fulfillable_bookings) as total_bookings,
    ROUND(SUM(fulfillable_bookings)::FLOAT / NULLIF(COUNT(DISTINCT ds_session_id), 0), 2) as avg_bookings_per_session
FROM core.t2_bi_booking_sessions 
WHERE ds_session_id IS NOT NULL
GROUP BY country_name
ORDER BY avg_bookings_per_session DESC;
```

### 8. Product Performance Analysis

**NLQ:** "Which products have the highest units sold per booking?"
```sql
SELECT 
    product_id,
    SUM(fulfillable_bookings) as total_bookings,
    SUM(units_sold) as total_units,
    ROUND(SUM(units_sold)::FLOAT / NULLIF(SUM(fulfillable_bookings), 0), 2) as avg_units_per_booking,
    SUM(gross_total_sgd) as total_revenue_sgd
FROM core.t2_bi_booking_sessions 
WHERE product_id IS NOT NULL
GROUP BY product_id
HAVING SUM(fulfillable_bookings) >= 10
ORDER BY avg_units_per_booking DESC;
```

### 9. Daily Trend Analysis

**NLQ:** "Show me daily booking trends for the past 7 days"
```sql
SELECT 
    booking_date_utc8,
    SUM(fulfillable_bookings) as daily_bookings,
    SUM(gross_total_sgd) as daily_revenue_sgd,
    SUM(commission_sgd) as daily_commission_sgd,
    COUNT(DISTINCT customer_id) as unique_customers
FROM core.t2_bi_booking_sessions 
WHERE booking_date_utc8 >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY booking_date_utc8
ORDER BY booking_date_utc8;
```

### 10. Cross-Continental Analysis

**NLQ:** "Compare booking performance between Asia and Europe this year"
```sql
SELECT 
    continent,
    SUM(fulfillable_bookings) as total_bookings,
    SUM(gross_total_sgd) as total_revenue_sgd,
    AVG(gross_total_sgd) as avg_revenue_per_session,
    SUM(kf_miles_accrued) as total_miles_earned,
    COUNT(DISTINCT customer_id) as unique_customers
FROM core.t2_bi_booking_sessions 
WHERE DATE_TRUNC('year', booking_date_utc8) = DATE_TRUNC('year', CURRENT_DATE)
    AND continent IN ('AS', 'EU')
GROUP BY continent
ORDER BY total_revenue_sgd DESC;
```

### 11. Discount Impact Analysis

**NLQ:** "What's the impact of discounts on booking conversion?"
```sql
SELECT 
    CASE 
        WHEN discount_sgd > 0 THEN 'With Discount'
        ELSE 'No Discount'
    END as discount_segment,
    SUM(fulfillable_bookings) as successful_bookings,
    SUM(failed_bookings) as failed_bookings,
    ROUND(100.0 * SUM(fulfillable_bookings) / NULLIF(SUM(fulfillable_bookings) + SUM(failed_bookings), 0), 2) as success_rate_pct,
    AVG(gross_total_sgd) as avg_revenue_per_session
FROM core.t2_bi_booking_sessions 
GROUP BY discount_segment;
```

## Key NLQ System Prompt Guidelines for Session-Level Data

When implementing these samples in your NLQ system prompt, ensure the LLM:

1. **Understands aggregated nature** - Data is pre-aggregated at session/user/day/destination/country/continent level
2. **Uses SUM for metrics** - Most fields are already aggregated counts/sums that need to be summed again for different groupings
3. **Handles geographic dimensions** - Properly uses country_name, continent, and destination_id for geographic analysis
4. **Recognizes KrisFlyer-specific metrics** - Uses kf_earn_bookings, kf_burn_bookings, kf_miles_accrued, kf_miles_redeemed appropriately
5. **Applies proper date filtering** - Uses booking_date_utc8 for date-based filtering
6. **Considers session-level analysis** - Leverages ds_session_id, ds_user_id for user behavior analysis
7. **Uses fulfillable_bookings** - Primary metric for successful bookings instead of raw booking counts
8. **Handles NULL values** - Uses NULLIF for division operations to avoid divide-by-zero errors
9. **Applies business logic** - Understands different booking states and their business meanings
10. **Considers customer segmentation** - Uses first_booking_date vs booking_date_utc8 for new/returning customer analysis

These samples cover the most common business questions for session-level booking analytics and demonstrate proper SQL patterns for this aggregated dataset structure.

