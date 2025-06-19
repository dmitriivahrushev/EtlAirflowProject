CREATE MATERIALIZED VIEW IF NOT EXISTS mart.f_customer_retention_mv AS
WITH sales_with_week AS (
SELECT
    s.customer_id,
    s.item_id,
    s.payment_amount,
    s.quantity,
    s.date_id,
    c.week_of_year AS period_id,
    c.week_of_year_iso AS period_name,
CASE WHEN s.payment_amount < 0 THEN 'refunded' END AS refund_flag
FROM mart.f_sales s
JOIN mart.d_calendar c ON s.date_id = c.date_id
),

orders_per_period AS (
SELECT
    customer_id,
    period_id,
    COUNT(*) AS orders_in_week
FROM sales_with_week
GROUP BY customer_id, period_id
),

customer_type AS (
SELECT
    o.customer_id,
    o.period_id,
    CASE
        WHEN o.orders_in_week = 1 THEN 'new'
        WHEN o.orders_in_week > 1 THEN 'returning'
    END AS customer_group
FROM orders_per_period o
),

classified_sales AS (
SELECT
    s.*,
    ct.customer_group
FROM sales_with_week s
LEFT JOIN customer_type ct ON s.customer_id = ct.customer_id AND s.period_id = ct.period_id
)

SELECT
    period_name,
    period_id,
    item_id,
    COUNT(DISTINCT CASE WHEN customer_group = 'new' THEN customer_id END) AS new_customers_count,
    COUNT(DISTINCT CASE WHEN customer_group = 'returning' THEN customer_id END) AS returning_customers_count,
    COUNT(DISTINCT CASE WHEN refund_flag = 'refunded' THEN customer_id END) AS refunded_customer_count,
    SUM(CASE WHEN customer_group = 'new' THEN payment_amount ELSE 0 END) AS new_customers_revenue,
    SUM(CASE WHEN customer_group = 'returning' THEN payment_amount ELSE 0 END) AS returning_customers_revenue,
    SUM(CASE WHEN refund_flag = 'refunded' THEN payment_amount ELSE 0 END) AS customers_refunded
FROM classified_sales
GROUP BY period_name, period_id, item_id
ORDER BY period_id, item_id;