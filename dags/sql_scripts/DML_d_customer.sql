INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT customer_id, first_name, last_name, max(city_id) from staging.user_order_log
WHERE customer_id NOT IN (SELECT customer_id FROM mart.d_customer)
GROUP BY customer_id, first_name, last_name
