CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.user_order_log (
    uniq_id        VARCHAR(32) PRIMARY KEY NOT NULL,
    date_time      timestamp NOT NULL,
    city_id        integer   NOT NULL,
    city_name      VARCHAR(100),
    customer_id    integer   NOT NULL,
    first_name     VARCHAR(100),
    last_name      VARCHAR(100),
    item_id        integer   NOT NULL,
    item_name      VARCHAR(100),
    quantity       bigint,
    payment_amount numeric(10, 2),
    status         VARCHAR(10)
);


