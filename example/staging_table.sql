create schema staging;

create table staging.user_order_log
(
    uniq_id  varchar(32)       not null
        constraint user_order_log_pk
            primary key,
    date_time      timestamp not null,
    city_id        integer   not null,
    city_name      varchar(100),
    customer_id    integer   not null,
    first_name     varchar(100),
    last_name      varchar(100),
    item_id        integer   not null,
    item_name      varchar(100),
    quantity       bigint,
    payment_amount numeric(10, 2)
);

alter table staging.user_order_log
    owner to jovyan;
create index uo1
    on staging.user_order_log (customer_id);
create index uo2
    on staging.user_order_log (item_id);


create schema mart;

CREATE TABLE mart.d_calendar
(
  date_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE mart.d_calendar ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_id);

CREATE INDEX d_date_date_actual_idx
  ON mart.d_calendar(date_actual);

COMMIT;

INSERT INTO mart.d_calendar
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;

create table mart.d_city
(
    id        serial
        primary key,
    city_id   integer
        unique,
    city_name varchar(50)
);

alter table mart.d_city
    owner to jovyan;

create index d_city1
    on mart.d_city (city_id);

create table mart.d_customer
(
    id          serial
        primary key,
    customer_id integer not null
        unique,
    first_name  varchar(15),
    last_name   varchar(15),
    city_id     integer
);

alter table mart.d_customer
    owner to jovyan;

create index d_cust1
    on mart.d_customer (customer_id);

create table mart.d_item
(
    id        serial
        primary key,
    item_id   integer not null
        unique,
    item_name varchar(50)
);

alter table mart.d_item
    owner to jovyan;

create unique index d_item1
    on mart.d_item (item_id);

CREATE TABLE mart.f_sales(
   id  serial 
        primary key,
   date_id          INT NOT NULL,
   item_id          INT NOT NULL,
   customer_id      INT NOT NULL,
   city_id          INT NOT NULL,
   quantity         BIGINT,
   payment_amount   DECIMAL(10,2),
   FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
   FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id),
   FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id),
   FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);

alter table mart.d_item
    owner to jovyan;

CREATE INDEX f_ds1  ON mart.f_sales (date_id);
CREATE INDEX f_ds2  ON mart.f_sales (item_id);
CREATE INDEX f_ds3  ON mart.f_sales (customer_id);
CREATE INDEX f_ds4  ON mart.f_sales (city_id);