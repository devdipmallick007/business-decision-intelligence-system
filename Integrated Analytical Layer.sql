-- Active: 1747503580517@@127.0.0.1@3306@salesdata
SELECT
    order_number,
    line_item,
    order_date,
    delivery_date,
    customerkey,
    storekey,
    productkey,
    quantity,
    currency_code
FROM sales;


SELECT
    s.*,
    c.gender,
    c.city,
    c.state,
    c.country,
    c.continent,
    c.birthday
FROM sales s
LEFT JOIN customers c
    ON s.customerkey = c.customerkey;


SELECT
    s.*,
    c.gender,
    c.city,
    c.state,
    c.country,
    c.continent,
    p.product_name,
    p.brand,
    p.category,
    p.subcategory,
    p.unit_price_usd,
    p.unit_cost_usd
FROM sales s
LEFT JOIN customers c
    ON s.customerkey = c.customerkey
LEFT JOIN products p
    ON s.productkey = p.productkey;


SELECT
    s.*,
    c.gender,
    c.city,
    c.state,
    c.country,
    c.continent,
    p.product_name,
    p.brand,
    p.category,
    p.subcategory,
    p.unit_price_usd,
    p.unit_cost_usd,
    st.country   AS store_country,
    st.state     AS store_state,
    st.square_meters,
    st.open_date
FROM sales s
LEFT JOIN customers c
    ON s.customerkey = c.customerkey
LEFT JOIN products p
    ON s.productkey = p.productkey
LEFT JOIN stores st
    ON s.storekey = st.storekey;


SELECT
    s.*,
    c.gender,
    c.city,
    c.state,
    c.country,
    c.continent,
    p.product_name,
    p.brand,
    p.category,
    p.subcategory,
    p.unit_price_usd,
    p.unit_cost_usd,
    st.country   AS store_country,
    st.state     AS store_state,
    st.square_meters,
    st.open_date,
    er.exchange
FROM sales s
LEFT JOIN customers c
    ON s.customerkey = c.customerkey
LEFT JOIN products p
    ON s.productkey = p.productkey
LEFT JOIN stores st
    ON s.storekey = st.storekey
LEFT JOIN exchangerates er
    ON s.currency_code = er.currency
   AND s.order_date = er.rate_date;


SELECT
    s.order_number,
    s.line_item,
    s.order_date,
    s.delivery_date,
    s.customerkey,
    s.storekey,
    s.productkey,
    s.quantity,
    s.currency_code,
    er.exchange,

    p.unit_price_usd * er.exchange AS unit_price_usd_fx,
    p.unit_cost_usd  * er.exchange AS unit_cost_usd_fx,


    s.quantity * (p.unit_price_usd * er.exchange) AS gross_revenue_usd,
    s.quantity * (p.unit_cost_usd  * er.exchange) AS total_cost_usd,
    (s.quantity * (p.unit_price_usd * er.exchange)) -
    (s.quantity * (p.unit_cost_usd  * er.exchange)) AS gross_margin_usd

FROM sales s
LEFT JOIN products p
    ON s.productkey = p.productkey
LEFT JOIN exchangerates er
    ON s.currency_code = er.currency
   AND s.order_date = er.rate_date;



CREATE TABLE sales_event_analytical AS
SELECT
    s.order_number,
    s.line_item,
    s.order_date,
    s.delivery_date,
    s.customerkey,
    s.storekey,
    s.productkey,
    s.quantity,
    s.currency_code,
    er.exchange,

    c.gender,
    c.city,
    c.state,
    c.country,
    c.continent,

    p.product_name,
    p.brand,
    p.category,
    p.subcategory,

    st.country   AS store_country,
    st.state     AS store_state,
    st.square_meters,
    st.open_date,

    p.unit_price_usd * er.exchange AS unit_price_usd_fx,
    p.unit_cost_usd  * er.exchange AS unit_cost_usd_fx,

    s.quantity * (p.unit_price_usd * er.exchange) AS gross_revenue_usd,
    s.quantity * (p.unit_cost_usd  * er.exchange) AS total_cost_usd,
    (s.quantity * (p.unit_price_usd * er.exchange)) -
    (s.quantity * (p.unit_cost_usd  * er.exchange)) AS gross_margin_usd

FROM sales s
LEFT JOIN customers c
    ON s.customerkey = c.customerkey
LEFT JOIN products p
    ON s.productkey = p.productkey
LEFT JOIN stores st
    ON s.storekey = st.storekey
LEFT JOIN exchangerates er
    ON s.currency_code = er.currency
   AND s.order_date = er.rate_date;


SELECT
    order_number,
    line_item,
    COUNT(*) AS cnt
FROM sales_event_analytical
GROUP BY order_number, line_item
HAVING cnt > 1;


CREATE TABLE sales_daily_store_product AS
SELECT
    order_date,
    storekey,
    productkey,
    SUM(quantity)            AS total_quantity,
    SUM(gross_revenue_usd)   AS total_revenue_usd,
    SUM(gross_margin_usd)    AS total_margin_usd,
    AVG(unit_price_usd_fx)   AS avg_unit_price_usd
FROM sales_event_analytical
GROUP BY order_date, storekey, productkey;
