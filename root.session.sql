-- Active: 1747503580517@@127.0.0.1@3306@salesdata
SELECT * from sales

SELECT
    s.*,p.*
FROM sales s
LEFT JOIN products p
    ON s.productkey = p.productkey;

SELECT
    s.order_date,
    s.storekey,
    s.productkey,
    SUM(s.quantity) AS total_quantity,
    COUNT(DISTINCT s.order_number) AS order_count
FROM sales s
GROUP BY
    s.order_date,
    s.storekey,
    s.productkey;

