-- Sales Demo Query

WITH CustomerInfo AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        d.department_name
    FROM 
        customers c
    JOIN 
        departments d ON c.department_id = d.department_id
    WHERE 
        c.active = 1
),
OrderSales AS (
    SELECT 
        o.order_id AS transaction_id,
        o.customer_id,
        p.product_id,
        o.order_date AS transaction_date,
        o.total_amount AS amount,
        'Sale' AS transaction_type
    FROM 
        orders o
    JOIN 
        products p ON o.product_id = p.product_id
    WHERE 
        o.order_date >= '2023-01-01'
),
OrderRefunds AS (
    SELECT 
        r.refund_id AS transaction_id,
        r.customer_id,
        p.product_id,
        r.refund_date AS transaction_date,
        -r.refund_amount AS amount,
        'Refund' AS transaction_type
    FROM 
        refunds r
    JOIN 
        products p ON r.product_id = p.product_id
    WHERE 
        r.refund_date >= '2023-01-01'
),
UnifiedTransactions AS (
    SELECT * FROM OrderSales
    UNION ALL
    SELECT * FROM OrderRefunds
)
SELECT 
    ci.first_name,
    ci.last_name,
    ci.email,
    ci.department_name,
    ut.transaction_id,
    ut.product_id,
    ut.transaction_date,
    ut.amount,
    ut.transaction_type
FROM 
    UnifiedTransactions ut
JOIN 
    CustomerInfo ci ON ut.customer_id = ci.customer_id
ORDER BY 
    ci.last_name,
    ut.transaction_date DESC;


-- Market Share Query

SELECT q1.brand_name,
         q1.category_name,
         q1.gross_total_by_category,
         q1.order_month,
         q1.cart_source,
         q2.category_gross_total
FROM 
    (SELECT DISTINCT brand_name,
         date_format(order_date,
         '%M') AS order_month, category_name, SUM(gross_total) AS "gross_total_by_category",cart_source
    FROM "accounting_prod"."public"."order_orderitem"
    WHERE order_date
        BETWEEN TIMESTAMP '2021-01-01 00:00:00'
            AND TIMESTAMP '2022-02-01 00:00:00'
    GROUP BY  brand_name,category_name,date_format(order_date, '%M'),cart_source
    ORDER BY  brand_name ASC) AS q1
LEFT JOIN 
    (SELECT DISTINCT category_name,
         date_format(order_date,
         '%M') AS category_month, sum(gross_total) AS "category_gross_total",cart_source
    FROM "accounting_prod"."public"."order_orderitem"
    WHERE order_date
        BETWEEN TIMESTAMP '2021-01-01 00:00:00'
            AND TIMESTAMP '2022-02-01 00:00:00'
    GROUP BY  category_name,category_name,date_format(order_date, '%M'),cart_source) AS q2
    ON q1.category_name=q2.category_name
        AND q1.order_month=q2.category_month
        AND q1.cart_source=q2.cart_source
