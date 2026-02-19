
-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================

-- check for uniqness for customers 
SELECT * FROM gold.dim_customers

SELECT customer_key,
COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) >1


-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================
SELECT product_key,
COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) >1

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * FROM gold.dim_products
SELECT * FROM gold.dim_customers
SELECT * FROM gold.fact_sales


SELECT *
FROM gold. fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
WHERE C.customer_key IS  NULL OR P.product_key IS  NULL