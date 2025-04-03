-- Data Analysis Project

---------------------------------------------------------------------
-- CHANGE OVER TIME
--------------------------------------------------------------------

--  Analyze Sales performance over time
SELECT 
	YEAR(order_date) 'Year',
	MONTH(order_date) 'month',
	SUM(sales) Total_sales,
	COUNT(DISTINCT customer_key) total_customers,
	SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE YEAR(order_date) IS NOT NULL
GROUP BY 
	YEAR(order_date),
	MONTH(order_date)
ORDER BY 'Year','month'

-- (changing the date format)
SELECT 
	FORMAT(order_date,'yyyy-MMM') order_date,
	SUM(sales) Total_sales,
	COUNT(DISTINCT customer_key) total_customers,
	SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 
	FORMAT(order_date,'yyyy-MMM')
ORDER BY order_date  -- But here the data is not sorted correctly

SELECT 
	DATETRUNC(MONTH,order_date) order_date,
	SUM(sales) Total_sales,
	COUNT(DISTINCT customer_key) total_customers,
	SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 
	DATETRUNC(MONTH,order_date)
ORDER BY order_date

--------------------------------------------------------------------
-- COMULATIVE ANALYSIS
--------------------------------------------------------------------

-- Calculate the total sales per month and the running total and moving average of sales over time
SELECT 
	order_Month,
	Total_Sales,
	SUM(total_sales) OVER(ORDER BY order_Month) Running_Total,
	SUM(total_sales) OVER(PARTITION BY YEAR(order_Month) ORDER BY order_Month) Running_Total_within_year,
	AVG(total_sales) OVER(PARTITION BY YEAR(order_Month) ORDER BY order_Month) Moving_Average_within_year
FROM (
	SELECT 
		DATETRUNC(MONTH,order_date) AS order_Month,
		SUM(sales) Total_Sales	
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH,order_date)
) T

--------------------------------------------------------------------
-- PERFORMANCE ANALYSIS
--------------------------------------------------------------------

-- Analyxe the yearly performance of products by comparing each product's sales to both its
-- average sales performace and previous year's sales
SELECT
	order_year,
	product_name,
	Total_Sales,
	AVG(Total_Sales) OVER(PARTITION BY product_name) AS average_sales,
	Total_Sales - AVG(Total_Sales) OVER(PARTITION BY product_name) AS avg_sales_diff,
	CASE
		WHEN Total_Sales - AVG(Total_Sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Avg'
		WHEN Total_Sales - AVG(Total_Sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Avg'
		ELSE 'Avg'
	END AS avg_change,
	-- year-over-year analysis
	LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
	Total_Sales - LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) AS yearly_sales_diff,
	CASE 
		WHEN Total_Sales - LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		WHEN Total_Sales - LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		ELSE 'No Change'
	END py_change

FROM (
	SELECT
		YEAR(s.order_date) order_year,
		p.product_name,
		SUM(s.sales) Total_Sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY YEAR(s.order_date), p.product_name
) T

--------------------------------------------------------------------
-- PART-TO-WHOLE ANALYSIS
--------------------------------------------------------------------

-- Which categories contribute the most to overall sales
WITH category_sales AS 
(
	SELECT 
		p.category,
		SUM(s.sales) sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	GROUP BY p.category
) 

SELECT 
	category,
	sales,
	SUM(sales) OVER() total_sales,
	CONCAT(ROUND((CAST(sales AS FLOAT) / SUM(sales) OVER()) * 100 , 2), '%') '%_contribution'
FROM category_sales
ORDER BY sales DESC

--------------------------------------------------------------------
-- DATA SEGMENTATION
--------------------------------------------------------------------

-- Segment Products into cost ranges and count how many products fall into each segment
WITH product_segment AS 
(
	SELECT 
		CASE 
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost <= 500 THEN '100 to 500'
			WHEN COST <= 1000 THEN '500 to 1000'
			ELSE 'Above 1000'
		END AS cost_segment,
		product_key
	FROM gold.dim_products
)
SELECT
	cost_segment,
	COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_segment
ORDER BY 
	CASE cost_segment
		WHEN 'Below 100' THEN 1
		WHEN '100 to 500' THEN 2
		WHEN '500 to 1000' THEN 3
		ELSE 4
	END

/* Group customers into three segments based on their spending behaviour:
     - VIP: Customers with atleast 12 month of history and spending more than $5000
	 - Regular: Customers with atleast 12 month of history but spending $5000 or less
	 - New: Customers with a lifespan less than 12 months
 and find the totsal number of customers by each group
*/

WITH customer_spending  As (
	SELECT 
		customer_key,
		SUM(Sales) total_sales,
		DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) total_months
	FROM gold.fact_sales
	GROUP BY customer_key
) 
SELECT 
	segment,
	COUNT(customer_key) total_customers
FROM (
	SELECT 
		customer_key,	
		CASE 
			WHEN total_months >= 12 AND total_sales > 5000 THEN 'VIP'
			WHEN total_months >= 12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS segment
	FROM customer_spending
) T
GROUP BY segment

/*
=======================================================================
CUSTOMER REPORT
=======================================================================
Purpose:
     - This report consoldates key customer metrics and behaviors

Highlights:
	1. Gathers essential fiels such as names , ages, and transaction details
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates custmer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
=======================================================================
*/

-- Creating a view of report for end users and dashboarding
CREATE VIEW gold.report_customers AS
	WITH base_query AS 
	(
	-- 1) Base Query : Retrives core columns from tables
		SELECT 
			s.order_number ,
			s.product_key,
			s.order_date,
			s.quantity,
			s.sales,
			c.customer_key,
			c.customer_number,
			c.first_name + ' ' + c.last_name AS name,
			DATEDIFF(YEAR,c.birthdate,GETDATE()) age
		FROM gold.fact_sales s
		LEFT JOIN gold.dim_customers c
		ON s.customer_key = c.customer_key
		WHERE s.order_date IS NOT NULL
	),

	customer_aggregation AS 
	(
	-- 2) Customer aggregation: Summarize key metrix at customer level
		SELECT 
			customer_key,
			customer_number,
			name,
			age,
			COUNT(DISTINCT order_number) AS total_orders,
			SUM(sales) AS total_sales,
			SUM(quantity) AS total_Quantity,
			COUNT(DISTINCT product_key) AS total_products,
			MAX(order_date) AS last_order_date,
			DATEDIFF(MONTH , MIN(order_date), MAX(order_date)) AS lifespan
		FROM base_query
		GROUP BY 
			customer_key,
			customer_number,
			name,
			age
	)
	-- 3) Generating Final report
	SELECT 
		customer_key,
		customer_number,
		name,
		age,
		CASE
			WHEN age < 20 THEN 'Under 20'
			WHEN age <= 29 THEN '20-29'
			WHEN age <= 39 THEN '30-39'
			WHEN age <= 49 THEN '40-49'
			WHEN age <= 59 THEN '50-59'
			ELSE ' 60 and Above'
		END AS age_group,
		CASE  
			WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
			WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_segment,
		DATEDIFF(MONTH , last_order_date , GETDATE()) AS recency,
		total_orders,
		total_sales,
		total_Quantity,
		total_products,
		lifespan,
	-- Compute average order value
		CASE 
			WHEN total_orders = 0 THEN 0
			ELSE total_sales / total_orders 
		END AS avg_order_value,
	-- Compute average monthly spend
		CASE 
			WHEN lifespan = 0 THEN total_sales
			ELSE total_sales / lifespan
		END AS avg_monthly_spend
	FROM customer_aggregation

SELECT * FROM gold.report_customers

/*
=======================================================================
PRODUCT REPORT
=======================================================================
Purpose:
     - This report consoldates key product metrics and behaviors

Highlights:
	1. Gather essential fields such as product name , category, subcategory and cost.
	2. Segments products by revenue to identify High-Performance , Mid-Range , or Low-Performers.
	3. Aggregates products-level matrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (month since last order)
		- average order revenue (AOR)
		- average monthly revenue
======================================================================
*/

-- Creating a view of report for end users and dashboarding
CREATE VIEW gold.report_products AS
	WITH base_query AS
	(	
		-- 1) Base Query : Retrives core columns from tables
		SELECT
			p.product_key,
			p.product_number,
			p.product_name,
			p.category,
			p.subcategory,
			p.cost,
			s.customer_key,
			s.order_number,
			s.order_date,
			s.quantity,
			s.sales
		FROM gold.fact_sales s
		LEFT JOIN gold.dim_products p
		ON s.product_key = p.product_key
		WHERE s.order_date IS NOT NULL
	),
	product_aggregations AS 
	(
		-- 2) product aggregation: Summarize key metrix at product level

		SELECT 
			product_key,
			product_number,
			product_name,
			category,
			subcategory,
			cost,
			COUNT(order_number) AS total_orders,
			SUM(sales) AS total_sales,
			SUM(quantity) AS total_quantity,
			COUNT(DISTINCT customer_key) AS total_customers,
			DATEDIFF(MONTH , MIN(order_date), MAX(order_date)) AS lifespan,
			MAX(order_date) AS last_order_date
		FROM base_query
		GROUP BY 
			product_key,
			product_number,
			product_name,
			category,
			subcategory,
			cost
	)

	-- 3) Final Report
	SELECT 
		product_key,
		product_number,
		product_name,
		category,
		subcategory,
		cost,
		total_orders,
		total_sales,
		CASE 
			WHEN total_sales < 20000 THEN 'Low-Performance'
			WHEN total_sales < 50000 THEN 'Mid-Range'
			ELSE 'High-Performance'
		END AS performance,
		total_quantity,
		total_customers,
		lifespan,
		last_order_date,
		DATEDIFF(MONTH, last_order_date , GETDATE()) AS recency,
		CASE
			WHEN total_orders = 0 THEN total_sales
			ELSE total_sales / total_orders 
		END AS avg_order_revenue,
		CASE
			WHEN lifespan = 0 THEN total_sales
			ELSE total_sales / lifespan 
		END AS avg_monthly_revenue
	
	FROM product_aggregations

SELECT * 
FROM gold.report_products
