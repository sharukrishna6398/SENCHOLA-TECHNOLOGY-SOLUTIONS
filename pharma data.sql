use pharma;
create table pharma_tb(Distributor varchar(200),
customer varchar(200),
city varchar(200),
country varchar(200),
latitude float,
longitude float,
channel_name varchar(200),
sub_channel varchar(200),
product_name varchar(200),
product_class varchar(200),
quantity int,
price int,
sales int,month_name varchar(200),
year_ int,
name_of_the_sales_rep varchar(200),
manager varchar(200),
sales_team varchar(200));
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pharma.csv'
INTO TABLE pharma_tb
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select*from pharma_tb;
SELECT DISTINCT country from  pharma_tb;
SELECT customer
FROM pharma_tb
WHERE sub_channel = 'Retail';

SELECT product_class ,sum(quantity) total_quantity
FROM pharma_tb
group by product_class
having product_class='Antibiotics';
select*from pharma_tb;
select year_,sum(sales) as sales
from pharma_tb group by year_;
SELECT customer, SUM(sales) AS highest_sales
FROM pharma_tb
GROUP BY customer
ORDER BY highest_sales DESC
LIMIT 1;

select name_of_the_sales_rep,manager from pharma_tb where manager='James Goodwill';
SELECT city, SUM(sales) AS highest_sales
FROM pharma_tb
GROUP BY city
ORDER BY highest_sales DESC
LIMIT 5;
select name_of_the_sales_rep,sum(sales) from pharma_tb group by name_of_the_sales_rep;

SELECT product_class, month_name, year_, SUM(sales) as total_sales
FROM pharma_tb
GROUP BY product_class, month_name, year_
LIMIT 50000;
SELECT name_of_the_sales_rep, SUM(sales) AS total_sales
FROM pharma_tb
WHERE YEAR_ = 2019
GROUP BY name_of_the_sales_rep
ORDER BY total_sales DESC
LIMIT 3;
SELECT 
    YEAR_ AS sales_year,
    month_name  AS sales_month,
    sub_channel,
    SUM(sales) AS monthly_sales
FROM pharma_tb
GROUP BY sales_year, sales_month, sub_channel
ORDER BY sub_channel, sales_year, sales_month;
select*from pharma_tb;
SELECT 
    product_class,
    SUM(sales) AS total_sales,
    AVG(price) AS average_price,
    SUM(quantity) AS total_quantity_sold
FROM pharma_tb
GROUP BY product_class
ORDER BY product_class;

SELECT year_, customer, total_sales
FROM (
    SELECT year_, customer, SUM(sales) AS total_sales,
           ROW_NUMBER() OVER (PARTITION BY year_ ORDER BY SUM(sales) DESC) AS sales_rank
    FROM pharma_tb
    GROUP BY year_, customer
) ranked_sales
WHERE sales_rank <= 5;

select country,round(sum(sales))as total_sales,year_ from pharma_tb group by year_,country
 order by year_;
SELECT year_, month_name, MIN(total_sales) AS lowest_sales
FROM (
    SELECT YEAR_ AS year_, MONTH_NAME AS month_name, SUM(sales) AS total_sales
    FROM pharma_tb
    GROUP BY YEAR_, MONTH_NAME
) AS sales
GROUP BY year_, month_name
LIMIT 0, 50000;



WITH tot_sales AS (
    SELECT 
        RANK() OVER(PARTITION BY sub_channel ORDER BY SUM(sales)) AS rnk,
        SUM(sales) AS total_sales,
        country,
        sub_channel
    FROM pharma_tb
    GROUP BY country, sub_channel
)
SELECT 
    tot_sales.country,
    tot_sales.sub_channel,
    tot_sales.total_sales
FROM tot_sales
WHERE tot_sales.rnk = 1;


