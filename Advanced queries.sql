select category_id,
	product_name,
	count(*),
	min(price) as "Low-price",
	max(price) as "high-price",
	round(avg(price), 2) as "avg-price"
from inventory.products
group by rollup (category_id, product_name) -- subtotal
order by category_id, product_name;


select category_id, 
	size,
	count(*),
	min(price) as "Low-price",
	max(price) as "high-price",
	round(avg(price), 2) as "avg-price"
from inventory.products
group by cube (category_id, size) -- possible combinations of groups
order by category_id, size;


-- Segmenting groups with aggregate filters
select category_id,
	count(*) as "Count_all",
	avg(price) as "avg-price",
	-- small products
	count(*) filter (where size <= 16) as "count small",
	avg(price) filter (where size <= 16) as "avg-price small",
	-- large products
	count(*) filter (where size > 16) as "count large",
	avg(price) filter (where size > 16) as "avg-price large"
from inventory.products
group by rollup(category_id)
order by category_id

---------------------------------------------------------------

With a as (
	select sku,
	product_name,
	size,
	price,
	avg(price) over(partition by size),
	price - avg(price) over(partition by size) as "difference"
from inventory.products
group by sku, product_name, size
order by price - avg(price) over(partition by size) DESC)
select *
from a
where difference >= 0;
--or--
select *
from (select sku,
	product_name,
	size,
	price,
	avg(price) over(partition by size),
	price - avg(price) over(partition by size) as "difference"
from inventory.products
group by sku, product_name, size
order by price - avg(price) over(partition by size) DESC)
where difference >=0;


select sku,
	product_name,
	size,
	price,
	avg(price) over(xyz),
	max(price) over(xyz),
	min(price) over(xyz)
from inventory.products
window xyz as (partition by size);


select category_id,
	sum(category_id) over (order by category_id) as "Running total"
from inventory.categories;


select order_lines.order_id,
	order_lines.line_id,
	order_lines.sku,
	order_lines.quantity,
	products.price as "price each",
	order_lines.quantity * products.price as "line total",
	sum(order_lines.quantity * products.price)
		over(partition by order_id) as "order total",
	sum(order_lines.quantity * products.price)
		over(partition by order_id order by line_id) as "running total"
from sales.order_lines inner join inventory.products
	on order_lines.sku = products.sku;


select order_id,
	sum(order_id) over(order by order_id rows between 2 preceding and 0 following) 
		as "3 period leading sum",
	sum(order_id) over(order by order_id rows between 0 preceding and 2 following) 
		as "3 period trailing sum",
	avg(order_id) over(order by order_id rows between 1 preceding and 1 following)
		as "3 period moving avg"
from sales.orders;


select company,
	first_value(company) over(order by company
		rows between unbounded preceding and unbounded following),
	last_value(company) over(order by company
		rows between unbounded preceding and unbounded following),
	nth_value(company, 3) over(order by company
		rows between unbounded preceding and unbounded following)
from sales.customers
order by company;
	

select *
from sales.orders;


select distinct customer_id,
	first_value(order_date)
		over(partition by customer_id
			order by order_date
			rows between unbounded preceding and unbounded following),
	last_value(order_date)
		over(partition by customer_id
			order by order_date
			rows between unbounded preceding and unbounded following),
	last_value(order_date)
		over(partition by customer_id
			order by order_date
			rows between unbounded preceding and unbounded following) - 
	first_value(order_date)
		over(partition by customer_id
			order by order_date
			rows between unbounded preceding and unbounded following) as "days"
from sales.orders
order by customer_id;

---------------------------------------------------------------

select gender,
percentile_disc(0.5) within group (order by height_inches) as "discrete median",
percentile_cont(0.5) within group (order by height_inches) as "continuous median"
from public.people_heights
group by rollup (gender);









