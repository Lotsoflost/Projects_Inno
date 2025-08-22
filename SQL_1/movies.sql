-- 1. Return the number of films in each category, sorted in descending order.

SELECT
    cg.name AS category_name,
    COUNT(c.film_id) AS film_count
FROM film AS f
JOIN film_category AS c
    ON c.film_id = f.film_id
JOIN category AS cg
    ON cg.category_id = c.category_id
GROUP BY cg.name
ORDER BY film_count DESC;



-- 2. Return the 10 actors whose films were rented the most, sorted in descending order.
WITH cte_most_rented_films AS (
    SELECT 
        f.film_id, sum(p.amount) AS total_sales
    FROM film f
    JOIN inventory i ON f.film_id = i.inventory_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN payment p ON p.rental_id = r.rental_id
    GROUP BY f.film_id, f.title
)

SELECT 
    a.first_name || ' ' || a.last_name AS top_actor_name, SUM(f.total_sales) total_sales
FROM film_actor fa 
    JOIN cte_most_rented_films f ON fa.film_id = f.film_id
    JOIN actor a ON fa.actor_id = a.actor_id
    GROUP BY f.total_sales, a.first_name, a.last_name
    ORDER BY 2 DESC
    LIMIT 10;

-- 3. Return the film category with the highest total amount of money spent.
SELECT * FROM rental_by_category LIMIT 1;
/*
 the mat view was already created
 create materialized view if not exists public.rental_by_category as
SELECT c.name        AS category,
       sum(p.amount) AS total_sales
FROM payment p
         JOIN rental r ON p.rental_id = r.rental_id
         JOIN inventory i ON r.inventory_id = i.inventory_id
         JOIN film f ON i.film_id = f.film_id
         JOIN film_category fc ON f.film_id = fc.film_id
         JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY (sum(p.amount)) DESC;

alter materialized view public.rental_by_category owner to postgres;

create unique index if not exists rental_category
    on public.rental_by_category (category);

refresh materialized view public.rental_by_category
 */
-- 4. Return the titles of films that are not in the inventory; 
-- write the query without using the IN operator.

SELECT 
    f.film_id, f.title 
FROM film f
    LEFT JOIN inventory i ON f.film_id = i.film_id
    WHERE i.film_id IS NULL;

-- 5. Return the top 3 actors who appeared in the most films in the "Children" category; 
-- if there are ties, return all tied actors.

WITH cte_prepare AS (
    SELECT
        c.name AS category_name,
        a.first_name || ' ' || a.last_name AS top_actor_name,
        COUNT(*) AS film_cnt
FROM film AS f
    JOIN film_category AS fc
        ON fc.film_id = f.film_id
    JOIN category AS c
        ON c.category_id = fc.category_id
    JOIN film_actor AS fa
        ON fa.film_id = f.film_id
    JOIN actor AS a
        ON a.actor_id = fa.actor_id
    WHERE c.name = 'Children'
    GROUP BY c.name, a.first_name, a.last_name
),
cte_with_max AS (
    SELECT
        *,
        MAX(film_cnt) OVER () AS max_film_cnt  
    FROM cte_prepare
)
SELECT
    category_name,
    top_actor_name,
    film_cnt
FROM cte_with_max
WHERE film_cnt = max_film_cnt
ORDER BY top_actor_name;


-- 6. Return cities with counts of active and inactive customers (active — customer.active = 1), 
-- sorted by the number of inactive customers in descending order.
SELECT
    ct.city AS city_name,
    COUNT(*) FILTER (WHERE c.active = 1) AS active_custs,
    COUNT(*) FILTER (WHERE c.active = 0) AS inactive_custs
FROM customer AS c
JOIN address AS ad ON ad.address_id = c.address_id
JOIN city    AS ct ON ct.city_id     = ad.city_id
GROUP BY ct.city
ORDER BY inactive_custs DESC, city_name;


-- 7. In a single query, return the film category with the largest total number of rental
-- hours in cities (where the customer.address_id belongs to that city) whose names start with "a",
-- and do the same for cities whose names contain the "-" character.

SELECT DISTINCT ON (tag)
    category_name, tag, round(cast(total_hours as numeric), 2) as total_hours
FROM (
    SELECT
        cg.name AS category_name,
        CASE WHEN UPPER(ct.city) LIKE 'A%' THEN 'A-city' ELSE 'hyphen-city' END AS tag,
        SUM(EXTRACT(EPOCH FROM (COALESCE(r.return_date, current_timestamp) - r.rental_date)) / 3600.0) AS total_hours
    FROM rental AS r
    JOIN customer  AS c  ON c.customer_id   = r.customer_id
    JOIN address   AS ad ON ad.address_id   = c.address_id
    JOIN city      AS ct ON ct.city_id      = ad.city_id
    JOIN inventory AS i  ON i.inventory_id  = r.inventory_id
    JOIN film      AS f  ON f.film_id       = i.film_id
    JOIN film_category AS fc ON fc.film_id  = f.film_id
    JOIN category  AS cg ON cg.category_id  = fc.category_id
    WHERE (UPPER(ct.city) LIKE 'A%' OR ct.city LIKE '%-%')
    GROUP BY cg.name, tag
) AS agg
ORDER BY tag, total_hours DESC, category_name;

