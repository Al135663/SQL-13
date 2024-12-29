--sql-13

-- SQL query to rank movies by revenue within each genre
WITH RankedMovies AS (
    SELECT 
        m.movie_id, 
        m.title, 
        m.revenue,
        g.genre_name,
        ROW_NUMBER() OVER (PARTITION BY g.genre_id ORDER BY m.revenue DESC) as revenue_rank
    from  movie as m
    JOIN movie_genres as mg on m.movie_id = mg.movie_id
    JOIN genre as g on mg.genre_id = g.genre_id)
select 
    movie_id, 
    title, 
    revenue,
    genre_name,
    revenue_rank from RankedMovies
where revenue_rank <= 10;

--Using RANK and DENSE_RANK for Ranking Rows

WITH MovieRatings as (
select m.movie_id, m.title, g.genre_name, m.vote_average,
        RANK() OVER (PARTITION BY g.genre_id ORDER BY m.vote_average DESC) as vote_rank,
        DENSE_RANK() OVER (PARTITION BY g.genre_id ORDER BY m.vote_average DESC) as vote_dense_rank
from movie as m
JOIN movie_genres as mg ON m.movie_id = mg.movie_id
JOIN genre as g ON mg.genre_id = g.genre_id)
SELECT movie_id, title, genre_name,vote_average, vote_rank, vote_dense_rank
FROM MovieRatings
ORDER BY genre_name, vote_rank;

-- TSQL13 ICA Demo [3]: Using NTILE to Distribute Rows Across Buckets
WITH RevenueQuartiles as (
    select
        m.movie_id,
        m.title,
        m.revenue,
        NTILE(4) OVER (ORDER BY m.revenue DESC) as revenue_quartile
FROM movie as m
WHERE m.revenue IS NOT NULL)  -- Ensuring only movies with non-null revenue are considered
SELECT movie_id, title, revenue, revenue_quartile
from RevenueQuartiles
ORDER by revenue_quartile, revenue DESC;

-- Applying OFFSET and FETCH for Pagination
-- SQL query to paginate movies by release date
SELECT movie_id, title,  release_date 
from movie 
order by release_date DESC
OFFSET 5 ROWS       -- Start from the fifth row 
FETCH NEXT 8 ROWS ONLY; -- Retrieve only the next 8 rows


--Using LAG and LEAD for Accessing Prior and Subsequent Rows 
WITH RevenueComparison as (
select m.movie_id, m.title, g.genre_name, m.revenue, m.release_date,
 LAG(m.revenue) OVER (PARTITION BY g.genre_id ORDER BY m.release_date) AS previous_revenue,
 LEAD(m.revenue) OVER (PARTITION BY g.genre_id ORDER BY m.release_date) AS next_revenue
from movie as m
 JOIN movie_genres as mg ON m.movie_id = mg.movie_id
 JOIN genre as g ON mg.genre_id = g.genre_id)
select movie_id,title,genre_name,revenue,release_date, previous_revenue,next_revenue
FROM RevenueComparison
ORDER BY genre_name, release_date;

--Aggregating Data with PARTITION BY and OVER Clauses.
SELECT m.movie_id, m.title, g.genre_name, m.revenue,
 SUM(m.revenue) OVER (PARTITION BY g.genre_id) as total_genre_revenue,
AVG(m.revenue) OVER (PARTITION BY g.genre_id) as average_genre_revenue
from movie as m
join movie_genres as mg on m.movie_id = mg.movie_id
join genre as g on mg.genre_id = g.genre_id
ORDER BY g.genre_name, m.revenue DESC;


--Using FIRST_VALUE and LAST_VALUE in Window Functions
WITH GenreEvolution as (select m.movie_id, m.title,g.genre_name, m.release_date,
  FIRST_VALUE(m.title) OVER (PARTITION BY g.genre_id ORDER BY m.release_date) as first_movie_in_genre,
  LAST_VALUE(m.title) OVER (PARTITION BY g.genre_id ORDER BY m.release_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
  AS last_movie_in_genre
from movie as m
    JOIN movie_genres as mg on m.movie_id = mg.movie_id
    JOIN genre as g on mg.genre_id = g.genre_id)
SELECT
    movie_id,
    title,
    genre_name,
    release_date,
    first_movie_in_genre,
    last_movie_in_genre
from GenreEvolution
order by genre_name, release_date;

--Optimizing Window Functions for Performance.
select m.movie_id, m.title,g.genre_name, m.revenue,
SUM(m.revenue) OVER (PARTITION BY g.genre_id ORDER BY m.release_date 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
from movie as m
JOIN movie_genres mg ON m.movie_id = mg.movie_id
JOIN genre g ON mg.genre_id = g.genre_id
WHERE m.revenue IS NOT NULL
ORDER BY g.genre_name, m.release_date;
