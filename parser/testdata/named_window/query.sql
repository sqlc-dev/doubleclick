SELECT number, sum(number) OVER w FROM numbers(10) WINDOW w AS (ORDER BY number)
