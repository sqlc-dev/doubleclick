SELECT number % 10, count(*) FROM numbers(100) GROUP BY number % 10 WITH TOTALS
