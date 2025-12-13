SELECT number, lag(number) OVER (ORDER BY number) FROM numbers(10)
