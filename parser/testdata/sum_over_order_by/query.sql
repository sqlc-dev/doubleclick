SELECT number, sum(number) OVER (ORDER BY number) FROM numbers(10)
