SELECT number, rank() OVER (ORDER BY number) FROM numbers(10)
