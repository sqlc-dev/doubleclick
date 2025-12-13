SELECT number, dense_rank() OVER (ORDER BY number) FROM numbers(10)
