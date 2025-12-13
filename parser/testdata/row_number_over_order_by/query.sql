SELECT number, row_number() OVER (ORDER BY number) FROM numbers(10)
