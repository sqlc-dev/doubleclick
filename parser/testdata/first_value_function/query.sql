SELECT number, first_value(number) OVER (ORDER BY number) FROM numbers(10)
