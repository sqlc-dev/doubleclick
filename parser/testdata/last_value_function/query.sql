SELECT number, last_value(number) OVER (ORDER BY number) FROM numbers(10)
