SELECT number, nth_value(number, 2) OVER (ORDER BY number) FROM numbers(10)
