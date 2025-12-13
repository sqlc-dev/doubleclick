SELECT number, lead(number) OVER (ORDER BY number) FROM numbers(10)
