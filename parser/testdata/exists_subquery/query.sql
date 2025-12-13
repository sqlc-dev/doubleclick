SELECT number FROM numbers(10) WHERE EXISTS (SELECT 1 FROM numbers(5) WHERE number = 1)
