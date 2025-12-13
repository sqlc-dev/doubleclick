SELECT number FROM numbers(10) WHERE number IN (SELECT number FROM numbers(5))
