SELECT number FROM numbers(10) WHERE number NOT IN (SELECT number FROM numbers(5))
