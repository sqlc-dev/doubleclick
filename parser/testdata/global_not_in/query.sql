SELECT * FROM test_table WHERE id GLOBAL NOT IN (SELECT id FROM other_table)
