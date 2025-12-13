SELECT * FROM test_table WHERE id GLOBAL IN (SELECT id FROM other_table)
