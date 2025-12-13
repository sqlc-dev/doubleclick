SELECT count(*) FROM users GROUP BY status HAVING count(*) > 1
