SELECT StartDate, sumMerge(Visits) AS Visits, uniqMerge(Users) AS Users FROM basic_mv GROUP BY StartDate ORDER BY StartDate
