SELECT
  CASE
    WHEN `t0`.`f` > 0
    THEN `t0`.`d` * 2
    WHEN `t0`.`c` < 0
    THEN `t0`.`a` * 2
    ELSE CAST(NULL AS BIGINT)
  END AS `SearchedCase((Greater(f, 0), Less(c, 0)), (Multiply(d, 2), Multiply(a, 2)), Cast(None, int64))`
FROM `alltypes` AS `t0`