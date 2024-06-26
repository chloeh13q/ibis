SELECT
  `t0`.`g`,
  RANK() OVER (ORDER BY `t0`.`f` ASC) - 1 AS `minr`,
  DENSE_RANK() OVER (ORDER BY `t0`.`f` ASC) - 1 AS `denser`
FROM `alltypes` AS `t0`