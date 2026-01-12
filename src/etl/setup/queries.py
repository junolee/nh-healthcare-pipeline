daily_provider_metrics = """
WITH dates AS (
  SELECT
    date,
    day_of_week,
    day_of_week_name,
    is_weekday
  FROM nh_silver.dim_dates
),
claim AS (
  SELECT
    provider_id,
    observed_score AS readmission_rate
  FROM nh_silver.dim_claims
  WHERE measure_code = 521
), 
prov AS (
  SELECT
    provider_id,
    provider_name,
    provider_state AS state,
    ownership_type,
    num_certified_beds AS num_beds,
    substantiated_complaints AS num_complaints,
    rn_turnover,
    qm_rating
  FROM nh_silver.dim_providers
),
staff AS (
  SELECT
    work_date,
    provider_id,
    num_patients,
    hrs_registered_nurses AS rn_hrs
  FROM nh_silver.fct_staffing_levels
)
SELECT
  s.work_date AS date,
  s.provider_id,
  p.provider_name,
  
  s.num_patients,
  s.rn_hrs,
  (s.rn_hrs * 1.0) / NULLIF(s.num_patients, 0)  AS hrs_per_res,
  (s.num_patients * 1.0) / NULLIF(p.num_beds, 0)     AS occupancy_rate,
  p.num_beds,
  
  c.readmission_rate,
  p.num_complaints,
  p.rn_turnover,
  p.qm_rating,
  
  p.state,
  p.ownership_type,
  
  d.day_of_week,
  d.day_of_week_name,
  d.is_weekday
FROM staff s
JOIN prov p ON s.provider_id = p.provider_id
JOIN claim c ON s.provider_id = c.provider_id
JOIN dates d ON s.work_date = d.date;
"""

agg_daily_metrics = """
WITH daily AS (
  SELECT
    date,
    day_of_week,
    day_of_week_name,
    is_weekday,
    SUM(num_patients)                             AS num_patients,
    AVG(occupancy_rate)                           AS occupancy_rate,
    SUM(rn_hrs)                                   AS rn_hours,
    (SUM(rn_hrs) / NULLIF(SUM(num_patients), 0))  AS hrs_per_res
  FROM daily_provider_metrics
  GROUP BY 1,2,3,4
)
SELECT
  *,
  AVG(occupancy_rate) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS occ_7d,
  AVG(hrs_per_res) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS hpr_7d
FROM daily
ORDER BY date;
"""


agg_provider_metrics = """
SELECT
  provider_id,
  provider_name,
  state,
  ownership_type,

  AVG(num_patients)           AS avg_num_patients,
  SUM(rn_hrs)                 AS total_rn_hours,
  AVG(hrs_per_res)            AS avg_hrs_per_res,
  AVG(occupancy_rate)         AS avg_occupancy_rate,

  MAX(num_beds)               AS num_beds,
  MAX(readmission_rate)       AS readmission_rate,
  MAX(num_complaints)         AS num_complaints,
  MAX(rn_turnover)            AS rn_turnover,
  MAX(qm_rating)              AS qm_rating
FROM daily_provider_metrics
GROUP BY 1, 2, 3, 4;
"""



agg_state_metrics = """
SELECT
  state,
  AVG(num_patients)           AS avg_num_patients,
  SUM(rn_hrs)                 AS total_rn_hours,
  SUM(num_beds)               AS total_beds,
  AVG(hrs_per_res)            AS avg_hrs_per_res,
  AVG(occupancy_rate)         AS avg_occupancy_rate,
  AVG(qm_rating)              AS qm_rating,
  AVG(num_complaints)         AS num_complaints,
  AVG(rn_turnover)            AS rn_turnover
FROM daily_provider_metrics
GROUP BY 1;
"""