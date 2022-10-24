with prediction_step as (

SELECT
  batch_id,
  source_id,
  ts, 
  prediction_datetime,
  setpoint_velocity,
  actual_velocity,
  chain_position,
  case
    when setpoint_velocity is not null
    then CENTROID_ID
    else -1
  end as CENTROID_ID
FROM ML.PREDICT(MODEL industrial_ladle_demo.clustering_model, 
	(
	  SELECT 
		  setpoint_velocity as setpoint_velocity, 
		  actual_velocity as actual_velocity,
		  chain_position,
		  batch_id,
		  source_id,
		  ts,
		  CURRENT_DATETIME as prediction_datetime
	  FROM industrial_ladle_demo.ladle_hist 
    where processing_dttm >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 99960 MINUTE)
	    and batch_id not in ( 
				select batch_id
				from industrial_ladle_demo.ladle_prediction_hist
				where prediction_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 99960 MINUTE))
				)
	)
)

select
  batch_id,
  source_id,
  CURRENT_TIMESTAMP() as prediction_datetime,
  ts,
  CENTROID_ID,
  chain_position,
  case
      when abs(actual_velocity - setpoint_velocity_corrected) > velocity_std_threshold
      then 1 
      else 0 
    end as anomaly_flg
from(
  select
    batch_id,
	source_id,
    ts,
    actual_velocity,
    setpoint_velocity,
    chain_position,
    case
      when (IFNULL(setpoint_lag, - 99999) != setpoint_velocity or 
            IFNULL(setpoint_lead, - 99999) != setpoint_velocity or 
            (sign(actual_velocity - IFNULL(actual_lag_1, actual_velocity)) = 
             sign(actual_velocity - IFNULL(actual_lag_2, actual_velocity)) and
             sign(IFNULL(actual_lag_1, actual_velocity) - IFNULL(actual_lag_2, actual_velocity)) = 
             sign(IFNULL(actual_lag_2, actual_velocity) - IFNULL(actual_lag_3, actual_velocity)) and 
             IFNULL(setpoint_lag_10, - 99999) != setpoint_velocity
             )
      )
      then NULL 
      else setpoint_velocity 
    end as setpoint_velocity_corrected,
    t2.*

  from (
    select
      batch_id,
	  source_id,
      ts,
      setpoint_velocity,
      actual_velocity,
      chain_position,
      lag(setpoint_velocity, 6) over(partition by batch_id order by ts) as setpoint_lag,
      lag(setpoint_velocity, 10) over(partition by batch_id order by ts) as setpoint_lag_10,
      lead(setpoint_velocity, 6) over(partition by batch_id order by ts) as setpoint_lead,
      lag(actual_velocity, 1) over(partition by batch_id order by ts) as actual_lag_1,
      lag(actual_velocity, 2) over(partition by batch_id order by ts) as actual_lag_2,
      lag(actual_velocity, 3) over(partition by batch_id order by ts) as actual_lag_3,
      CENTROID_ID
    from prediction_step
  ) t1
  left join industrial_ladle_demo.ladle_clusters_statistics  t2 
    on t1.centroid_id = t2.centroid_id
)
order by batch_id, source_id, ts