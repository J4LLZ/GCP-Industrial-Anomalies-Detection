create table industrial_ladle_demo.ladle_clusters_statistics as 
select 
	CENTROID_ID,
	STDDEV(setpoint_velocity) as velocity_std,
	5 as num_std,
  5 * STDDEV(setpoint_velocity) as velocity_std_threshold
FROM ML.PREDICT(MODEL industrial_ladle_demo.clustering_model, 
	(
    select 
			setpoint_velocity 
    from industrial_ladle_demo.ladle_hist
				)
	)
group by CENTROID_ID
