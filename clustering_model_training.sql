CREATE OR REPLACE MODEL industrial_ladle_demo.clustering_model  
options(model_type='kmeans', num_clusters = 11, standardize_features = false) 
AS select setpoint_velocity 
from  industrial_ladle_demo.ladle_hist
where setpoint_velocity is not null;