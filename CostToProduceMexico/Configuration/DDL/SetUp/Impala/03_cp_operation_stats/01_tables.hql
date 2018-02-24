CREATE TABLE IF NOT EXISTS cp_operation_stats.last_processing_details(
  type_execution string, 
  type_process string, 
  hql_file string, 
  start_time string, 
  end_time string, 
  processing_time string, 
  status_exec string, 
  last_processing_date string);


CREATE TABLE IF NOT EXISTS cp_operation_stats.transformation_details_hist(
  type_execution string, 
  type_process string, 
  hql_file string, 
  start_time string, 
  end_time string, 
  processing_time string, 
  status_exec string, 
  processing_date string);
