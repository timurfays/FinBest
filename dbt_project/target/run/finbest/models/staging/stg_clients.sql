
  
    

  create  table "finbest"."mart"."stg_clients__dbt_tmp"
  
  
    as
  
  (
    select * from raw.masked_clients
  );
  