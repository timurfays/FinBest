
  
    

  create  table "finbest"."mart"."stg_rates__dbt_tmp"
  
  
    as
  
  (
    select * from staging.currency_rates
  );
  