
  
    

  create  table "finbest"."mart"."stg_transactions__dbt_tmp"
  
  
    as
  
  (
    select * from raw.masked_transactions
  );
  