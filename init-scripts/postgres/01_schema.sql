CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

-- staging
CREATE TABLE IF NOT EXISTS staging.transactions(
  transaction_id varchar PRIMARY KEY,
  client_id varchar,
  datetime timestamp,
  amount numeric,
  currency varchar(3),
  merchant varchar,
  transaction_type varchar,
  ip_address varchar);

CREATE TABLE IF NOT EXISTS staging.clients(
  client_id varchar PRIMARY KEY,
  first_name varchar,
  last_name varchar,
  email varchar,
  passport_id varchar,
  registration_date date,
  status varchar);

CREATE TABLE IF NOT EXISTS staging.currency_rates(
  date date,
  currency varchar(3),
  rate numeric,
  PRIMARY KEY (date,currency));

-- raw (маскированные)
CREATE TABLE IF NOT EXISTS raw.masked_transactions(
  transaction_id varchar PRIMARY KEY,
  client_id varchar,
  datetime timestamp,
  amount numeric,
  currency varchar(3),
  merchant varchar,
  transaction_type varchar,
  ip_network varchar,
  created_at timestamp DEFAULT now());

CREATE TABLE IF NOT EXISTS raw.masked_clients(
  client_id varchar PRIMARY KEY,
  first_name varchar,
  last_name varchar,
  email_hash varchar,
  passport_hash varchar,
  registration_date date,
  status varchar,
  created_at timestamp DEFAULT now());

-- mart
CREATE TABLE IF NOT EXISTS mart.client_balance(
  client_id varchar,
  date date,
  currency varchar(3),
  opening_balance numeric,
  closing_balance numeric,
  total_inflow numeric,
  total_outflow numeric,
  transaction_count int,
  updated_at timestamp DEFAULT now(),
  PRIMARY KEY (client_id,date,currency));

CREATE TABLE IF NOT EXISTS mart.suspicious_transactions(
  transaction_id varchar PRIMARY KEY,
  client_id varchar,
  datetime timestamp,
  amount numeric,
  currency varchar(3),
  merchant varchar,
  transaction_type varchar,
  rule_triggered varchar,
  risk_score numeric,
  detection_time timestamp DEFAULT now());
