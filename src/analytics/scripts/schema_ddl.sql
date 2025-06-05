DROP SCHEMA IF EXISTS {ConsETL.BRONZE_DB} CASCADE;
DROP SCHEMA IF EXISTS {ConsETL.SILVER_DB} CASCADE;
DROP SCHEMA IF EXISTS {ConsETL.GOLD_DB} CASCADE;

CREATE SCHEMA IF NOT EXISTS {ConsETL.BRONZE_DB}
COMMENT 'Esquema donde se almacenarán la tablas de la capa bronze';

CREATE SCHEMA IF NOT EXISTS {ConsETL.SILVER_DB}
COMMENT 'Esquema donde se almacenarán la tablas de la capa silver';

CREATE SCHEMA IF NOT EXISTS {ConsETL.GOLD_DB}
COMMENT 'Esquema donde se almacenarán la tablas de la capa gold';

CREATE TABLE IF NOT EXISTS {ConsETL.BRONZE_DB}.{ConsETL.TRANSACTIONS_TABLE}
(
  transaction_id VARCHAR(9) COMMENT 'Identificador de transacción',
  account_id VARCHAR(9) COMMENT 'Identificador de cuenta',
  transaction_type VARCHAR(50) COMMENT 'Tipo de transacción',
  operation VARCHAR(50) COMMENT 'Descripción de operación',
  transaction_amount DECIMAL(19, 8) COMMENT 'Importe de transacción',
  balance DECIMAL(19, 8) COMMENT 'Saldo de cuenta',
  bank VARCHAR(256) COMMENT 'Nombre del banco',
  account INT COMMENT 'Identificador de de cuenta',
  transaction_date TIMESTAMP COMMENT 'Fecha de transacciones bancarias',
  cutoff_date DATE COMMENT 'Fecha de corte de la información'
)
USING PARQUET
PARTITIONED BY (cutoff_date)
COMMENT 'Log de transacciones bancarias';

CREATE TABLE IF NOT EXISTS {ConsETL.BRONZE_DB}.{ConsETL.ACCOUNTS_TABLE}
(
  account_id VARCHAR(9) COMMENT 'Identificador de cuenta',
  customer_id INT COMMENT 'Identificador de distrito',
  account_type VARCHAR(100) COMMENT 'Tipo de cuenta',
  account_date DATE COMMENT 'Fecha de apertura de cuenta',
  cutoff_date DATE COMMENT 'Fecha de corte de la información',
  balance DECIMAL(19, 8) COMMENT 'Fecha de corte de la información'
)
USING PARQUET
PARTITIONED BY (cutoff_date)
COMMENT 'Maestro de cuentas';

CREATE TABLE IF NOT EXISTS {ConsETL.BRONZE_DB}.{ConsETL.CUSTOMERS_TABLE}
(
  customer_id INT COMMENT 'Identificador de cliente',
  name VARCHAR(100) COMMENT 'Nombre de cliente',
  age INT COMMENT 'edad',
  city VARCHAR(100) COMMENT 'Nombre de la ciudad',
  registration_date DATE COMMENT 'fecha de registro del cliente',
  status VARCHAR(1) COMMENT 'Estado civil'
)
USING PARQUET
COMMENT 'Maestro de clientes';

CREATE TABLE IF NOT EXISTS {ConsETL.SILVER_DB}.{ConsETL.FACT_TABLE}
(
  transaction_id VARCHAR(9) COMMENT 'Identificador de transacción',
  account_id VARCHAR(9) COMMENT 'Identificador de cuenta',
  customer_id INT COMMENT 'Identificador de cliente',
  name VARCHAR(100) COMMENT 'Nombre de cliente',
  transaction_date TIMESTAMP COMMENT 'Fecha de transacciones bancarias',
  transaction_amount DECIMAL(19, 8) COMMENT 'Importe de transacción',
  transaction_type VARCHAR(50) COMMENT 'Tipo de transacción',
  account_type VARCHAR(100) COMMENT 'Tipo de cuenta',
  balance DECIMAL(19, 8) COMMENT 'Fecha de corte de la información',
  registration_date DATE COMMENT 'fecha de registro del cliente',
  cutoff_date DATE COMMENT 'Fecha de corte de la información'
)
USING PARQUET
PARTITIONED BY (cutoff_date)
COMMENT 'Fact table de transacciones bancarias';

CREATE TABLE IF NOT EXISTS {ConsETL.SILVER_DB}.{ConsETL.LARGE_TRANSACTIONS_TABLE}
(
  transaction_id VARCHAR(9) COMMENT 'Identificador de transacción',
  account_id VARCHAR(9) COMMENT 'Identificador de cuenta',
  transaction_type VARCHAR(50) COMMENT 'Tipo de transacción',
  operation VARCHAR(50) COMMENT 'Descripción de operación',
  balance DECIMAL(19, 8) COMMENT 'Saldo de cuenta',
  bank VARCHAR(256) COMMENT 'Nombre del banco',
  account INT COMMENT 'Identificador de de cuenta',
  transaction_date TIMESTAMP COMMENT 'Fecha de transacciones bancarias',
  cutoff_date DATE COMMENT 'Fecha de corte de la información',
  is_large_transaction VARCHAR(9) COMMENT 'Flag para transacciones grandes',
  transaction_amount DECIMAL(19, 8) COMMENT 'Importe de transacción completado con el promedio si nulo'
)
USING PARQUET
PARTITIONED BY (cutoff_date)
COMMENT 'Transacciones bancarias grandes y con datos completados';

CREATE TABLE IF NOT EXISTS {ConsETL.GOLD_DB}.{ConsETL.ACCOUNT_MASTER_TABLE}
(
  account_id VARCHAR(9) COMMENT 'Identificador de cuenta',
  customer_id INT COMMENT 'Identificador de cliente',
  porc_use_balance VARCHAR(100) COMMENT 'Porcentaje de utilización de saldo',
  range_use_balance VARCHAR(100) COMMENT 'Rango de porcentaje de utilización de saldo',
  cutoff_date DATE COMMENT 'Fecha de corte de la información'
)
USING PARQUET
PARTITIONED BY (cutoff_date)
COMMENT 'Maestro de cuentas filtradas';

CREATE TABLE IF NOT EXISTS {ConsETL.GOLD_DB}.{ConsETL.TOP_YEARLY_ACCOUNT_TABLE}
(
  account_id VARCHAR(9) COMMENT 'Identificador de cuenta',
  total_transaction_amount DECIMAL(19, 8) COMMENT 'Importe Acumulado por tipo de transacción',
  rank INT COMMENT 'Ranking',
  transaction_type VARCHAR(50) COMMENT 'Tipo de transacción'
)
USING PARQUET
COMMENT 'Reporte Anual de clientes top';

CREATE TABLE IF NOT EXISTS {ConsETL.GOLD_DB}.{ConsETL.TOP_MONTHLY_TABLE}
(
  customer_id INT COMMENT 'Identificador de cliente',
  account_type VARCHAR(100) COMMENT 'Tipo de cuenta',
  total_transaction_amount DECIMAL(19, 8) COMMENT 'Importe acumulado de transacciones',
  avg_transaction_amount DECIMAL(19, 8) COMMENT 'Promedio acumulado de transacciones',
  rank INT COMMENT 'Ranking'
)
USING PARQUET
COMMENT 'Reporte Mensual de transacciones top';



CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.TRANSACTIONS_MASKED AS
SELECT
    transaction_id,
    CONCAT(SUBSTR(account_id, 1, 3), '***') AS account_id,
    transaction_type,
    operation,
    transaction_amount,
    balance,
    bank,
    account,
    transaction_date,
    cutoff_date
FROM {ConsETL.BRONZE_DB}.{ConsETL.TRANSACTIONS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.TRANSACTIONS_DATE_GENERALIZED AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    transaction_amount,
    balance,
    bank,
    account,
    DATE_TRUNC('month', transaction_date) AS transaction_month,
    cutoff_date
FROM {ConsETL.BRONZE_DB}.{ConsETL.TRANSACTIONS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.TRANSACTIONS_AGGREGATED AS
SELECT
    account_id,
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_amount) AS total_transaction_amount,
    AVG(transaction_amount) AS avg_transaction_amount
FROM {ConsETL.BRONZE_DB}.{ConsETL.TRANSACTIONS_TABLE}
GROUP BY account_id;



CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.ACCOUNTS_MASKED AS
SELECT
    CONCAT(SUBSTR(account_id, 1, 4), '***') AS account_id,
    customer_id,
    account_type,
    account_date,
    cutoff_date,
    balance
FROM {ConsETL.BRONZE_DB}.{ConsETL.ACCOUNTS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.ACCOUNTS_TYPE_GENERALIZED AS
SELECT
    account_id,
    customer_id,
    CASE
        WHEN balance < 10000 THEN 'Low Balance'
        WHEN balance BETWEEN 10000 AND 50000 THEN 'Medium Balance'
        ELSE 'High Balance'
    END AS balance_category,
    cutoff_date
FROM {ConsETL.BRONZE_DB}.{ConsETL.ACCOUNTS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.ACCOUNTS_AGGREGATED AS
SELECT
    account_type,
    COUNT(account_id) AS total_accounts,
    SUM(balance) AS total_balance,
    AVG(balance) AS avg_balance
FROM {ConsETL.BRONZE_DB}.{ConsETL.ACCOUNTS_TABLE}
GROUP BY account_type;



CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.CUSTOMERS_MASKED AS
SELECT
    customer_id,
    CONCAT(LEFT(name, 1), REPEAT('*', LENGTH(name) - 1)) AS name,
    age,
    city,
    registration_date,
    status
FROM {ConsETL.BRONZE_DB}.{ConsETL.CUSTOMERS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.CUSTOMERS_AGE_GENERALIZED AS
SELECT
    customer_id,
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 40 THEN '20-40'
        WHEN age BETWEEN 41 AND 60 THEN '41-60'
        ELSE 'Over 60'
    END AS age_group,
    city,
    registration_date,
    status
FROM {ConsETL.BRONZE_DB}.{ConsETL.CUSTOMERS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.BRONZE_DB}.CUSTOMERS_SUMMARY_BY_CITY AS
SELECT
    city,
    COUNT(customer_id) AS total_customers,
    AVG(age) AS avg_age
FROM {ConsETL.BRONZE_DB}.{ConsETL.CUSTOMERS_TABLE}
GROUP BY city;



CREATE OR REPLACE VIEW {ConsETL.SILVER_DB}.FACT_MASKED AS
SELECT
    transaction_id,
    CONCAT(SUBSTR(account_id, 1, 3), '***') AS account_id,
    CONCAT(LEFT(name, 1), REPEAT('*', LENGTH(name) - 1)) AS name,
    transaction_type,
    transaction_date,
    transaction_amount,
    account_type,
    balance,
    registration_date,
    cutoff_date
FROM {ConsETL.SILVER_DB}.{ConsETL.FACT_TABLE};

CREATE OR REPLACE VIEW {ConsETL.SILVER_DB}.FACT_AMOUNT_GENERALIZED AS
SELECT
    transaction_id,
    account_id,
    customer_id,
    transaction_type,
    transaction_date,
    CASE
        WHEN transaction_amount < 100 THEN 'Low'
        WHEN transaction_amount BETWEEN 100 AND 1000 THEN 'Medium'
        ELSE 'High'
    END AS transaction_amount_category,
    account_type,
    cutoff_date
FROM {ConsETL.SILVER_DB}.{ConsETL.FACT_TABLE};

CREATE OR REPLACE VIEW {ConsETL.SILVER_DB}.FACT_AGGREGATED AS
SELECT
    customer_id,
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_amount) AS total_transaction_amount,
    AVG(transaction_amount) AS avg_transaction_amount,
    MAX(transaction_date) AS last_transaction_date
FROM {ConsETL.SILVER_DB}.{ConsETL.FACT_TABLE}
GROUP BY customer_id;



CREATE OR REPLACE VIEW {ConsETL.SILVER_DB}.LARGE_TRANSACTIONS_FLAGGED AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    operation,
    bank,
    is_large_transaction,
    CASE
        WHEN transaction_amount IS NULL THEN 'Unknown'
        ELSE 'Known'
    END AS transaction_amount_status,
    transaction_amount,
    cutoff_date
FROM {ConsETL.SILVER_DB}.{ConsETL.LARGE_TRANSACTIONS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.SILVER_DB}.LARGE_TRANSACTIONS_BALANCE_GENERALIZED AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    CASE
        WHEN balance < 10000 THEN 'Low'
        WHEN balance BETWEEN 10000 AND 50000 THEN 'Medium'
        ELSE 'High'
    END AS balance_category,
    bank,
    is_large_transaction,
    transaction_date,
    cutoff_date
FROM {ConsETL.SILVER_DB}.{ConsETL.LARGE_TRANSACTIONS_TABLE};

CREATE OR REPLACE VIEW {ConsETL.SILVER_DB}.LARGE_TRANSACTIONS_AGGREGATED AS
SELECT
    account_id,
    COUNT(transaction_id) AS total_large_transactions,
    SUM(transaction_amount) AS total_large_transaction_amount,
    AVG(transaction_amount) AS avg_large_transaction_amount
FROM {ConsETL.SILVER_DB}.{ConsETL.LARGE_TRANSACTIONS_TABLE}
WHERE is_large_transaction = 'Verdadera'
GROUP BY account_id;



CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.ACCOUNT_MASTER_GENERALIZED AS
SELECT
    account_id,
    customer_id,
    porc_use_balance,
    CASE
        WHEN range_use_balance = '71-100%' THEN 'High Usage'
        WHEN range_use_balance = '41-70%' THEN 'Moderate Usage'
        ELSE 'Low Usage'
    END AS usage_category,
    cutoff_date
FROM {ConsETL.GOLD_DB}.{ConsETL.ACCOUNT_MASTER_TABLE};

CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.ACCOUNT_MASTER_AGGREGATED AS
SELECT
    range_use_balance,
    COUNT(account_id) AS total_accounts,
    AVG(CAST(SUBSTRING(porc_use_balance, 1, LENGTH(porc_use_balance) - 1) AS DECIMAL(10,2))) AS avg_usage_percentage
FROM {ConsETL.GOLD_DB}.{ConsETL.ACCOUNT_MASTER_TABLE}
GROUP BY range_use_balance;



CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.TOP_YEARLY_MASKED AS
SELECT
    CONCAT(SUBSTR(account_id, 1, 3), '***') AS account_id,
    total_transaction_amount,
    rank,
    transaction_type
FROM {ConsETL.GOLD_DB}.{ConsETL.TOP_YEARLY_ACCOUNT_TABLE};

CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.TOP_YEARLY_AGGREGATED AS
SELECT
    transaction_type,
    COUNT(account_id) AS total_top_accounts,
    SUM(total_transaction_amount) AS total_transaction_volume
FROM {ConsETL.GOLD_DB}.{ConsETL.TOP_YEARLY_ACCOUNT_TABLE}
GROUP BY transaction_type;



CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.TOP_MONTHLY_MASKED AS
SELECT
    customer_id,
    CONCAT(LEFT(account_type, 2), '**') AS account_type,
    total_transaction_amount,
    avg_transaction_amount,
    rank
FROM {ConsETL.GOLD_DB}.{ConsETL.TOP_MONTHLY_TABLE};

CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.TOP_MONTHLY_AVG_GENERALIZED AS
SELECT
    customer_id,
    account_type,
    total_transaction_amount,
    CASE
        WHEN avg_transaction_amount < 5000 THEN 'Low'
        WHEN avg_transaction_amount BETWEEN 5000 AND 10000 THEN 'Medium'
        ELSE 'High'
    END AS avg_transaction_amount_category,
    rank
FROM {ConsETL.GOLD_DB}.{ConsETL.TOP_MONTHLY_TABLE};

CREATE OR REPLACE VIEW {ConsETL.GOLD_DB}.TOP_MONTHLY_AGGREGATED AS
SELECT
    account_type,
    COUNT(customer_id) AS total_top_customers,
    SUM(total_transaction_amount) AS total_transaction_volume,
    AVG(avg_transaction_amount) AS avg_transaction_value
FROM {ConsETL.GOLD_DB}.{ConsETL.TOP_MONTHLY_TABLE}
GROUP BY account_type;
