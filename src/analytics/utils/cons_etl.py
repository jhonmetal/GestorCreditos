class ConsETL(object):
    """Constants for ETL process."""
    APP_NAME = 'ETL_Process'
    BRONZE_DB = 'fin_bronze'
    SILVER_DB = 'fin_silver'
    GOLD_DB = 'fin_gold'
    DDL_PATH = "scripts/schema_ddl.sql"
    TRANSACTIONS_PATH = "./landing/transactions.csv"
    ACCOUNTS_PATH = "./landing/accounts.csv"
    CUSTOMERS_PATH = "./landing/customers.csv"
    ACCOUNT_MASTER_TABLE = "account_master"
    CUSTOMERS_TABLE = "customers"
    ACCOUNTS_TABLE = "accounts"
    TRANSACTIONS_TABLE = "transactions"
    FACT_TABLE = "transactions_master"
    TOP_YEARLY_ACCOUNT_TABLE = "top_amount_yearly_accounts"
    TOP_MONTHLY_TABLE = "top_monthly_accounts"
    LARGE_TRANSACTIONS_TABLE = "large_transactions"
