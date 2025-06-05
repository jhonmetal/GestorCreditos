from dataclasses import dataclass, field
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, sum, rank, dense_rank, row_number, when, lit, desc, year, date_format, avg, \
    length, first, round, current_date, expr
from analytics.utils.cons_etl import ConsETL
from analytics.utils.logger_wrapper import LoggerWrapper
from analytics.utils.util import Utils


@dataclass
class Transform(Utils):
    """
    A class to handle transformation and analysis of Spark DataFrames.

    Attributes:
        spark (SparkSession): The active Spark session.
        logger (LoggerWrapper): An instance of LoggerWrapper for logging messages.
        transform_dfs (Dict[str, DataFrame]): A dictionary to store transformed DataFrames.
    """
    spark: SparkSession
    logger: 'LoggerWrapper' = field(init=False, default_factory=lambda: LoggerWrapper())
    transform_dfs: Dict[str, DataFrame] = field(default_factory=dict)

    def __post_init__(self):
        self.t_transactions_df = self.spark.table(f"{ConsETL.BRONZE_DB}.{ConsETL.TRANSACTIONS_TABLE}")
        self.t_accounts_df = self.spark.table(f"{ConsETL.BRONZE_DB}.{ConsETL.ACCOUNTS_TABLE}")
        self.t_customers_df = self.spark.table(f"{ConsETL.BRONZE_DB}.{ConsETL.CUSTOMERS_TABLE}")

    def transform_data(self) -> List[Dict[str, DataFrame]]:
        """
        Creates a fact table by joining necessary tables and stores it in the transform_dfs attribute.

        Returns:
            List[Dict[str, DataFrame]]: A list of dictionaries containing fact table names as keys
                                        and the corresponding DataFrames as values.
        """
        self.logger.info("Transforming data and creating fact table")

        # Silver Table 1: Fact Table
        columns = ["t.transaction_id", "t.account_id", "c.customer_id", "c.name", "t.transaction_date", "t.transaction_amount",
                   "t.transaction_type", "a.account_type", "a.balance", "c.registration_date", "t.cutoff_date"]

        fact_df = ((self.t_transactions_df.alias("t")
                    .join(self.t_accounts_df.alias("a"), col("t.account_id") == col("a.account_id"), "inner")
                    .join(self.t_customers_df.alias("c"), col("a.customer_id") == col("c.customer_id"), "inner"))
                   .select(columns))

        self.transform_dfs[ConsETL.FACT_TABLE] = fact_df


        # Silver Table 2: Completed Transactions Table

        self.logger.info("Null Value Counts:")
        self.count_nulls(self.t_transactions_df).show()

        self.logger.info("Null and Blank Value Counts:")
        self.count_nulls_and_blanks(self.t_transactions_df).show()

        self.logger.info("Null and Blank Value Percentages:")
        self.percentage_nulls_and_blanks(self.t_transactions_df).show()

        avg_amount_overall = self.t_transactions_df.agg(avg("transaction_amount").alias("avg_transaction_amount"))
        overall_avg = avg_amount_overall.first()["avg_transaction_amount"]

        large_trx_df = self.t_transactions_df \
            .select(
            *[column for column in self.t_transactions_df.columns if column != "transaction_amount"],
            when(col("transaction_amount") > 10000, lit("Verdadera"))
            .when(col("transaction_amount") <= 10000, lit("Falso"))
            .otherwise(lit("Ninguno")).alias("is_large_transaction"),
            when(col("transaction_amount").isNull() | (length(col("transaction_amount")) == 0),
                 lit(overall_avg))
            .otherwise(col("transaction_amount")).alias("transaction_amount")
        )

        large_trx_df.show(truncate=False)
        self.transform_dfs[ConsETL.LARGE_TRANSACTIONS_TABLE] = large_trx_df


        # Convert dictionary items to a list of key-value dictionaries
        list_df = [{key: value} for key, value in self.transform_dfs.items()]

        return list_df

    def analysis_data(self) -> List[Dict[str, DataFrame]]:
        """
        Analyzes transformed data for end applications and reporting.

        Returns:
            List[Dict[str, DataFrame]]: A list of dictionaries containing transformed DataFrame names as keys
                                        and the corresponding DataFrames as values.
        """
        self.transform_dfs = {}
        self.logger.info("Transforming")

        fact_table_df = self.spark.table(f"{ConsETL.SILVER_DB}.{ConsETL.FACT_TABLE}")

        # Gold Table 1: Transaction fees for last three years
        last_three_years = year(current_date()) - 3
        trx_fee_df = self.t_transactions_df.filter(year(col("cutoff_date")) >= last_three_years) \
            .select("transaction_id",
                    "account_id",
                    "transaction_date",
                    "transaction_amount",
                    "transaction_type",
                    when(col("transaction_type") == "purchase", col("transaction_amount") * 0.1).otherwise(
                        lit(0)).alias("transaction_fee"))
        trx_fee_df.limit(50).show(truncate=False)

        window_spec_account = Window.partitionBy("account_id")
        window_spec_type = Window.partitionBy("account_id", "transaction_type")
        # Step 1: Calculate total transaction amount per account
        df_with_total_amount = trx_fee_df.select(
            "account_id",
            "transaction_type",
            "transaction_amount",
            sum("transaction_amount").over(window_spec_account).alias("total_transaction_amount"),
            sum("transaction_amount").over(window_spec_type).alias("transaction_type_total")
        )

        # Step 2: Calculate rank and top transaction type using the total amount
        window_spec_rank = Window.orderBy(desc("total_transaction_amount"))
        window_spec_top_type = Window.partitionBy("account_id").orderBy(desc("transaction_type_total"))

        top_amount_trx_df = df_with_total_amount.select(
            *df_with_total_amount.columns,
            dense_rank().over(window_spec_rank).alias("rank"),
            row_number().over(window_spec_top_type).alias("type_rank")
        ).filter(col("type_rank") == 1).select(
            "account_id",
            "total_transaction_amount",
            "rank",
            "transaction_type"
        ).dropDuplicates(["account_id"]).sort("rank", ascending=True)
        top_amount_trx_df.show(truncate=False)

        self.transform_dfs[ConsETL.TOP_YEARLY_ACCOUNT_TABLE] = top_amount_trx_df

        # Gold Table 2: Monthly transaction analysis
        last_twelve_months = date_format(current_date() - expr("INTERVAL 1 YEAR"), "yyyy-MM")

        window_spec_total_trx = Window.orderBy(desc("total_transaction_amount"))

        top_monthly_df = (
            fact_table_df.filter(date_format(col("cutoff_date"), "yyyy-MM") >= last_twelve_months)
            .groupBy("customer_id", "account_type")
            .agg(sum("transaction_amount").alias("total_transaction_amount"),
                 avg("transaction_amount").alias("avg_transaction_amount")
                 )
            .select("customer_id",
                    "account_type",
                    "total_transaction_amount",
                    "avg_transaction_amount",
                    rank().over(window_spec_total_trx).alias("rank")
                    ))

        top_monthly_df.show(truncate=False)

        self.transform_dfs[ConsETL.TOP_MONTHLY_TABLE] = top_monthly_df

        # Gold Table 3:

        porc_use_balance_col = round((col("total_expenses") / (col("initial_balance") + col("total_deposits"))) * 100,
                                     2)

        account_master_df = (
            fact_table_df
            .filter(year(col("registration_date")) >= last_three_years)
            .groupBy("account_id", "customer_id", "cutoff_date")
            .agg(
                first("balance").alias("initial_balance"),
                sum(when(col("transaction_type") == "deposit", col("transaction_amount")).otherwise(0)).alias(
                    "total_deposits"),
                sum(when(col("transaction_type") != "deposit", col("transaction_amount")).otherwise(0)).alias(
                    "total_expenses")
            )
            .select(
                "account_id",
                "customer_id",
                porc_use_balance_col.alias("porc_use_balance"),
                when(porc_use_balance_col <= 50, "0-50%")
                .when(porc_use_balance_col <= 70, "51-70%")
                .otherwise("71-100%").alias("range_use_balance"),
                "cutoff_date"
            )
        )
        account_master_df.show(truncate=False)
        self.transform_dfs[ConsETL.ACCOUNT_MASTER_TABLE] = account_master_df

        # Convert dictionary items to a list of key-value dictionaries
        list_df = [{key: value} for key, value in self.transform_dfs.items()]

        return list_df
