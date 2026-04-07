# Databricks notebook source
# MAGIC %md
# MAGIC # BronzeWork Incremental
# MAGIC
# MAGIC Incremental Bronze ingestion with rerun-safe watermark logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Imports and setup
# MAGIC This cell imports the PySpark and Delta helpers used in the notebook, switches to the correct catalog, and makes sure the **Bronze schema** exists before we start loading data.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid

# COMMAND ----------

spark.sql("use catalog datatocrunch_novacart_adb")

# COMMAND ----------

spark.sql("create schema if not exists bronze_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Bronze control table
# MAGIC This table stores the **watermark** for each source table.
# MAGIC
# MAGIC It helps the pipeline remember:
# MAGIC - the latest timestamp already processed
# MAGIC - the latest primary key processed at that timestamp
# MAGIC - how many rows were written in the latest run
# MAGIC
# MAGIC This is what makes the Bronze load incremental and rerun-safe.

# COMMAND ----------

spark.sql("""
          create table if not exists datatocrunch_novacart_adb.bronze_schema.ingestion_control(
              layer string,
              table_name string,
              ts_col string,
              pk_col string,
              last_successful_ts  timestamp,
              last_successful_pk bigint,
              last_run_id string,
              rows_written bigint,
              run_status string,
              updated_at timestamp
          )
          using delta
          """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Source table configuration
# MAGIC This cell defines which source tables will be loaded into Bronze and which columns should be used as:
# MAGIC - **primary key**
# MAGIC - **timestamp / watermark column**
# MAGIC
# MAGIC It also creates a unique `bronze_run_id` for the current pipeline run.

# COMMAND ----------

tables_config = {
    "orders"   : {"pk_col" : "order_id" , "ts_col" : "updated_at"},
    "products" : { "pk_col" : "product_id", "ts_col" : "updated_at"},
    "payments" : { "pk_col" : "payment_id", "ts_col" : "processed_at"}
 }

bronze_run_id = str(uuid.uuid4())
print("Current Bronze Run ID:",bronze_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Helper functions
# MAGIC This cell contains reusable functions:
# MAGIC
# MAGIC - `get_last_successful_watermark()` reads the last processed watermark from the control table
# MAGIC - `upsert_bronze_control()` updates the control table after a successful Bronze load
# MAGIC
# MAGIC These functions keep the main load logic cleaner and easier to understand.

# COMMAND ----------

def get_last_successful_watermark(table_name:str):
    ctrl = (
        spark.table("datatocrunch_novacart_adb.bronze_schema.ingestion_control")
        .filter(
            (F.col("layer")=="bronze") &
            (F.col("table_name")==table_name) &
            (F.col("run_status")=="success")
        )
        .orderBy(F.col("updated_at").desc())
        .limit(1)
    )
    rows = ctrl.collect()
    if not rows:
        return None, None

    return rows[0]["last_successful_ts"], rows[0]["last_successful_pk"]

# COMMAND ----------

def upsert_bronze_control(table_name,ts_col,pk_col,last_ts,last_pk,rows_written,run_id):
    control_df = spark.createDataFrame(
        [(
            "bronze",
            table_name,
            ts_col,
            pk_col,
            last_ts,
            int(last_pk) if last_pk is not None else None,
            run_id,
            int(rows_written),
            "success",
            datetime.now(timezone.utc)
        )],
            schema="""
            layer string,
            table_name string,
            ts_col string,
            pk_col string,
            last_successful_ts timestamp,
            last_successful_pk bigint,
            last_run_id string,
            rows_written bigint,
            run_status string,
            updated_at timestamp
            """
    )
    dt = DeltaTable.forName(spark,"datatocrunch_novacart_adb.bronze_schema.ingestion_control")
    (dt.alias("t")
        .merge(control_df.alias("s"),"t.table_name = s.table_name and t.layer = s.layer")
        .whenMatchedUpdate(set={
            "ts_col" : "s.ts_col",
            "pk_col" : "s.pk_col",
            "last_successful_ts" : "s.last_successful_ts",
            "last_successful_pk" : "s.last_successful_pk",
            "last_run_id" : "s.last_run_id",
            "rows_written" : "s.rows_written",
            "run_status" : "s.run_status",
            "updated_at" : "s.updated_at"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Bronze incremental load loop
# MAGIC This is the main Bronze logic.
# MAGIC
# MAGIC For each table, the notebook:
# MAGIC 1. reads the last watermark
# MAGIC 2. reads the source SQL table
# MAGIC 3. filters only **new / changed rows**
# MAGIC 4. adds Bronze audit columns
# MAGIC 5. appends the rows into the Bronze Delta table
# MAGIC 6. updates the control table
# MAGIC
# MAGIC This is the core incremental loading logic.

# COMMAND ----------

for table_name, cfg in tables_config.items():
    pk_col = cfg["pk_col"]
    ts_col = cfg["ts_col"]
    source_table = f"`datatocrunch_novacart_sql_connection_catalog`.dbo.{table_name}"
    target_table = f"datatocrunch_novacart_adb.bronze_schema.{table_name}_raw"

    last_successful_ts, last_successful_pk = get_last_successful_watermark(table_name)

    ######################## FIX — Truncate watermark to millisecond precision #####
    # SQL Server datetime2(7) has 100-ns precision; Python/Delta has microsecond
    # precision. When the filter is pushed down to SQL Server, the sub-microsecond
    # residual causes ">" to return True for timestamps that are functionally equal.
    # Truncating both sides to milliseconds eliminates the mismatch.
    if last_successful_ts is not None:
        last_successful_ts = last_successful_ts.replace(
            microsecond=(last_successful_ts.microsecond // 1000) * 1000
        )
    ######################## FIX — END ############################################

    print(f"\n=== Processing {table_name} ===")
    print("last_successful_ts =", last_successful_ts)
    print("last_successful_pk =", last_successful_pk)

    ######################## FIX — Truncate source ts to millisecond precision #####
    source_df = spark.read.table(source_table).withColumn(
        ts_col, F.date_trunc("MILLISECOND", F.col(ts_col).cast("timestamp"))
    )
    ######################## FIX — END ############################################

    if last_successful_ts is None:
        rows_to_load = source_df
    else:
        ######################## ADDED SECTION - START #########################
        if last_successful_pk is None:
            rows_to_load = source_df.filter(
            F.col(ts_col) > F.lit(last_successful_ts)
        )
        else:
        ######################## ADDED SECTION - END #########################
            rows_to_load = source_df.filter(
                (F.col(ts_col) > F.lit(last_successful_ts)) |
                (
                    (F.col(ts_col) == F.lit(last_successful_ts)) &
                    (F.col(pk_col).cast("long") > F.lit(int(last_successful_pk)))
                )
        )

    rows_to_load = (
        rows_to_load
        .withColumn("bronze_ingested_at", F.current_timestamp())
        .withColumn("bronze_run_id", F.lit(bronze_run_id))
        .withColumn("bronze_source_table", F.lit(source_table))
    )

    row_count = rows_to_load.count()

    print(f"{table_name} rows_to_load = {row_count}")

    if row_count == 0:
        print(f"No new rows for {table_name}.")
        upsert_bronze_control(
        table_name,
        ts_col,
        pk_col,
        last_successful_ts,
        last_successful_pk,
        row_count,      
        bronze_run_id
    )
        continue

    rows_to_load.write.format("delta").mode("append").saveAsTable(target_table)
    
    ######################## ADDED SECTION - START #########################
    # max_ts = rows_to_load.agg(F.max(ts_col).alias("max_ts")).collect()[0]["max_ts"]

    # max_pk = (
    #     rows_to_load
    #     .filter(F.col(ts_col) == F.lit(max_ts))
    #     .agg(F.max(F.col(pk_col).cast("long")).alias("max_pk"))
    #     .collect()[0]["max_pk"]
    # )

    watermark_row = (
        rows_to_load
        .select(ts_col, pk_col)
        .orderBy(
            F.col(ts_col).desc(),
            F.col(pk_col).cast("long").desc()
        )
        .limit(1)
        .collect()[0]
    )

    ######################## ADDED SECTION - END #########################
    
    max_ts = watermark_row[ts_col]
    max_pk = int(watermark_row[pk_col]) if watermark_row[pk_col] is not None else None

   
    upsert_bronze_control(table_name, ts_col, pk_col, max_ts, max_pk, row_count, bronze_run_id)
    print(f"Wrote {row_count} rows to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Quick validation
# MAGIC This final cell prints the Bronze row counts and displays the control table so you can verify that the incremental logic is working correctly.

# COMMAND ----------

print("Orders Bronze Count :", spark.sql("select count(*) from datatocrunch_novacart_adb.bronze_schema.orders_raw").collect()[0][0])
print("Products Bronze Count :", spark.sql("select count(*) from datatocrunch_novacart_adb.bronze_schema.products_raw").collect()[0][0])
print("Payments Bronze Count :", spark.sql("select count(*) from datatocrunch_novacart_adb.bronze_schema.payments_raw").collect()[0][0])

display(spark.sql("select * from datatocrunch_novacart_adb.bronze_schema.ingestion_control").orderBy("table_name"))