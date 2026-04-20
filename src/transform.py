"""
Core business logic for the Search Keyword Performance ETL.

Attribution model
-----------------
First-touch within a session.  A "session" is defined as all hits sharing
the same IP address (proxy for a single visitor in this dataset).

The pipeline:
  1. Read & validate the TSV hit-level file.
  2. Parse search engine domain + keyword from the referrer column.
  3. Propagate the *first* search referrer in each session to every hit
     (Window function — no collect()).
  4. Filter to purchase events (event_list standalone contains "1") that
     also have a search attribution.
  5. Parse revenue from product_list (only valid on purchase events).
  6. Aggregate revenue by (search engine, keyword), sort descending.
  7. Write a tab-delimited output file.

Scalability notes (for 10 GB+ files)
--------------------------------------
- Reads are streamed; no in-driver collect() on raw data.
- Window functions avoid shuffles where possible (partition by IP).
- pandas_udf processes referrer parsing in vectorized batches.
- The only collect() is on the *aggregated* result (guaranteed tiny — one row
  per search engine × keyword combination).
- For truly enormous files, consider:
    a) Partitioning the input by date on S3 and reading a single partition.
    b) Replacing IP-based sessions with a proper session ID if available.
    c) Using EMR with auto-scaling instead of a single Glue worker.
"""

import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
)

from utils.logger import get_logger
from utils.url_parser import extract_search_domain, extract_search_keyword

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Schema — explicit schema prevents Spark from inferring wrong types and
# avoids a full data scan just for schema inference.
# ---------------------------------------------------------------------------

HIT_DATA_SCHEMA = StructType([
    StructField("hit_time_gmt",  LongType(),   nullable=True),
    StructField("date_time",     StringType(), nullable=True),
    StructField("user_agent",    StringType(), nullable=True),
    StructField("ip",            StringType(), nullable=True),
    StructField("event_list",    StringType(), nullable=True),
    StructField("geo_city",      StringType(), nullable=True),
    StructField("geo_region",    StringType(), nullable=True),
    StructField("geo_country",   StringType(), nullable=True),
    StructField("pagename",      StringType(), nullable=True),
    StructField("page_url",      StringType(), nullable=True),
    StructField("product_list",  StringType(), nullable=True),
    StructField("referrer",      StringType(), nullable=True),
])

# Regex that matches "1" as a standalone event in a comma-separated list.
# Prevents false matches on events 10, 11, 12, 13, 14, etc.
PURCHASE_EVENT_REGEX = r"(^|,)\s*1\s*(,|$)"


class SearchKeywordProcessor:
    """
    Orchestrates the Search Keyword Performance ETL pipeline.

    Usage::

        processor = SearchKeywordProcessor(spark=spark, output_dir="s3://bucket/out/")
        processor.run(input_file="s3://bucket/in/data.tsv")
    """

    def __init__(self, spark: SparkSession, output_dir: str) -> None:
        self.spark = spark
        self.output_dir = output_dir
        logger.info("SearchKeywordProcessor initialised | output_dir=%s", output_dir)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, input_file: str) -> str:
        """
        Execute the full ETL pipeline end-to-end.

        Args:
            input_file: Path or S3 URI to the tab-separated hit-level file.

        Returns:
            Path / URI of the written output file.
        """
        logger.info("=== Pipeline START | input=%s ===", input_file)

        df = self._read_input(input_file)
        df = self._parse_search_attributes(df)
        df = self._attribute_session_search(df)
        df = self._filter_purchase_events(df)
        df = self._parse_revenue(df)
        result = self._aggregate_results(df)
        output_path = self._write_output(result)

        logger.info("=== Pipeline COMPLETE | output=%s ===", output_path)
        return output_path

    # ------------------------------------------------------------------
    # Private pipeline steps
    # ------------------------------------------------------------------

    def _read_input(self, file_path: str) -> DataFrame:
        """Read and schema-validate the TSV input file."""
        logger.info("Reading input: %s", file_path)

        df = (
            self.spark.read
            # Tab-separated; some fields are quoted (e.g. user_agent)
            .option("sep",       "\t")
            .option("header",    "true")
            .option("quote",     '"')
            .option("escape",    '"')
            .option("nullValue", "")
            # PERMISSIVE: log corrupt rows rather than failing the job
            .option("mode",      "PERMISSIVE")
            .schema(HIT_DATA_SCHEMA)
            .csv(file_path)
        )

        self._validate_schema(df)
        logger.info("Schema validated. Partition count: %d", df.rdd.getNumPartitions())
        return df

    def _validate_schema(self, df: DataFrame) -> None:
        """Raise early if any required column is absent."""
        required = {"hit_time_gmt", "ip", "event_list", "product_list", "referrer"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        logger.info("Schema check passed — all required columns present.")

    def _parse_search_attributes(self, df: DataFrame) -> DataFrame:
        """
        Add 'search_domain' and 'search_keyword' columns derived from referrer.

        Rows where the referrer is not a known search engine will have NULL
        in both new columns.
        """
        logger.info("Parsing search attributes from referrer URLs.")
        return (
            df
            .withColumn("search_domain",  extract_search_domain(F.col("referrer")))
            .withColumn("search_keyword", extract_search_keyword(F.col("referrer")))
        )

    def _attribute_session_search(self, df: DataFrame) -> DataFrame:
        """
        Propagate the FIRST external search referrer in each session to all hits.

        Uses an unbounded Window partitioned by IP so that downstream steps
        can join on the attributed engine/keyword without any shuffle explosion.

        Example
        -------
        Session for IP 67.98.123.1:
          hit 1: referrer=google.com?q=Ipod  → search_domain='google.com'
          hit 2: referrer=<internal>          → search_domain=NULL
          hit 9: purchase event               → search_domain=NULL
          ─── after attribution ───────────────────────────────────────
          hit 9: session_search_domain='google.com', keyword='Ipod'
        """
        logger.info("Applying first-touch session search attribution via Window.")

        # Unbounded window over the whole session so first() picks up the
        # earliest non-null value regardless of the current row's position.
        full_session_window = (
            Window
            .partitionBy("ip")
            .orderBy("hit_time_gmt")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        return (
            df
            .withColumn(
                "session_search_domain",
                F.first("search_domain",  ignorenulls=True).over(full_session_window),
            )
            .withColumn(
                "session_search_keyword",
                F.first("search_keyword", ignorenulls=True).over(full_session_window),
            )
        )

    def _filter_purchase_events(self, df: DataFrame) -> DataFrame:
        """
        Keep only rows that are:
          (a) A purchase event — event_list contains standalone "1", AND
          (b) Attributed to an external search engine in this session.

        IMPORTANT: The regex is necessary. A naive .contains("1") would also
        match events 10, 11, 12, 13, 14 — all of which appear in this dataset.
        """
        logger.info("Filtering for purchase events with search attribution.")

        is_purchase  = F.col("event_list").rlike(PURCHASE_EVENT_REGEX)
        has_search   = F.col("session_search_domain").isNotNull()
        has_products = F.col("product_list").isNotNull() & (F.col("product_list") != "")

        filtered = df.filter(is_purchase & has_search & has_products)
        logger.info(
            "Purchase rows with search attribution: %d",
            filtered.count(),  # acceptable here — result is always tiny
        )
        return filtered

    def _parse_revenue(self, df: DataFrame) -> DataFrame:
        """
        Extract revenue from product_list and sum it per hit.

        product_list format (per Appendix B):
          Category;Product Name;Qty;Revenue;CustomEvent,...

        Revenue is the 4th field (index 3) of each semicolon-delimited product.
        Multiple products are comma-separated; their revenues are summed.

        Revenue is ONLY valid when the purchase event is set — already guaranteed
        by the upstream filter.
        """
        logger.info("Parsing revenue from product_list.")

        return (
            df
            # Explode comma-separated products into individual rows
            .withColumn("product_entry",   F.explode(F.split("product_list", ",")))
            .withColumn("product_parts",   F.split("product_entry", ";"))
            # Revenue is index 3; guard against malformed / short entries
            .withColumn(
                "product_revenue",
                F.when(
                    F.size("product_parts") > 3,
                    F.trim(F.col("product_parts").getItem(3)).cast(DoubleType()),
                ).otherwise(F.lit(0.0)),
            )
            # Re-aggregate to hit level (handles multi-product rows)
            .groupBy(
                "ip",
                "hit_time_gmt",
                "session_search_domain",
                "session_search_keyword",
            )
            .agg(F.sum("product_revenue").alias("hit_revenue"))
        )

    def _aggregate_results(self, df: DataFrame) -> DataFrame:
        """
        Aggregate total revenue per (search engine, keyword) pair.
        Result is sorted by Revenue descending.
        """
        logger.info("Aggregating results by search engine and keyword.")

        return (
            df
            .groupBy(
                F.col("session_search_domain").alias("Search Engine Domain"),
                F.col("session_search_keyword").alias("Search Keyword"),
            )
            .agg(F.round(F.sum("hit_revenue"), 2).alias("Revenue"))
            .orderBy(F.desc("Revenue"))
        )

    def _write_output(self, df: DataFrame) -> str:
        """
        Write the aggregated result to a tab-delimited file.

        Naming convention: YYYY-mm-dd_SearchKeywordPerformance.tab

        For LOCAL paths  → written as a single file via pandas (aggregated
                           result is always tiny — safe to collect).
        For S3 paths     → written via Spark's native S3 writer (coalesce 1)
                           so no local disk is required on the driver.

        Returns:
            The full path / URI of the written file.
        """
        date_str  = datetime.now().strftime("%Y-%m-%d")
        filename  = f"{date_str}_SearchKeywordPerformance.tab"

        if self.output_dir.startswith("s3://") or self.output_dir.startswith("s3a://"):
            # S3 path — use Spark writer (no local temp file needed)
            full_path = f"{self.output_dir.rstrip('/')}/{filename}"
            logger.info("Writing to S3: %s", full_path)
            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .option("sep",    "\t")
                .option("header", "true")
                .csv(full_path)
            )
        else:
            # Local path — pandas gives us a proper single named file
            os.makedirs(self.output_dir, exist_ok=True)
            full_path = os.path.join(self.output_dir, filename)
            logger.info("Writing locally: %s", full_path)
            # toPandas() is safe here because the aggregated df is tiny
            df.toPandas().to_csv(full_path, sep="\t", index=False)

        logger.info("Output written: %s", full_path)
        return full_path