"""
Entry point for the Search Keyword Performance ETL.

Usage
-----
Local:
    python src/main.py --input data/data.sql --output output/

AWS Glue (set as --script-location, pass job args):
    --input  s3://my-landing-bucket/data/data.sql
    --output s3://my-processed-bucket/results/

AWS EMR (via spark-submit):
    spark-submit --py-files dist/src.zip src/main.py \
        --input s3://... --output s3://...
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession

from utils.logger import get_logger
from transform import SearchKeywordProcessor

logger = get_logger(__name__)

_JAVA_OPTIONS = " ".join([
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
])


def create_spark_session(app_name: str = "SearchKeywordPerformance") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled",           "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Java 17/21 compatibility flags
        .config("spark.driver.extraJavaOptions",   _JAVA_OPTIONS)
        .config("spark.executor.extraJavaOptions", _JAVA_OPTIONS)
    )

    env = os.getenv("ENVIRONMENT", "local").lower()
    if env == "local":
        builder = builder.master("local[*]")
        logger.info("Running in LOCAL mode.")
    else:
        logger.info("Running in %s mode.", env.upper())

    return builder.getOrCreate()


def parse_args() -> argparse.Namespace:
    """Define and parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Adobe Analytics — Search Keyword Performance ETL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py --input data/data.sql
  python src/main.py --input s3://bucket/landing/data.sql \\
                     --output s3://bucket/processed/
        """,
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path or S3 URI of the tab-separated hit-level input file.",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_DIR", "output/"),
        help="Output directory (local path or S3 URI). Default: output/",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Application log level. Default: INFO",
    )

    # parse_known_args ignores Glue-injected system arguments (--JOB_NAME,
    # --TempDir, --job-bookmark-option etc.) that argparse would otherwise
    # reject with exit code 2, causing a silent SystemExit: 2 failure.
    args, _ = parser.parse_known_args()
    return args


def main() -> int:
    args = parse_args()

    # Re-configure root logger with requested level
    import logging
    logging.getLogger().setLevel(args.log_level.upper())

    logger.info("Input  : %s", args.input)
    logger.info("Output : %s", args.output)

    spark: SparkSession | None = None
    try:
        spark = create_spark_session()
        logger.info("Spark version: %s", spark.version)

        processor = SearchKeywordProcessor(spark=spark, output_dir=args.output)
        processor.run(input_file=args.input)
        return 0

    except Exception as exc:
        logger.error("Job FAILED: %s", exc, exc_info=True)
        return 1

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped.")


if __name__ == "__main__":
    sys.exit(main())