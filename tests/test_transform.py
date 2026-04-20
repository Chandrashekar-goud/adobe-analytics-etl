"""
Integration-style tests for the SearchKeywordProcessor pipeline.

Uses an in-memory Spark DataFrame that mirrors the sample data to verify
correct revenue attribution and aggregation.
"""

import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
)

from transform import SearchKeywordProcessor, HIT_DATA_SCHEMA

# ---------------------------------------------------------------------------
# Inline test dataset (mirrors the provided data.sql sample)
# ---------------------------------------------------------------------------

SAMPLE_ROWS = [
    # --- Session: IP 67.98.123.1 (Salem, OR) → Google → Ipod Touch $290 ---
    (1254033280, "2009-09-27 06:34:40", "Mozilla/5.0", "67.98.123.1", "",   "Salem",        "OR", "US", "Home",                   "http://www.esshopzilla.com",                        "",                                    "http://www.google.com/search?hl=en&q=Ipod"),
    (1254033676, "2009-09-27 06:41:16", "Mozilla/5.0", "67.98.123.1", "",   "Salem",        "OR", "US", "Search Results",         "http://www.esshopzilla.com/search/?k=Ipod",         "",                                    "http://www.esshopzilla.com"),
    (1254033973, "2009-09-27 06:46:13", "Mozilla/5.0", "67.98.123.1", "2",  "Salem",        "OR", "US", "Ipod - Nano - 8 GB",    "http://www.esshopzilla.com/product/?pid=as32213",  "Electronics;Ipod - Nano - 8GB;1;;",   "http://www.esshopzilla.com/search/?k=Ipod"),
    (1254034270, "2009-09-27 06:51:10", "Mozilla/5.0", "67.98.123.1", "",   "Salem",        "OR", "US", "Search Results",         "http://www.esshopzilla.com/search/?k=Ipod",         "",                                    "http://www.esshopzilla.com/product/?pid=as32213"),
    (1254034567, "2009-09-27 06:56:07", "Mozilla/5.0", "67.98.123.1", "2",  "Salem",        "OR", "US", "Ipod - Touch - 32 GB",  "http://www.esshopzilla.com/product/?pid=as23233",  "Electronics;Ipod - Touch - 32GB;1;;", "http://www.esshopzilla.com/search/?k=Ipod"),
    (1254034864, "2009-09-27 07:01:04", "Mozilla/5.0", "67.98.123.1", "12", "Salem",        "OR", "US", "Shopping Cart",          "http://www.esshopzilla.com/cart/",                  "",                                    "http://www.esshopzilla.com/product/?pid=as23233"),
    (1254035062, "2009-09-27 07:04:22", "Mozilla/5.0", "67.98.123.1", "11", "Salem",        "OR", "US", "Order Checkout Details", "https://www.esshopzilla.com/checkout/",             "",                                    "http://www.esshopzilla.com/cart/"),
    (1254035161, "2009-09-27 07:06:01", "Mozilla/5.0", "67.98.123.1", "",   "Salem",        "OR", "US", "Order Confirmation",     "https://www.esshopzilla.com/checkout/?a=confirm",   "",                                    "https://www.esshopzilla.com/checkout/"),
    (1254035260, "2009-09-27 07:07:40", "Mozilla/5.0", "67.98.123.1", "1",  "Salem",        "OR", "US", "Order Complete",         "https://www.esshopzilla.com/checkout/?a=complete",  "Electronics;Ipod - Touch - 32GB;1;290;", "https://www.esshopzilla.com/checkout/?a=confirm"),

    # --- Session: IP 23.8.61.21 (Rochester, NY) → Bing → Zune $250 ---
    (1254033379, "2009-09-27 06:36:19", "Safari/525",  "23.8.61.21",  "2",  "Rochester",    "NY", "US", "Zune - 32 GB",           "http://www.esshopzilla.com/product/?pid=asfe13",    "Electronics;Zune - 328GB;1;;",         "http://www.bing.com/search?q=Zune&form=QBLH"),
    (1254033775, "2009-09-27 06:42:55", "Safari/525",  "23.8.61.21",  "12", "Rochester",    "NY", "US", "Shopping Cart",          "http://www.esshopzilla.com/cart/",                  "",                                    "http://www.esshopzilla.com/product/?pid=asfe13"),
    (1254034072, "2009-09-27 06:47:52", "Safari/525",  "23.8.61.21",  "11", "Rochester",    "NY", "US", "Order Checkout Details", "https://www.esshopzilla.com/checkout/",             "",                                    "http://www.esshopzilla.com/cart/"),
    (1254034369, "2009-09-27 06:52:49", "Safari/525",  "23.8.61.21",  "",   "Rochester",    "NY", "US", "Order Confirmation",     "https://www.esshopzilla.com/checkout/?a=confirm",   "",                                    "https://www.esshopzilla.com/checkout/"),
    (1254034666, "2009-09-27 06:57:46", "Safari/525",  "23.8.61.21",  "1",  "Rochester",    "NY", "US", "Order Complete",         "https://www.esshopzilla.com/checkout/?a=complete",  "Electronics;Zune - 32GB;1;250;",       "https://www.esshopzilla.com/checkout/?a=confirm"),

    # --- Session: IP 44.12.96.2 (Duncan, OK) → Google → ipod $190 ---
    (1254033577, "2009-09-27 06:39:37", "Safari/525",  "44.12.96.2",  "",   "Duncan",       "OK", "US", "Hot Buys",               "http://www.esshopzilla.com/hotbuys/",               "",                                    "http://www.google.com/search?hl=en&q=ipod"),
    (1254033874, "2009-09-27 06:44:34", "Safari/525",  "44.12.96.2",  "2",  "Duncan",       "OK", "US", "Ipod - Nano - 8 GB",    "http://www.esshopzilla.com/product/?pid=as32213",  "Electronics;Ipod - Nano - 8GB;1;;",   "http://www.esshopzilla.com/hotbuys/"),
    (1254034171, "2009-09-27 06:49:31", "Safari/525",  "44.12.96.2",  "12", "Duncan",       "OK", "US", "Shopping Cart",          "http://www.esshopzilla.com/cart/",                  "",                                    "http://www.esshopzilla.com/product/?pid=as23233"),
    (1254034468, "2009-09-27 06:54:28", "Safari/525",  "44.12.96.2",  "11", "Duncan",       "OK", "US", "Order Checkout Details", "https://www.esshopzilla.com/checkout/",             "",                                    "http://www.esshopzilla.com/cart/"),
    (1254034765, "2009-09-27 06:59:25", "Safari/525",  "44.12.96.2",  "",   "Duncan",       "OK", "US", "Order Confirmation",     "https://www.esshopzilla.com/checkout/?a=confirm",   "",                                    "https://www.esshopzilla.com/checkout/"),
    (1254034963, "2009-09-27 07:02:43", "Safari/525",  "44.12.96.2",  "1",  "Duncan",       "OK", "US", "Order Complete",         "https://www.esshopzilla.com/checkout/?a=complete",  "Electronics;Ipod - Nano - 8GB;1;190;", "https://www.esshopzilla.com/checkout/?a=confirm"),

    # --- Session: IP 112.33.98.231 (Salt Lake City, UT) → Yahoo → no purchase ---
    (1254033478, "2009-09-27 06:37:58", "Safari/525",  "112.33.98.231","",  "Salt Lake City","UT", "US", "Home",                  "http://www.esshopzilla.com",                        "",                                    "http://search.yahoo.com/search?p=cd+player"),
]

COLUMNS = [f.name for f in HIT_DATA_SCHEMA.fields]


@pytest.fixture()
def processor(spark: SparkSession, tmp_path) -> SearchKeywordProcessor:
    return SearchKeywordProcessor(spark=spark, output_dir=str(tmp_path))


@pytest.fixture()
def sample_df(spark: SparkSession):
    return spark.createDataFrame(SAMPLE_ROWS, schema=HIT_DATA_SCHEMA)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPurchaseEventFilter:
    def test_only_purchase_events_pass(self, spark, processor, sample_df):
        from src.utils.url_parser import extract_search_domain, extract_search_keyword
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        df = sample_df
        df = processor._parse_search_attributes(df)
        df = processor._attribute_session_search(df)
        df = processor._filter_purchase_events(df)

        rows = df.select("ip", "event_list").collect()
        for row in rows:
            # Every remaining row must have standalone "1"
            events = [e.strip() for e in (row.event_list or "").split(",")]
            assert "1" in events, f"Non-purchase row leaked through: {row}"

    def test_event_10_not_treated_as_purchase(self, spark, processor):
        """Regression: event 10 (Shopping Cart Open) must not match purchase filter."""
        row = SAMPLE_ROWS[0]  # empty event_list — not a purchase
        df = spark.createDataFrame([row], schema=HIT_DATA_SCHEMA)
        # Manually set event_list to "10"
        from pyspark.sql import functions as F
        df = df.withColumn("event_list", F.lit("10"))
        df = processor._parse_search_attributes(df)
        df = processor._attribute_session_search(df)
        result = processor._filter_purchase_events(df)
        assert result.count() == 0, "Event '10' was incorrectly matched as a purchase"


class TestSessionAttribution:
    def test_first_search_referrer_propagated(self, spark, processor, sample_df):
        df = processor._parse_search_attributes(sample_df)
        df = processor._attribute_session_search(df)

        # Salem IP — should be attributed to google.com throughout
        salem_rows = df.filter(df.ip == "67.98.123.1").select(
            "session_search_domain", "session_search_keyword"
        ).distinct().collect()

        assert len(salem_rows) == 1
        assert salem_rows[0]["session_search_domain"] == "google.com"
        assert salem_rows[0]["session_search_keyword"] == "Ipod"

    def test_no_search_session_has_null_attribution(self, spark, processor, sample_df):
        """A session that never touched a search engine should have NULL attribution."""
        no_search_row = (
            9999999999, "2009-09-27 09:00:00", "Mozilla/5.0", "1.2.3.4",
            "1", "Chicago", "IL", "US", "Order Complete",
            "https://www.esshopzilla.com/checkout/?a=complete",
            "Electronics;Test;1;99;",
            "http://www.esshopzilla.com",   # internal referrer, not a search engine
        )
        df = spark.createDataFrame([no_search_row], schema=HIT_DATA_SCHEMA)
        df = processor._parse_search_attributes(df)
        df = processor._attribute_session_search(df)
        result = processor._filter_purchase_events(df)
        # Purchase event present but NO search attribution → filtered out
        assert result.count() == 0


class TestRevenueAggregation:
    def test_total_revenue_and_order(self, spark, processor, sample_df):
        df = processor._parse_search_attributes(sample_df)
        df = processor._attribute_session_search(df)
        df = processor._filter_purchase_events(df)
        df = processor._parse_revenue(df)
        result = processor._aggregate_results(df)
        rows = result.collect()

        # Validate count and ordering
        assert len(rows) == 3
        assert rows[0]["Revenue"] >= rows[1]["Revenue"] >= rows[2]["Revenue"]

        revenues = {(r["Search Engine Domain"], r["Search Keyword"]): r["Revenue"] for r in rows}
        assert revenues[("google.com", "Ipod")]  == 290.0
        assert revenues[("bing.com",   "Zune")]  == 250.0
        assert revenues[("google.com", "ipod")]  == 190.0

    def test_output_file_created(self, spark, processor, sample_df, tmp_path):
        """Smoke-test the full pipeline writes a file."""
        df = processor._parse_search_attributes(sample_df)
        df = processor._attribute_session_search(df)
        df = processor._filter_purchase_events(df)
        df = processor._parse_revenue(df)
        result = processor._aggregate_results(df)
        out = processor._write_output(result)

        assert os.path.exists(out)
        with open(out) as f:
            lines = f.readlines()
        assert lines[0].strip() == "Search Engine Domain\tSearch Keyword\tRevenue"
        assert len(lines) == 4  # header + 3 data rows