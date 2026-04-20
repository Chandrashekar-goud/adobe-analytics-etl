"""
URL parsing utilities for identifying external search engine referrers.

Design notes:
- Pure Python helpers (_get_search_domain / _get_search_keyword) are
  unit-testable without Spark.
- Regular Spark UDFs are used instead of pandas_udf to avoid Apache Arrow's
  dependency on sun.misc.Unsafe, which is inaccessible in Java 17/21.
  On AWS Glue/EMR (Java 8/11), pandas_udf would work — but for local
  development on Java 17/21, regular UDFs are the correct choice.
"""

from typing import Optional
from urllib.parse import urlparse, parse_qs

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Configuration — extend this dict to support additional search engines
# ---------------------------------------------------------------------------

SEARCH_ENGINE_PARAMS: dict = {
    "google.com":       "q",
    "google.co.uk":     "q",
    "google.ca":        "q",
    "bing.com":         "q",
    "msn.com":          "q",
    "yahoo.com":        "p",
    "search.yahoo.com": "p",
    "ask.com":          "q",
    "aol.com":          "q",
    "duckduckgo.com":   "q",
}


# ---------------------------------------------------------------------------
# Pure-Python helpers (no Spark dependency — fully unit testable)
# ---------------------------------------------------------------------------

def _normalize_netloc(netloc: str) -> str:
    """Strip 'www.' prefix and lowercase the domain."""
    return netloc.lower().lstrip("www.")


def _match_engine(netloc: str) -> Optional[str]:
    """
    Return the canonical engine key if netloc belongs to a known search engine.
    Handles subdomains (e.g. 'search.yahoo.com' matches 'yahoo.com').
    """
    normalized = _normalize_netloc(netloc)
    for engine in SEARCH_ENGINE_PARAMS:
        if normalized == engine or normalized.endswith("." + engine):
            return engine
    return None


def _get_search_domain(url: Optional[str]) -> Optional[str]:
    """
    Extract the canonical search engine domain from a referrer URL.

    Returns:
        Root domain string (e.g. 'google.com') or None if not a search engine.
    """
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        return _match_engine(parsed.netloc)
    except Exception:
        return None


def _get_search_keyword(url: Optional[str]) -> Optional[str]:
    """
    Extract the search keyword from a search engine referrer URL.

    Returns:
        Keyword string or None if not a recognisable search engine referrer.
    """
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        engine = _match_engine(parsed.netloc)
        if engine is None:
            return None
        param = SEARCH_ENGINE_PARAMS[engine]
        keywords = parse_qs(parsed.query).get(param, [])
        return keywords[0] if keywords else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Standard Spark UDFs — uses pickle serialization, NOT Arrow.
# This avoids the sun.misc.Unsafe / DirectByteBuffer issue on Java 17/21.
# On Glue/EMR (Java 8/11), pandas_udf would be preferred for performance,
# but these are functionally identical and correct on all Java versions.
# ---------------------------------------------------------------------------

@udf(StringType())
def extract_search_domain(url):
    """Spark UDF: extract search engine domain from a referrer URL column."""
    if not url or not isinstance(url, str):
        return None
    try:
        from urllib.parse import urlparse
        ENGINES = {
            "google.com":       "q",
            "google.co.uk":     "q",
            "google.ca":        "q",
            "bing.com":         "q",
            "msn.com":          "q",
            "yahoo.com":        "p",
            "search.yahoo.com": "p",
            "ask.com":          "q",
            "aol.com":          "q",
            "duckduckgo.com":   "q",
        }
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        for engine in ENGINES:
            if netloc == engine or netloc.endswith("." + engine):
                return engine
        return None
    except Exception:
        return None


@udf(StringType())
def extract_search_keyword(url):
    """Spark UDF: extract search keyword from a referrer URL column."""
    if not url or not isinstance(url, str):
        return None
    try:
        from urllib.parse import urlparse, parse_qs
        ENGINES = {
            "google.com":       "q",
            "google.co.uk":     "q",
            "google.ca":        "q",
            "bing.com":         "q",
            "msn.com":          "q",
            "yahoo.com":        "p",
            "search.yahoo.com": "p",
            "ask.com":          "q",
            "aol.com":          "q",
            "duckduckgo.com":   "q",
        }
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        matched_engine = None
        for engine in ENGINES:
            if netloc == engine or netloc.endswith("." + engine):
                matched_engine = engine
                break
        if matched_engine is None:
            return None
        param = ENGINES[matched_engine]
        keywords = parse_qs(urlparse(url).query).get(param, [])
        return keywords[0] if keywords else None
    except Exception:
        return None