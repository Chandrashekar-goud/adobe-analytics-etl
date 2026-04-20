"""Unit tests for URL parsing helpers — no Spark required."""

import pytest
from utils.url_parser import _get_search_domain, _get_search_keyword


class TestGetSearchDomain:
    def test_google(self):
        assert _get_search_domain("http://www.google.com/search?q=Ipod") == "google.com"

    def test_bing(self):
        assert _get_search_domain("http://www.bing.com/search?q=Zune") == "bing.com"

    def test_yahoo(self):
        assert _get_search_domain("http://search.yahoo.com/search?p=cd+player") == "yahoo.com"

    def test_msn(self):
        assert _get_search_domain("http://www.msn.com/search?q=test") == "msn.com"

    def test_internal_url_returns_none(self):
        assert _get_search_domain("http://www.esshopzilla.com/cart/") is None

    def test_empty_string_returns_none(self):
        assert _get_search_domain("") is None

    def test_none_returns_none(self):
        assert _get_search_domain(None) is None

    def test_malformed_url_returns_none(self):
        assert _get_search_domain("not_a_url") is None


class TestGetSearchKeyword:
    def test_google_q_param(self):
        url = "http://www.google.com/search?hl=en&q=Ipod&aq=f"
        assert _get_search_keyword(url) == "Ipod"

    def test_bing_q_param(self):
        url = "http://www.bing.com/search?q=Zune&form=QBLH&qs=n"
        assert _get_search_keyword(url) == "Zune"

    def test_yahoo_p_param(self):
        url = "http://search.yahoo.com/search?p=cd+player&toggle=1"
        assert _get_search_keyword(url) == "cd player"

    def test_no_keyword_param_returns_none(self):
        # google URL with no 'q' parameter
        assert _get_search_keyword("http://www.google.com/search?hl=en") is None

    def test_non_search_engine_returns_none(self):
        assert _get_search_keyword("http://www.esshopzilla.com/hotbuys/") is None

    def test_empty_returns_none(self):
        assert _get_search_keyword("") is None