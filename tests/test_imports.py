import importlib

def test_imports():
    for mod in [
        "extractor.vp_keepa.client",
        "extractor.vp_scraper.clients.scraperapi_client",
        "extractor.spapi.orders_daily",
    ]:
        assert importlib.import_module(mod)
