import importlib

def test_imports():
    modules = [
        "extractor.vp_keepa.client",
        "extractor.vp_scraper.clients.scraperapi_client",
        "extractor.spapi.orders_daily",
    ]
    for mod in modules:
        assert importlib.import_module(mod)
