import json, pathlib, fastjsonschema as fjs
ROOT = pathlib.Path(__file__).resolve().parents[1]
for p in [
    ROOT/"extractor/vp_keepa/schema/keepa_product.schema.json",
    ROOT/"extractor/vp_scraper/schema/scraper_product.schema.json",
    ROOT/"extractor/vp_scraper/schema/scraper_offers.schema.json",
]:
    fjs.compile(json.loads(p.read_text()))
