import json
import pathlib
import fastjsonschema as fjs

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMAS = [
    ROOT / "extractor/vp_keepa/schema/keepa_product.schema.json",
    ROOT / "extractor/vp_scraper/schema/scraper_product.schema.json",
    ROOT / "extractor/vp_scraper/schema/scraper_offers.schema.json",
]

def test_json_schemas_compile_if_present():
    for p in SCHEMAS:
        if p.exists():
            data = json.loads(p.read_text())
            fjs.compile(data)
