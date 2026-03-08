.PHONY: pipeline extract transform final validate clean

pipeline:
	python -m pipeline.run

extract:
	python -c "from pipeline.extract.scrape_manifest import build_manifest; build_manifest(list(range(2000,2025)))"
	python -c "from pipeline.extract.download_crush import download_crush; download_crush()"

transform:
	python -c "from pipeline.transform.parse_crush_tb08 import parse_all_crush; parse_all_crush(list(range(2000,2025)))"

final:
	python -c "from pipeline.load.build_dimensions import build_dimensions; build_dimensions(list(range(2000,2025)))"
	python -c "from pipeline.load.build_facts import build_facts; build_facts(list(range(2000,2025)))"
	python -c "from pipeline.load.build_bridge import build_bridge; build_bridge(list(range(2000,2025)))"

validate:
	python -c "from pipeline.load.validate import validate; validate()"

clean:
	rm -rf data/raw/ data/silver/
