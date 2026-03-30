# Sample data snapshots

These snapshots are generated from live sources for development and validation.
- `rss_items_sample.json`: first 25 parsed RSS items
- `detail_1324132.json`: parsed detail sample for one call

Regenerate manually:

```bash
python pipeline/collectors/compras_rss.py > rss_preview.json
python pipeline/collectors/compras_details.py 1324132 > detail_1324132.json
```
