from __future__ import annotations

import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from pipeline.collectors.compras_rss import _extract_dates, _extract_id, _strip_html


class TestRssParser(unittest.TestCase):
    def test_extract_id(self):
        self.assertEqual(_extract_id("http://x/consultas/detalle/id/1324132"), "1324132")
        self.assertEqual(_extract_id("http://x/consultas/detalle/id/i487659"), "i487659")

    def test_strip_html(self):
        raw = "A<br/>B&nbsp;C"
        self.assertIn("A", _strip_html(raw))
        self.assertIn("B C", _strip_html(raw))

    def test_extract_dates(self):
        desc = "Recepción de ofertas hasta: 01/04/2027 11:00hs Publicado: 24/03/2026 10:30hs | Última Modificación: 26/03/2026 14:48hs"
        pub, deadline, mod = _extract_dates(desc)
        self.assertEqual(pub, "24/03/2026 10:30")
        self.assertEqual(deadline, "01/04/2027 11:00")
        self.assertEqual(mod, "26/03/2026 14:48")

    def test_fixture_shape(self):
        xml_text = Path("tests/fixtures/rss_sample.xml").read_text(encoding="utf-8")
        root = ET.fromstring(xml_text)
        items = root.findall("./channel/item")
        self.assertGreaterEqual(len(items), 2)


if __name__ == "__main__":
    unittest.main()
