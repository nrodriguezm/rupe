from __future__ import annotations

import unittest
from pathlib import Path

from pipeline.collectors.compras_details import parse_detail


class TestDetailParser(unittest.TestCase):
    def test_parse_detail_title_and_buyer(self):
        html = Path("tests/fixtures/detail_sample.html").read_text(encoding="utf-8")
        d = parse_detail(html, "1324132")
        self.assertIn("Compra Directa 29/2026", d.title or "")
        self.assertEqual(d.organismo, "Presidencia de la República")
        self.assertEqual(d.buyer, "Secretaría Nacional del Deporte")
        self.assertIn("Recepción de ofertas", d.body_text)


if __name__ == "__main__":
    unittest.main()
