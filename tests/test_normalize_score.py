from __future__ import annotations

import unittest

from pipeline.transforms.normalize_opportunities import normalize_listing_item
from pipeline.transforms.score_and_assign import score


class TestNormalizeAndScore(unittest.TestCase):
    def test_normalize_dates(self):
        item = {
            "external_id": "abc123",
            "title": "Compra de uniformes escolares",
            "description": "remeras y túnicas",
            "published": "24/03/2026 10:30",
            "deadline": "01/04/2027 11:00",
            "status": "open",
        }
        op = normalize_listing_item(item, "http://example")
        self.assertEqual(op.external_id, "abc123")
        self.assertEqual(op.publish_at.year, 2026)
        self.assertEqual(op.deadline_at.year, 2027)

    def test_score_hits(self):
        item = {
            "external_id": "x1",
            "title": "Compra de uniforme escolar",
            "description": "remera y túnica para colegio",
            "published": "24/03/2026 10:30",
            "deadline": "01/04/2027 11:00",
            "status": "open",
        }
        op = normalize_listing_item(item, "http://example")
        profile = {"keywords_include": ["uniforme", "escolar", "colegio", "remera"], "keywords_exclude": ["dragado"]}
        s, reasons = score(op, profile)
        self.assertGreaterEqual(s, 30)
        self.assertIn("uniforme", reasons["include_hits"])

    def test_category_and_amount_from_title_desc(self):
        item = {
            "external_id": "x2",
            "title": "Concurso de Precios 55/2026 - Organismo X | Unidad Y",
            "description": "Monto estimado: $ 12.345,67",
            "published": "24/03/2026 10:30",
            "deadline": "01/04/2027 11:00",
            "status": "open",
        }
        op = normalize_listing_item(item, "http://example")
        self.assertEqual(op.category, "Concurso de Precios")
        self.assertIsNotNone(op.amount)
        self.assertEqual(op.currency, "UYU")


if __name__ == "__main__":
    unittest.main()
