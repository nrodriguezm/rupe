from __future__ import annotations

import unittest

from pipeline.transforms.entity_resolution import norm_name, resolve_buyer_to_rupe


class TestEntityResolution(unittest.TestCase):
    def test_norm_name(self):
        self.assertEqual(norm_name("Intendencia de Montevideo"), "intendencia de montevideo")
        self.assertEqual(norm_name("Secretaría Nacional del Deporte"), "secretaria nacional del deporte")

    def test_resolve(self):
        suppliers = [
            {"identification": "1", "legal_name": "Intendencia de Montevideo"},
            {"identification": "2", "legal_name": "Ministerio de Defensa Nacional"},
        ]
        s, score = resolve_buyer_to_rupe("Intendencia de Montevideo", suppliers, min_score=0.5)
        self.assertIsNotNone(s)
        self.assertEqual(s["identification"], "1")
        self.assertGreaterEqual(score, 0.8)


if __name__ == "__main__":
    unittest.main()
