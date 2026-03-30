from __future__ import annotations

import unittest

from pipeline.transforms.rupe_parse import detect_field, parse_rows


class TestRupeIngest(unittest.TestCase):
    def test_detect_field(self):
        row = {"País": "UY", "Identificación": "123", "Denominación Social": "ABC"}
        self.assertEqual(detect_field(row, ["pais"]), "País")
        self.assertEqual(detect_field(row, ["ident"]), "Identificación")
        self.assertEqual(detect_field(row, ["denomin"]), "Denominación Social")

    def test_parse_rows(self):
        csv_text = "País,Identificación,Denominación Social,Domicilio Fiscal,Localidad,Departamento,Estado\nUY,123,ABC SA,Calle 1,Montevideo,Montevideo,Activo\n"
        rows = parse_rows(csv_text, "Registro de Proveedores - Test")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["identification"], "123")
        self.assertEqual(rows[0]["legal_name"], "ABC SA")
        self.assertEqual(rows[0]["status"], "Activo")


if __name__ == "__main__":
    unittest.main()
