import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from pipelines.bronze import s3_satisfaction as s3


def xlsx_synthetique(lignes_xml, partagees=()):
    tampon = io.BytesIO()
    m = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with zipfile.ZipFile(tampon, "w") as z:
        z.writestr("xl/workbook.xml",
                   f'<workbook xmlns="{m}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                   '<sheets><sheet name="Resultats" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr("xl/_rels/workbook.xml.rels",
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   '<Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>')
        z.writestr("xl/sharedStrings.xml",
                   f'<sst xmlns="{m}">' + "".join(f"<si>{s}</si>" for s in partagees) + "</sst>")
        z.writestr("xl/worksheets/sheet1.xml", f'<worksheet xmlns="{m}"><sheetData>{lignes_xml}</sheetData></worksheet>')
    return tampon.getvalue()


class TestNommage(unittest.TestCase):
    def test_snake_case_sans_accents(self):
        self.assertEqual(s3.snake_case("Libellé de la variable"), "libelle_de_la_variable")
        self.assertEqual(s3.snake_case("score_PECinf_rea_ajust"), "score_pecinf_rea_ajust")

    def test_colonnes_uniques_et_non_vides(self):
        self.assertEqual(s3.noms_colonnes(["Finess", "finess", "", "2019", "_source"]),
                         ["finess", "finess_2", "colonne_3", "c_2019", "source"])
        self.assertEqual(s3.noms_colonnes(["_ligne"]), ["ligne"])

    def test_nom_table_et_collision_csv_xlsx(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "2016").mkdir()
            for nom in ("jeu-A_donnees.csv", "jeu-A_donnees.xlsx", "rapport.pdf", "~$jeu.xlsx"):
                (Path(d) / "2016" / nom).write_bytes(b"x")
            tables = [f.table for f in s3.lister_fichiers(Path(d))]
        self.assertEqual(tables, ["jeu_a_donnees_csv", "jeu_a_donnees_xlsx"])


class TestLecture(unittest.TestCase):
    def test_csv_cp1252_point_virgule(self):
        lignes, enc, sep = s3.lire_csv("finess;région\r\n000000001;Île-de-France\r\n".encode("cp1252"))
        self.assertEqual((enc, sep), ("cp1252", ";"))
        self.assertEqual(lignes[1], ["000000001", "Île-de-France"])

    def test_csv_utf8_virgule_avec_guillemets(self):
        lignes, enc, sep = s3.lire_csv('a,b\n"x, y",é\n'.encode("utf-8"))
        self.assertEqual((enc, sep), ("utf-8-sig", ","))
        self.assertEqual(lignes[1], ["x, y", "é"])

    def test_xlsx_cellules_partagees_creuses_et_lignes_vides(self):
        xml = ('<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c><c r="C1" t="inlineStr"><is><t>c</t></is></c></row>'
               '<row r="2"><c r="A2" t="s"><v>2</v></c><c r="C2"><v>73.94</v></c></row>'
               '<row r="3"><c r="A3"/></row>')
        lignes, feuille = s3.lire_xlsx(xlsx_synthetique(xml, ["<t>finess</t>", "<r><t>sco</t></r><r><t>re</t></r>", "<t>000000002</t>"]))
        self.assertEqual(feuille, "Resultats")
        self.assertEqual(lignes, [["finess", "score", "c"], ["000000002", "", "73.94"]])

    def test_lire_complete_les_lignes_courtes(self):
        with tempfile.TemporaryDirectory() as d:
            chemin = Path(d) / "f.csv"
            chemin.write_bytes(b"a;b;c\n1;2\n")
            f = s3.lire(s3.lister_fichiers(Path(d))[0])
        self.assertEqual(f.colonnes, ["a", "b", "c"])
        self.assertEqual(f.lignes, [["1", "2", ""]])
        self.assertEqual(len(f.sha256), 64)


class TestScript(unittest.TestCase):
    def test_script_copy_valeurs_vides_en_null_et_echappement(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "f.csv").write_bytes("nom;val\nl'hôpital;\n".encode("cp1252"))
            f = s3.lire(s3.lister_fichiers(Path(d))[0])
            script = s3.script_chargement(f, "b1")
        self.assertIn('CREATE TABLE IF NOT EXISTS bronze_s3."f"', script)
        self.assertIn("ARRAY['nom','val']::TEXT[]", script)
        self.assertIn("l'hôpital,,s3_satisfaction,b1,f.csv,2\n\\.\n", script)


if __name__ == "__main__":
    unittest.main()
