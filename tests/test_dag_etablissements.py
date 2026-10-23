"""Tests de dags/dag_etablissements.py sans Airflow : analyse du fichier, puis import avec un Airflow factice.

    python -m unittest tests.test_dag_etablissements
"""

import ast
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

from pipelines.bronze import csv_bronze

DAG_PY = Path(__file__).resolve().parent.parent / "dags" / "dag_etablissements.py"
TABLE = "s2_activite_professionnel_sante"


def arbre():
    return ast.parse(DAG_PY.read_text(encoding="utf-8"))


def parametres_dag():
    appel = next(
        n for n in ast.walk(arbre())
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "DAG"
    )
    return {k.arg: k.value for k in appel.keywords}


def charger_dag():
    """Importe le fichier du DAG avec de faux modules airflow ; renvoie le module et le faux PythonOperator."""
    airflow = types.ModuleType("airflow")
    airflow.DAG = mock.MagicMock()
    operateurs = types.ModuleType("airflow.operators")
    python = types.ModuleType("airflow.operators.python")
    python.PythonOperator = mock.MagicMock()
    exceptions = types.ModuleType("airflow.exceptions")
    exceptions.AirflowFailException = type("AirflowFailException", (Exception,), {})
    faux = {
        "airflow": airflow,
        "airflow.operators": operateurs,
        "airflow.operators.python": python,
        "airflow.exceptions": exceptions,
    }
    with mock.patch.dict(sys.modules, faux):
        spec = importlib.util.spec_from_file_location("dag_etablissements", DAG_PY)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module, python.PythonOperator


class TestDagEtablissements(unittest.TestCase):
    def test_planification_mensuelle_sans_rattrapage(self):
        p = parametres_dag()
        self.assertEqual(ast.literal_eval(p["dag_id"]), "dag_etablissements")
        self.assertEqual(ast.literal_eval(p["schedule"]), "@monthly")
        self.assertIs(ast.literal_eval(p["catchup"]), False)
        self.assertEqual(ast.literal_eval(p["max_active_runs"]), 1)
        self.assertEqual(ast.literal_eval(p["max_active_tasks"]), 1)

    def test_tables_alignees_sur_le_catalogue(self):
        constante = next(
            n.value for n in arbre().body
            if isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "TABLES_S2" for t in n.targets)
        )
        attendues = [c.table for c in csv_bronze.CATALOGUE if c.source == "s2"]
        self.assertEqual(sorted(ast.literal_eval(constante)), sorted(attendues))

    def test_une_tache_par_table(self):
        module, operateur = charger_dag()
        appels = operateur.call_args_list
        self.assertEqual([a.kwargs["op_kwargs"]["table"] for a in appels], list(module.TABLES_S2))
        self.assertEqual([a.kwargs["task_id"] for a in appels], [f"bronze_{t}" for t in module.TABLES_S2])

    def test_la_tache_lance_csv_bronze_sur_sa_table(self):
        module, _ = charger_dag()
        with mock.patch.object(csv_bronze, "main", return_value=0) as main:
            module.ingerer_table(TABLE)
        main.assert_called_once_with(["--table", TABLE])

    def test_la_tache_echoue_si_l_ingestion_echoue(self):
        module, _ = charger_dag()
        with mock.patch.object(csv_bronze, "main", return_value=1):
            with self.assertRaisesRegex(Exception, TABLE):
                module.ingerer_table(TABLE)


if __name__ == "__main__":
    unittest.main()
