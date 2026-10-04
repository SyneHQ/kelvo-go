#!/usr/bin/env python3
"""Fail-closed controls for the live Date32 evidence, without source access."""
import copy
import unittest
from unittest.mock import Mock, patch
from pathlib import Path
from types import SimpleNamespace
import hashlib
import io
import json
import os
import tempfile

import pyarrow as pa
import federation_date32_acceptance as gate


class Date32AcceptanceControls(unittest.TestCase):
    def fixture(self, mode="pushed"):
        case={"name":"epoch_eq","date_push":True}
        reference={"names":["id"],"types":["INTEGER"],"rows":[[3],[13]]}
        predicates=[{"kind":"comparison","column":"day","type":"date32","op":"eq","value":"0"}] if mode=="pushed" else []
        report={"passed":True,"name":"epoch_eq","mode":mode,**copy.deepcopy(reference),"active_readers":0,"seconds":0.1,
                "stats":{"events":{"rows":2,"bytes":16,"scans":1,"source_wire_bytes":100},"peer":{"rows":0,"bytes":0,"scans":0}},
                "plans":[{"table":"events","plan":{"columns":["id"],"filters":predicates}}],
                "schema":{alias+"."+column:"date32" for alias in ("events","peer") for column in ("day","classic")}}
        events=[{"sql":"SELECT id FROM fixture"+(" WHERE toInt32(day)=0" if mode=="pushed" else ""),
                 "status":200,"complete":True,"finished":True,"arrow_rows":2,"arrow_buffer_bytes":16,"wire_bytes":100}]
        return case,report,reference,events

    def test_accepts_complete_typed_triplet(self):
        for mode in ("pushed","disabled"):
            case,report,reference,events=self.fixture(mode)
            self.assertEqual(gate.validate_phase(case,mode,report,reference,events)["rows"],2)

    def test_rejects_scalar_and_schema_changes(self):
        for mutation in (lambda r:r.update(rows=[[True],[13]]),lambda r:r.update(types=["BIGINT"]),
                         lambda r:r.update(names=["other"]),lambda r:r.update(rows=[[13],[3]])):
            case,report,reference,events=self.fixture();mutation(report)
            with self.assertRaises(AssertionError):gate.validate_phase(case,"pushed",report,reference,events)

    def test_rejects_missing_or_failed_required_result(self):
        for mutation in (lambda r:r.update(passed=False),lambda r:r.update(name="different"),lambda r:r.update(mode="disabled"),
                         lambda r:r.update(active_readers=1),lambda r:r.update(plans=[]),lambda r:r.update(stats={})): 
            case,report,reference,events=self.fixture();mutation(report)
            with self.assertRaises(AssertionError):gate.validate_phase(case,"pushed",report,reference,events)

    def test_rejects_unobserved_or_unenforced_date_push(self):
        case,report,reference,events=self.fixture();report["plans"][0]["plan"]["filters"]=[]
        with self.assertRaises(AssertionError):gate.validate_phase(case,"pushed",report,reference,events)
        case,report,reference,events=self.fixture();events[0]["sql"]="SELECT id FROM fixture"
        with self.assertRaises(AssertionError):gate.validate_phase(case,"pushed",report,reference,events)

    def test_rejects_source_counter_and_completion_corruption(self):
        for field,value in (("arrow_rows",3),("wire_bytes",99),("status",500),("complete",False),("finished",False),("arrow_decode_failed",True)):
            case,report,reference,events=self.fixture();events[0][field]=value
            with self.assertRaises(AssertionError):gate.validate_phase(case,"pushed",report,reference,events)

    def test_disabled_rejects_native_and_source_filters(self):
        case,report,reference,events=self.fixture("disabled");report["plans"][0]["plan"]["filters"]=[{"kind":"is_not_null"}]
        with self.assertRaises(AssertionError):gate.validate_phase(case,"disabled",report,reference,events)
        case,report,reference,events=self.fixture("disabled");events[0]["sql"]+=" WHERE id>0"
        with self.assertRaises(AssertionError):gate.validate_phase(case,"disabled",report,reference,events)

    def test_normalizes_only_supported_exact_arrow_types(self):
        table=pa.table({"id":pa.array([1,None],type=pa.int32()),"day":pa.array([0,None],type=pa.date32())})
        result=gate.normalized(table)
        self.assertEqual(result,{"names":["id","day"],"types":["INTEGER","DATE"],"rows":[[1,"1970-01-01"],[None,None]]})
        with self.assertRaises(AssertionError):gate.normalized(pa.table({"bad":pa.array([0],type=pa.timestamp("us"))}))

    def test_required_workload_and_full_domain_cases_present(self):
        cases=gate.cases();names={case["name"] for case in cases}
        self.assertEqual(len(cases),len(names));self.assertEqual(len(cases),24)
        self.assertTrue({"epoch_eq","epoch_ne","epoch_lt","epoch_le","epoch_gt","epoch_ge","finite_min","finite_max",
                         "negative_infinity","positive_infinity","before_source_calendar","after_source_calendar",
                         "classic_date","null_and_or","projection_reorder","count_only","mixed_residual","self_join",
                         "independent_aliases","outer_join","cte_window","repeated_scan"}<=names)

class Date32CleanupControls(unittest.TestCase):
    baseline="a"*64+" sha256:"+"b"*64+" true 2026-10-04T00:00:00Z 4294967296 1500000000"

    def fixture(self,directory,intent=True):
        f=gate.Fixture.__new__(gate.Fixture)
        f.args=SimpleNamespace(container="fixture-container",expected_container_identity=self.baseline,expected_source_version="26.9.7.9")
        f.directory=Path(directory);f.child=None;f.observer=None;f.attempt="first";f.command=0
        f.manifest={"id":"c"*24,"container":"fixture-container","creation_intent":intent,
                    "container_identity":self.baseline,"container_id":"a"*64,"source_version":"26.9.7.9","preserved_rows":"42"}
        f.database="kelvo_date32_"+f.manifest["id"];f.username=f.database+"_reader"
        gate.private_write(f.directory/"fixture.json",json.dumps(f.manifest))
        f.identity=Mock(return_value=self.baseline)
        f.admin=Mock(side_effect=lambda sql,**kw:"42" if "system.parts" in sql else "0")
        return f

    def test_cleanup_only_removes_owned_names_and_verifies_absence(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory);result=f.cleanup()
            self.assertTrue(result["passed"])
            sql=[call.args[0] for call in f.admin.call_args_list]
            self.assertIn("DROP USER IF EXISTS `"+f.username+"`",sql)
            self.assertIn("DROP DATABASE IF EXISTS `"+f.database+"` SYNC",sql)
            self.assertTrue(any("startsWith(query_id, '"+f.database+"_admin_')" in value for value in sql))
            self.assertEqual(sum("system.users" in value for value in sql),1)
            self.assertEqual(sum("system.databases" in value for value in sql),1)
            self.assertEqual(f.identity.call_count,2)

    def test_changed_container_refuses_before_admin(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory);f.identity.return_value="changed"
            with self.assertRaises(AssertionError):f.cleanup()
            f.admin.assert_not_called()

    def test_failed_local_shutdown_does_not_suppress_source_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory);f.observer=Mock();f.observer.stop.side_effect=RuntimeError("stop failed")
            with self.assertRaises(AssertionError):f.cleanup()
            self.assertTrue(any(call.args[0].startswith("DROP DATABASE") for call in f.admin.call_args_list))
            self.assertTrue(json.loads((Path(directory)/"cleanup.json").read_text())["owned_database_removed"])

    def test_repeated_cleanup_preserves_primary_receipt_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory);f.cleanup()
            paths=[Path(directory)/name for name in ("cleanup.json","launch-closed.json")]
            before=[gate.sha(path) for path in paths];f.cleanup()
            self.assertEqual(before,[gate.sha(path) for path in paths])
            self.assertEqual(len(list(Path(directory).glob("cleanup-attempt-*.json"))),2)

    def test_no_creation_intent_never_runs_source_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory,False);self.assertEqual(f.cleanup(),{"passed":True,"created":False})
            f.admin.assert_not_called();f.identity.assert_not_called()

    def test_namespace_collision_refuses_create(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory,False)
            f.admin.side_effect=lambda sql,**kw: "UTC" if "timezone" in sql else "26.9.7.9" if "version()" in sql else "1"
            with self.assertRaises(AssertionError):f.setup()
            self.assertFalse(any(call.args[0].startswith("CREATE") for call in f.admin.call_args_list))
            self.assertFalse(f.manifest["creation_intent"])

    def test_invalid_nonce_refuses_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            f=self.fixture(directory);f.manifest["id"]="invalid;DROP"
            gate.private_write(Path(directory)/"fixture.json",json.dumps(f.manifest));f.args.directory=Path(directory)
            with self.assertRaises(AssertionError):gate.Fixture(f.args,cleanup=True)

    def test_absent_and_private_empty_directory_cleanup_is_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            for target in (Path(directory)/"absent",Path(directory)):
                args=["gate","--action","cleanup","--directory",str(target),"--endpoint","http://127.0.0.1/",
                      "--expected-container-identity",self.baseline,"--expected-source-version","26.9.7.9"]
                with patch("sys.argv",args),patch("sys.platform","linux"),patch("sys.stdout",new_callable=io.StringIO),patch.object(gate,"Fixture") as constructor:
                    self.assertEqual(gate.main(),0);constructor.assert_not_called()

if __name__=="__main__":unittest.main()

