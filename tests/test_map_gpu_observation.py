from __future__ import annotations

import ast
import unittest

from test_stream_v3_map_runtime_probe import healthy_sample, load_probe, NOW


class MapGpuObservationTests(unittest.TestCase):
    def probe_namespace(self):
        tree = ast.parse(load_probe().PROCESS_PROBE)
        selected = [node for node in tree.body if
                    isinstance(node, ast.FunctionDef) and node.name in ("browser_snapshot", "gpu_process_arguments")
                    or isinstance(node, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id == "SWIFTSHADER_FLAGS" for t in node.targets)]
        namespace = {}
        exec(compile(ast.Module(body=selected, type_ignores=[]), "browser-probe", "exec"), namespace)
        return namespace

    def snapshot(self, processes, gpu_pids):
        return self.probe_namespace()["browser_snapshot"](processes, gpu_pids)

    def test_gpu_detection_accepts_real_flattened_child_arguments(self):
        detect = self.probe_namespace()["gpu_process_arguments"]
        self.assertTrue(detect(["chromium", "--type=gpu-process"]))
        self.assertTrue(detect([
            "/usr/lib/chromium/chromium --type=gpu-process --no-sandbox --use-angle=vulkan", ""]))
        self.assertIn("if gpu_process_arguments(args):", load_probe().PROCESS_PROBE)

    def test_other_process_types_and_partial_tokens_are_rejected(self):
        detect = self.probe_namespace()["gpu_process_arguments"]
        for args in (["chromium --type=renderer"], ["chromium --type=gpu-process-extra"], []):
            self.assertFalse(detect(args))

    def processes(self):
        return {
            10: {"parent": 1, "args": ["chromium", "--app=http://127.0.0.1/adsb-map/",
                                       "--use-gl=angle", "--use-angle=vulkan"]},
            11: {"parent": 10, "args": ["chromium", "--type=zygote"]},
            12: {"parent": 11, "args": ["chromium", "--type=gpu-process"]},
        }

    def test_nvidia_libraries_belong_to_the_vulkan_map_browser(self):
        result = self.snapshot(self.processes(), {12})
        self.assertEqual(result["nvidia_vulkan_main_process_count"], 1)
        self.assertEqual(result["swiftshader_main_process_count"], 0)

    def test_vulkan_flags_without_driver_evidence_fail(self):
        self.assertEqual(self.snapshot(self.processes(), set())["nvidia_vulkan_main_process_count"], 0)

    def test_foreign_gpu_process_does_not_qualify_the_map_browser(self):
        processes = self.processes()
        processes[12]["parent"] = 999
        self.assertEqual(self.snapshot(processes, {12})["nvidia_vulkan_main_process_count"], 0)

    def test_parent_cycle_fails_closed(self):
        processes = self.processes()
        processes[11]["parent"] = 12
        self.assertEqual(self.snapshot(processes, {12})["nvidia_vulkan_main_process_count"], 0)

    def test_another_app_gpu_does_not_qualify_the_map(self):
        processes = self.processes()
        processes[20] = {"parent": 1, "args": ["chromium", "--app=http://127.0.0.1/other/",
                                                 "--use-gl=angle", "--use-angle=vulkan"]}
        processes[12]["parent"] = 20
        self.assertEqual(self.snapshot(processes, {12})["nvidia_vulkan_main_process_count"], 0)

    def test_swiftshader_rollback_is_still_recognized(self):
        processes = self.processes()
        processes[10]["args"][-1:] = ["--use-angle=swiftshader", "--enable-unsafe-swiftshader"]
        result = self.snapshot(processes, set())
        self.assertEqual(result["swiftshader_main_process_count"], 1)
        self.assertEqual(result["nvidia_vulkan_main_process_count"], 0)

    def test_actual_gpu_backend_passes_existing_delivery_checks(self):
        sample = healthy_sample()
        sample["process"]["browser"].update(
            swiftshader_main_process_count=0, nvidia_vulkan_main_process_count=1)
        payload = load_probe().evaluate_sample(sample, now_epoch=NOW)
        self.assertTrue(payload["delivery_critical_ok"])
        self.assertEqual(payload["browser"]["rendering_backend"], "nvidia-vulkan")

    def test_unknown_backend_does_not_pass(self):
        sample = healthy_sample()
        sample["process"]["browser"]["swiftshader_main_process_count"] = 0
        payload = load_probe().evaluate_sample(sample, now_epoch=NOW)
        self.assertIn("browser_contract", payload["critical_reasons"])

    def test_gpu_does_not_hide_a_stale_render_or_webgl_failure(self):
        for failure in ("stale", "fatal"):
            with self.subTest(failure=failure):
                sample = healthy_sample()
                sample["process"]["browser"].update(
                    swiftshader_main_process_count=0, nvidia_vulkan_main_process_count=1)
                if failure == "stale":
                    sample["process"]["render"]["payload"]["age_sec"] = 31
                else:
                    sample["process"]["browser_log"]["context_fatal_failure"] = True
                self.assertFalse(load_probe().evaluate_sample(sample, now_epoch=NOW)["delivery_critical_ok"])


if __name__ == "__main__":
    unittest.main()
