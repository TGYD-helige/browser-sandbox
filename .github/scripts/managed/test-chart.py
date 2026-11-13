"""Validate the operator-facing chart with Helm's actual renderer."""
import pathlib
import subprocess
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]


def render(*arguments):
    output = subprocess.check_output([
        "helm", "template", "trial", str(ROOT / "charts/browser-sandbox"),
        "--namespace", "browser-test", "--set", "networkPolicy.apiServerCidrs[0]=10.43.0.1/32", "--set", "image.digest=sha256:" + "a" * 64,
        *arguments,
    ], text=True)
    return [item for item in yaml.safe_load_all(output) if item]


class ChartTest(unittest.TestCase):
    def test_only_controller_is_a_workload(self):
        resources = render()
        workloads = [item for item in resources if item["kind"] in
                     ["Pod", "Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"]]
        self.assertEqual([item["kind"] for item in workloads], ["Deployment"])
        deployment = workloads[0]
        self.assertEqual(deployment["spec"]["replicas"], 1)
        controller = deployment["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(controller["image"], "amaster/browser-sandbox@sha256:" + "a" * 64)
        self.assertEqual(controller["securityContext"]["capabilities"]["drop"], ["ALL"])
        roles = [item for item in resources if item["kind"] == "Role"]
        self.assertEqual(len(roles), 1)
        self.assertFalse(any("*" in rule.get("resources", []) for rule in roles[0]["rules"]))
        owners = [item for item in resources if item["kind"] == "ConfigMap" and
                  item["metadata"]["name"].endswith("-workers")]
        self.assertEqual(len(owners), 1)
        self.assertNotIn("ownerReferences", owners[0]["metadata"])

    def test_worker_network_is_private_and_internet_excludes_internal_ranges(self):
        policies = [item for item in render() if item["kind"] == "NetworkPolicy"]
        self.assertEqual(len(policies), 1)
        policy = policies[0]["spec"]
        self.assertEqual(policy["policyTypes"], ["Ingress", "Egress"])
        self.assertEqual(policy["podSelector"]["matchLabels"]["app.kubernetes.io/component"], "worker")
        source = policy["ingress"][0]["from"][0]
        self.assertEqual(source["podSelector"]["matchLabels"]["app.kubernetes.io/component"], "controller")
        self.assertNotIn("namespaceSelector", source)
        public = policy["egress"][0]["to"][0]["ipBlock"]
        self.assertEqual(public["cidr"], "0.0.0.0/0")
        for denied in ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16"]:
            self.assertIn(denied, public["except"])
        self.assertEqual(policy["egress"][1]["ports"], [
            {"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}])

    def test_worker_template_upgrade_rolls_controller_with_cleanup_budget(self):
        original = next(item for item in render() if item["kind"] == "Deployment")
        changed = next(item for item in render("--set", "worker.shmSize=1Gi") if item["kind"] == "Deployment")
        self.assertNotEqual(original["spec"]["template"], changed["spec"]["template"])
        self.assertGreaterEqual(original["spec"]["template"]["spec"]["terminationGracePeriodSeconds"], 80)

    def test_bare_worker_disruption_budget_uses_integer_minimum(self):
        budget = next(item for item in render("--set", "worker.disruptionBudget.enabled=true") if item["kind"] == "PodDisruptionBudget")
        self.assertEqual(budget["spec"]["minAvailable"], 1)
        self.assertNotIn("maxUnavailable", budget["spec"])


if __name__ == "__main__":
    unittest.main()
