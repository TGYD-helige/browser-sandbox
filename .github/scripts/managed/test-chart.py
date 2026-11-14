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
        workloads = [item for item in resources if not item["metadata"].get("annotations", {}).get("helm.sh/hook") and item["kind"] in
                     ["Pod", "Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"]]
        self.assertEqual([item["kind"] for item in workloads], ["Deployment"])
        deployment = workloads[0]
        self.assertEqual(deployment["spec"]["replicas"], 1)
        controller = deployment["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(controller["image"], "amaster/browser-sandbox@sha256:" + "a" * 64)
        self.assertEqual(controller["securityContext"]["capabilities"]["drop"], ["ALL"])
        roles = [item for item in resources if item["kind"] == "Role"]
        self.assertEqual(len(roles), 2)
        self.assertFalse(any("*" in rule.get("resources", []) for role in roles for rule in role["rules"]))
        owners = [item for item in resources if item["kind"] == "ConfigMap" and
                  item["metadata"]["name"].endswith("-workers")]
        self.assertEqual(len(owners), 1)
        self.assertNotIn("ownerReferences", owners[0]["metadata"])

    def test_controller_journal_has_its_own_retained_private_volume(self):
        resources = render()
        claim = next(item for item in resources if item["kind"] == "PersistentVolumeClaim")
        self.assertTrue(claim["metadata"]["name"].endswith("-data"))
        self.assertEqual(claim["metadata"]["annotations"]["helm.sh/resource-policy"], "keep")
        self.assertEqual(claim["spec"]["accessModes"], ["ReadWriteOnce"])
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        self.assertEqual(deployment["spec"]["strategy"]["type"], "Recreate")
        pod = deployment["spec"]["template"]["spec"]
        self.assertEqual(pod["securityContext"]["fsGroup"], 20000)
        controller = pod["containers"][0]
        env = {item["name"]: item.get("value") for item in controller["env"]}
        self.assertEqual(env["BROWSER_CONTROLLER_STATE"], "/controller-data/executions")
        self.assertNotIn("BROWSER_PROFILE_STORE_ROOT", env)
        self.assertIn({"name": "controller-data", "mountPath": "/controller-data"}, controller["volumeMounts"])
        self.assertFalse(any(item["kind"] == "PersistentVolumeClaim" for item in render("--set", "controllerData.existingClaim=retained")))

    def test_worker_network_is_private_and_internet_excludes_internal_ranges(self):
        policies = [item for item in render() if item["kind"] == "NetworkPolicy" and item["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "worker"]
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

    def test_profile_store_is_private_retained_and_mounted_only_by_controller(self):
        resources = render("--set", "profileStore.enabled=true")
        claims = [item for item in resources if item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"].endswith("-profiles")]
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]["metadata"]["annotations"]["helm.sh/resource-policy"], "keep")
        self.assertEqual(claims[0]["spec"]["accessModes"], ["ReadWriteOnce"])
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        self.assertEqual(deployment["spec"]["strategy"]["type"], "Recreate")
        pod = deployment["spec"]["template"]["spec"]
        self.assertEqual(pod["securityContext"]["fsGroup"], 20000)
        controller = pod["containers"][0]
        environment = {item["name"]: item.get("value") for item in controller["env"]}
        self.assertEqual(environment["BROWSER_PROFILE_STORE_ROOT"], "/var/lib/browser-sandbox/profiles")
        self.assertTrue(environment["BROWSER_PROFILE_STORE_ORIGIN"].endswith(":8083"))
        storage = next(item for item in resources if item["kind"] == "Service" and item["metadata"]["name"].endswith("-storage"))
        self.assertEqual(storage["spec"]["type"], "ClusterIP")
        self.assertEqual(storage["spec"]["ports"][0]["port"], 8083)
        self.assertFalse(any(item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"].endswith("-profiles") for item in render()))
        adopted = render("--set", "profileStore.enabled=true", "--set", "profileStore.existingClaim=retained")
        self.assertFalse(any(item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"].endswith("-profiles") for item in adopted))
        configuration = next(item for item in resources if item["kind"] == "ConfigMap" and item["metadata"]["name"].endswith("-config"))
        self.assertNotIn("profile-store", configuration["data"]["worker-template.json"])
        private = [item for item in resources if item["kind"] == "NetworkPolicy" and item["metadata"]["name"].endswith("-storage")]
        self.assertEqual(len(private), 1)
        allowed = private[0]["spec"]["ingress"]
        self.assertEqual(allowed[0]["ports"], [{"protocol": "TCP", "port": 8083}])
        self.assertEqual(allowed[0]["from"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/component"], "worker")

    def test_host_storage_egress_matches_runtime_exact_destination(self):
        resources = render("--set", "networkPolicy.storageEgress[0].address=10.42.1.2", "--set", "networkPolicy.storageEgress[0].port=9083")
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        environment = {item["name"]: item.get("value") for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(environment["BROWSER_STORAGE_EGRESS"], '[{"address":"10.42.1.2","port":9083}]')
        policy = next(item for item in resources if item["kind"] == "NetworkPolicy")
        self.assertEqual(policy["spec"]["egress"][-1], {
            "to": [{"ipBlock": {"cidr": "10.42.1.2/32"}}],
            "ports": [{"protocol": "TCP", "port": 9083}],
        })

    def test_worker_template_upgrade_rolls_controller_with_cleanup_budget(self):
        original = next(item for item in render() if item["kind"] == "Deployment")
        changed = next(item for item in render("--set", "worker.shmSize=1Gi") if item["kind"] == "Deployment")
        self.assertNotEqual(original["spec"]["template"], changed["spec"]["template"])
        self.assertGreaterEqual(original["spec"]["template"]["spec"]["terminationGracePeriodSeconds"], 80)

    def test_bare_worker_disruption_budget_uses_integer_minimum(self):
        budget = next(item for item in render("--set", "worker.disruptionBudget.enabled=true") if item["kind"] == "PodDisruptionBudget")
        self.assertEqual(budget["spec"]["minAvailable"], 1)
        self.assertNotIn("maxUnavailable", budget["spec"])

    def test_uninstall_hook_drains_before_network_policy_can_be_deleted(self):
        hook = next(item for item in render() if item["kind"] == "Job")
        self.assertEqual(hook["metadata"]["annotations"]["helm.sh/hook"], "pre-delete")
        self.assertEqual(hook["spec"]["backoffLimit"], 0)
        container = hook["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["command"], ["node", "/opt/browser-sandbox/dist/kubernetes-drain-main.js"])
        self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])
        self.assertGreater(hook["spec"]["activeDeadlineSeconds"], 180)

    def test_runtime_cannot_override_controller_owned_environment(self):
        for name in ["BROWSER_IMAGE", "BROWSER_BACKEND", "BROWSER_INSTALLATION", "BROWSER_HOST_CONTROL_HOST", "KUBERNETES_SERVICE_HOST", "NODE_OPTIONS"]:
            with self.subTest(name=name):
                with self.assertRaises(subprocess.CalledProcessError) as error:
                    render("--set-string", f"runtime.{name}=override")
                self.assertNotEqual(error.exception.returncode, 0)

    def test_drain_and_controller_have_separate_least_privilege_roles(self):
        resources = render()
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        hook = next(item for item in resources if item["kind"] == "Job")
        controller_sa = deployment["spec"]["template"]["spec"]["serviceAccountName"]
        drain_sa = hook["spec"]["template"]["spec"]["serviceAccountName"]
        self.assertNotEqual(controller_sa, drain_sa)
        bindings = {item["subjects"][0]["name"]: item["roleRef"]["name"] for item in resources if item["kind"] == "RoleBinding"}
        roles = {item["metadata"]["name"]: item["rules"] for item in resources if item["kind"] == "Role"}
        controller_rules = roles[bindings[controller_sa]]
        drain_rules = roles[bindings[drain_sa]]
        self.assertFalse(any("watch" in rule["verbs"] or "deployments/scale" in rule["resources"] for rule in controller_rules))
        self.assertFalse(any("create" in rule["verbs"] or "watch" in rule["verbs"] for rule in drain_rules))
        scale = next(rule for rule in drain_rules if "deployments/scale" in rule["resources"])
        self.assertEqual(scale["resourceNames"], [deployment["metadata"]["name"]])
        self.assertEqual(set(scale["verbs"]), {"get", "patch"})

    def test_host_control_is_private_and_separate_from_agent_service(self):
        resources = render()
        services = {item["metadata"]["name"]: item["spec"] for item in resources if item["kind"] == "Service"}
        self.assertEqual([port["port"] for port in services["trial-browser"]["ports"]], [8080])
        self.assertEqual(services["trial-browser-host"]["type"], "ClusterIP")
        self.assertEqual([port["port"] for port in services["trial-browser-host"]["ports"]], [8082])
        policy = next(item["spec"] for item in resources if item["kind"] == "NetworkPolicy" and item["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "controller")
        access = next(rule for rule in policy["ingress"] if rule["ports"][0]["port"] == 8080)
        host = next(rule for rule in policy["ingress"] if rule["ports"][0]["port"] == 8082)
        self.assertNotIn("from", access)
        self.assertEqual(host["from"], [{"podSelector": {"matchLabels": {"browser-sandbox.io/host-control": "true"}}}])


if __name__ == "__main__":
    unittest.main()
