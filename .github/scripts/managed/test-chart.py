"""Validate the operator-facing chart with Helm's actual renderer."""
import json
import pathlib
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]


def render(*arguments, release="trial", chart=ROOT / "charts/browser-sandbox"):
    output = subprocess.check_output([
        "helm", "template", release, str(chart),
        "--namespace", "browser-test", "--set", "networkPolicy.apiServerCidrs[0]=10.43.0.1/32", "--set", "image.digest=sha256:" + "a" * 64,
        *arguments,
    ], text=True)
    return [item for item in yaml.safe_load_all(output) if item]


class ChartTest(unittest.TestCase):
    def test_private_storage_domain_is_scoped_to_workers_and_configured_port(self):
        addresses = [{"address": "10.42.1.2", "port": 9083}]
        domains = [{"hostname": "s3.example.com", "port": 443}]
        resources = render("--set-json", "networkPolicy.storageEgress=" + json.dumps(addresses),
                           "--set-json", "networkPolicy.storageEgressDomains=" + json.dumps(domains))
        domain_policy = next(item["spec"] for item in resources if item["kind"] == "CiliumNetworkPolicy")
        network_policy = next(item["spec"] for item in resources if item["metadata"]["name"] == "trial-browser-workers")
        self.assertEqual(domain_policy["endpointSelector"], network_policy["podSelector"])
        self.assertEqual(domain_policy["egress"][1], {
            "toFQDNs": [{"matchName": "s3.example.com"}],
            "toPorts": [{"ports": [{"port": "443", "protocol": "TCP"}]}],
        })
        self.assertEqual(network_policy["egress"][-1], {
            "to": [{"ipBlock": {"cidr": "10.42.1.2/32"}}],
            "ports": [{"protocol": "TCP", "port": 9083}],
        })
        deploy = next(item for item in resources if item["kind"] == "Deployment")
        env = deploy["spec"]["template"]["spec"]["containers"][0]["env"]
        self.assertEqual(json.loads(next(e["value"] for e in env if e["name"] == "BROWSER_STORAGE_EGRESS")), addresses + domains)
        self.assertFalse(any(item["kind"] == "CiliumNetworkPolicy" for item in render()))
        with self.assertRaises(subprocess.CalledProcessError):
            render("--set-json", 'networkPolicy.storageEgressDomains=[{"hostname":"*.example.com","port":443}]')

    def test_storage_domain_rejects_cluster_local_hostnames(self):
        for hostname, cluster_domain in (
            ("minio.storage.svc.cluster.local", "cluster.local"),
            ("minio.storage.svc", "cluster.local"),
            ("10-42-1-2.storage.pod", "cluster.local"),
            ("minio.storage.svc.kube.internal", "kube.internal"),
            ("10-42-1-2.storage.pod.kube.internal", "kube.internal"),
        ):
            with self.subTest(hostname=hostname), self.assertRaises(subprocess.CalledProcessError):
                render("--set-string", "networkPolicy.clusterDomain=" + cluster_domain,
                       "--set-json", "networkPolicy.storageEgressDomains=" +
                       json.dumps([{"hostname": hostname, "port": 443}]))

    def test_storage_domain_allows_external_svc_and_pod_labels(self):
        hostnames = ["s3.prod.svc.example.com", "s3.prod.pod.example.com"]
        resources = render("--set-json", "networkPolicy.storageEgressDomains=" +
                           json.dumps([{"hostname": hostname, "port": 443} for hostname in hostnames]))
        policy = next(item["spec"] for item in resources if item["kind"] == "CiliumNetworkPolicy")
        self.assertEqual([rule["toFQDNs"][0]["matchName"] for rule in policy["egress"][1:]], hostnames)

    def test_image_reference_is_shared_by_controller_workers_and_drain(self):
        digest = "sha256:" + "a" * 64
        for settings, image in (
            ((), f"amaster/browser-sandbox@{digest}"),
            (("--set-string", "image.digest=,image.tag=v0.2.3"), "amaster/browser-sandbox:v0.2.3"),
            (("--set-string", "image.digest=,image.tag=sha-" + "b" * 40), "amaster/browser-sandbox:sha-" + "b" * 40),
            (("--set-string", "image.tag=v0.2.3"), f"amaster/browser-sandbox@{digest}"),
        ):
            with self.subTest(settings=settings):
                resources = render(*settings)
                workloads = {item["kind"]: item["spec"]["template"]["spec"]["containers"][0]
                             for item in resources if item["kind"] in ("Deployment", "Job")}
                self.assertEqual(workloads["Deployment"]["image"], image)
                self.assertEqual(workloads["Job"]["image"], image)
                env = {item["name"]: item.get("value") for item in workloads["Deployment"]["env"]}
                self.assertEqual(env["BROWSER_IMAGE"], image)

    def test_missing_and_floating_image_references_fail_at_render_time(self):
        for tag in ("", "latest", "sha-abc", "v0.2.3-rc.1"):
            with self.subTest(tag=tag), self.assertRaises(subprocess.CalledProcessError):
                render("--set-string", f"image.digest=,image.tag={tag}")

    def test_image_reference_requires_repository(self):
        for settings in (
            "image.repository=",
            "image.repository=,image.digest=,image.tag=v0.2.3",
            "image.repository= ,image.digest=,image.tag=v0.2.3",
        ):
            with self.subTest(settings=settings), self.assertRaises(subprocess.CalledProcessError):
                render("--set-string", settings)

    def test_active_active_matrix_uses_an_independent_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            values = pathlib.Path(directory) / "values.json"
            values.write_text(json.dumps({"controllerData": {"existingClaim": "trial-browser-data"}}))
            # Stop at the first Helm call, before the helper acquires cluster resources.
            subprocess.check_call([
                "node", "--input-type=module", "-e", """
                import { activeActive } from './.github/scripts/managed/active-active.mjs';
                await activeActive({
                    directory: process.argv[1], values: process.argv[2],
                    run: async () => { process.exit(0); },
                });
                """, directory, str(values),
            ], cwd=ROOT)
            resources = render("-f", str(pathlib.Path(directory) / "active-active-values.json"), release="ha")
            deployment = next(item for item in resources if item["kind"] == "Deployment")
            volumes = deployment["spec"]["template"]["spec"]["volumes"]
            journal = next(item for item in volumes if item["name"] == "controller-data")
            self.assertEqual(journal["persistentVolumeClaim"]["claimName"], "ha-browser-data")
            self.assertTrue(any(item["kind"] == "PersistentVolumeClaim" and item["metadata"]["name"] == "ha-browser-data" for item in resources))

    def test_invalid_release_names_fail_at_render_time(self):
        for release in ["1abc", "a.b", "a" * 49 + "--aa"]:
            with self.subTest(release=release):
                with self.assertRaises(subprocess.CalledProcessError):
                    render(release=release)
        for release in ["trial", "a" * 53]:
            with self.subTest(release=release):
                resources = render(release=release)
                self.assertTrue(any(item["kind"] == "Deployment" for item in resources))

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

    def test_v020_reused_values_default_to_a_single_replica(self):
        with tempfile.TemporaryDirectory() as directory:
            chart = pathlib.Path(directory) / "browser-sandbox"
            shutil.copytree(ROOT / "charts/browser-sandbox", chart)
            # Helm --reuse-values uses the previous chart's coalesced defaults.
            (chart / "values.yaml").write_bytes(subprocess.check_output([
                "git", "show", "v0.2.0:charts/browser-sandbox/values.yaml",
            ], cwd=ROOT))
            deployment = next(item for item in render(chart=chart) if item["kind"] == "Deployment")
            self.assertEqual(deployment["spec"]["replicas"], 1)
            with self.assertRaises(subprocess.CalledProcessError):
                render("--set", "controller.replicas=0", chart=chart)

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
        self.assertNotIn("ttlSecondsAfterFinished", hook["spec"])
        self.assertEqual(hook["metadata"]["annotations"]["helm.sh/hook-delete-policy"],
                         "before-hook-creation,hook-succeeded")
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

    def test_worker_seccomp_source_is_explicit(self):
        settings = "--set", "worker.securityContext.seccompProfile.type=Unconfined"
        configuration = next(item for item in render(*settings) if "worker-template.json" in item.get("data", {}))
        template = json.loads(configuration["data"]["worker-template.json"])
        self.assertEqual(template["securityContext"]["seccompProfile"]["type"], "Unconfined")
        for profile in ("Image", "RuntimeDefault"):
            with self.subTest(profile=profile), self.assertRaises(subprocess.CalledProcessError):
                render("--set", f"worker.securityContext.seccompProfile.type={profile}")

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

    def test_scaling_keeps_the_same_chart_managed_journal_claim(self):
        for retain in ("false", "true"):
            with self.subTest(retain=retain):
                settings = ("--set", "controller.sharedState=true", "--set", f"controllerData.retain={retain}")
                before = render(*settings)
                after = render(*settings, "--set", "controller.replicas=3")
                self.assertEqual(
                    [item for item in after if item["kind"] == "PersistentVolumeClaim"],
                    [item for item in before if item["kind"] == "PersistentVolumeClaim"],
                )
        for replicas in (1, 3):
            with self.subTest(replicas=replicas):
                resources = render("--set", f"controller.replicas={replicas}", "--set", "controllerData.existingClaim=retained")
                self.assertFalse(any(item["kind"] == "PersistentVolumeClaim" for item in resources))

    def test_shared_state_can_install_without_provisioning_a_journal_claim(self):
        skip_creation = ("--set", "controllerData.skipCreation=true")
        for settings in (("--set", "controller.sharedState=true"), ("--set", "controller.replicas=3")):
            with self.subTest(settings=settings):
                resources = render(*skip_creation, *settings)
                self.assertFalse(any(item["kind"] == "PersistentVolumeClaim" for item in resources))
                deployment = next(item for item in resources if item["kind"] == "Deployment")
                pod = deployment["spec"]["template"]["spec"]
                container = pod["containers"][0]
                self.assertNotIn("controller-data", {volume["name"] for volume in pod["volumes"]})
                self.assertNotIn("controller-data", {mount["name"] for mount in container["volumeMounts"]})
                env = {item["name"]: item.get("value") for item in container["env"]}
                self.assertEqual(env["BROWSER_SHARED_STATE"], "true")
                self.assertNotIn("BROWSER_CONTROLLER_STATE", env)
        with self.assertRaises(subprocess.CalledProcessError):
            render(*skip_creation)
        resources = render(*skip_creation, "--set", "controller.sharedState=true", "--set", "controllerData.existingClaim=retained")
        self.assertFalse(any(item["kind"] == "PersistentVolumeClaim" for item in resources))
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        pod = deployment["spec"]["template"]["spec"]
        self.assertIn({"name": "controller-data", "persistentVolumeClaim": {"claimName": "retained"}}, pod["volumes"])
        container = pod["containers"][0]
        self.assertIn({"name": "controller-data", "mountPath": "/controller-data", "readOnly": True}, container["volumeMounts"])
        self.assertIn({"name": "BROWSER_CONTROLLER_STATE", "value": "/controller-data/executions"}, container["env"])

    def test_active_active_has_shared_state_peer_isolation_and_no_rwo_mount(self):
        resources = render("--set", "controller.replicas=3")
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        self.assertEqual(deployment["spec"]["replicas"], 3)
        self.assertEqual(deployment["spec"]["strategy"]["type"], "RollingUpdate")
        pod = deployment["spec"]["template"]["spec"]
        self.assertIn("podAntiAffinity", pod["affinity"])
        self.assertFalse(any(volume.get("persistentVolumeClaim") for volume in pod["volumes"]))
        env = {item["name"]: item.get("value") for item in pod["containers"][0]["env"]}
        self.assertEqual(env["BROWSER_SHARED_STATE"], "true")
        self.assertEqual(env["BROWSER_CONTROLLER_REPLICAS"], "3")
        readiness = pod["containers"][0]["readinessProbe"]
        self.assertEqual(readiness["httpGet"], {"path": "/health", "port": "access"})
        self.assertLessEqual(readiness["periodSeconds"] * readiness["failureThreshold"], 10)
        self.assertNotIn("BROWSER_CONTROLLER_STATE", env)
        self.assertTrue(any(item["kind"] == "PodDisruptionBudget" and item["metadata"]["name"].endswith("-controller") for item in resources))
        policy = next(item["spec"] for item in resources if item["kind"] == "NetworkPolicy" and item["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "controller")
        peer = next(rule for rule in policy["ingress"] if rule["ports"][0]["port"] == 8084)
        self.assertEqual(peer["from"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/component"], "controller")
        self.assertFalse(any(8084 in [port["port"] for port in item["spec"]["ports"]] for item in resources if item["kind"] == "Service"))
        rules = next(item["rules"] for item in resources if item["kind"] == "Role" and not item["metadata"]["name"].endswith("-drain"))
        self.assertEqual(set(next(rule["verbs"] for rule in rules if "leases" in rule["resources"])), {"get", "list", "watch", "create", "update", "delete"})
        self.assertEqual(set(next(rule["verbs"] for rule in rules if "secrets" in rule["resources"])), {"get", "create", "update", "delete"})
        drain_rules = next(item["rules"] for item in resources if item["kind"] == "Role" and item["metadata"]["name"].endswith("-drain"))
        self.assertEqual(next(rule for rule in drain_rules if "leases" in rule["resources"]), {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["list"]})
        self.assertFalse(any("leases" in rule["resources"] for item in render() if item["kind"] == "Role" and item["metadata"]["name"].endswith("-drain") for rule in item["rules"]))
        with self.assertRaises(subprocess.CalledProcessError):
            render("--set", "controller.replicas=3", "--set", "profileStore.enabled=true")

    def test_single_replica_migration_keeps_source_read_only(self):
        resources = render("--set", "controller.sharedState=true")
        deployment = next(item for item in resources if item["kind"] == "Deployment")
        self.assertEqual(deployment["spec"]["strategy"]["type"], "Recreate")
        mounts = deployment["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
        self.assertTrue(next(mount for mount in mounts if mount["name"] == "controller-data")["readOnly"])


if __name__ == "__main__":
    unittest.main()
