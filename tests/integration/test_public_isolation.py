"""Network isolation and hardening of the production stack (docker-compose.yml, phase 10).

Static rules (always run) read the compose file and deploy/ as written:
- the public dashboard shares networks only with caddy, public-store and the edge prober, and holds only
  the read-only store user, whose policy allows nothing but GetObject; the edge prober shares a network
  only with caddy, the dashboard and prometheus, holds nothing and listens only on its probe address;
- monitoring components that sit on a second network listen only on their fixed net_monitor address,
  and prometheus.yml addresses them there; redis-exporter has no /scrape endpoint; the egress probe
  hands its never-resolving target to the proxy instead of resolving it;
- the fetcher and both egress proxies share no network with Postgres, Redis, execution, Langfuse or
  Prometheus; the fetcher has no secrets and no volumes (no lake, no docker socket); only the council
  reaches the fetcher;
- only caddy publishes on every interface, and only 443; console, Grafana and Langfuse bind the
  Tailscale address; Postgres and Redis publish nothing;
- every network is internal except the ingress, egress and masquerade-free admin bridges;
- every container has a memory and a process limit; fixed addresses lie outside the dynamic range;
- egress-proxy client addresses in deploy/squid/squid.conf are the fixed addresses in the compose file,
  every allowlist matches the requested name only (`dstdomain -n`) and IP literals are denied first;
- the testnet driver is a one-shot CLI: own profile, never restarted, no healthcheck, no scrape job;
- every mounted secret file is produced by scripts/gen_prod_secrets.py or listed as operator-provided;
  the generator refuses an existing empty secret and never leaves a partial file.

The `docker` tests rebuild the resolved topology (`docker compose config`, all profiles) with stand-in
listeners on the real service names and ports, then connect from the dashboard's and the fetcher's
networks: Postgres, Redis, config-api, console, execution, Prometheus, the exporters, Alertmanager,
Langfuse and the internet must be unreachable, by name and by IP, while public-store / egress-fetch
(positive controls) answer. More run the real components: Squid with deploy/squid (IP literals
denied, allowlisted names admitted; backup endpoints Squid would deny refused at start), the blackbox
exporter probing that Squid (passes while Squid denies, fails once it is stopped), and public-store
prepared by deploy/public-store/init.sh (every overwritten or deleted snapshot version survives, and the
publisher's user cannot remove one).
"""

from __future__ import annotations

import importlib.util
import ipaddress
import json
import re
import secrets
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from hdt.public.store import AccessDeniedError, PublicStore, StoreConfig, StoreError

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.prod.example"
SQUID_DIR = ROOT / "deploy" / "squid"
SQUID_CONF = SQUID_DIR / "squid.conf"
PUBLIC_STORE_DIR = ROOT / "deploy" / "public-store"
READER_POLICY = PUBLIC_STORE_DIR / "reader-policy.json"
PROMETHEUS_CONF = ROOT / "deploy" / "prometheus" / "prometheus.yml"
ALERTS_CONF = ROOT / "deploy" / "prometheus" / "alerts.yml"
BLACKBOX_DIR = ROOT / "deploy" / "blackbox"
BLACKBOX_CONF = BLACKBOX_DIR / "blackbox.yml"
EGRESS_IMAGE = "hdt/egress-proxy:pytest"
PROFILES = ("council", "news", "scoring", "trading")
MONITORING = {"alertmanager", "blackbox", "postgres-exporter", "redis-exporter"}

TRADING_STORES = {"postgres", "redis"}
PUBLIC_SIDE_FORBIDDEN = {
    "postgres",
    "redis",
    "config-api",
    "console",
    "execution",
    "risk",
    "council",
    "telegram-bot",
    "prometheus",
    "langfuse-web",
    "egress-proxy",
    "public-publisher",
    *MONITORING,
}
FETCHER_FORBIDDEN = {
    "postgres",
    "redis",
    "execution",
    "risk",
    "config-api",
    "console",
    "prometheus",
    "langfuse-web",
    "langfuse-worker",
    "egress-proxy",
    "backup",
    *MONITORING,
}
NON_INTERNAL_NETWORKS = {"net_ingress", "net_egress", "net_admin"}


def _load_compose() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    return _load_compose()


def _networks(service: dict[str, Any]) -> set[str]:
    nets = service.get("networks") or {}
    return set(nets) if isinstance(nets, list | dict) else set()


def _peers(model: dict[str, Any], name: str) -> set[str]:
    """Services that share at least one network with `name`."""
    own = _networks(model["services"][name])
    return {other for other, spec in model["services"].items() if other != name and own & _networks(spec)}


def _secret_names(service: dict[str, Any]) -> set[str]:
    return {entry if isinstance(entry, str) else entry["source"] for entry in service.get("secrets") or []}


def _network_options(service: dict[str, Any]) -> dict[str, Any]:
    """`networks` as a mapping (the list form carries no per-network options)."""
    nets = service.get("networks") or {}
    return {name: {} for name in nets} if isinstance(nets, list) else dict(nets)


def _fixed_address(service: dict[str, Any], network: str) -> str:
    options = _network_options(service).get(network)
    assert isinstance(options, dict), f"no fixed address on {network}"
    assert "ipv4_address" in options, f"no fixed address on {network}"
    return str(options["ipv4_address"])


def _listen_address(service: dict[str, Any]) -> str:
    """Address of the service's `--web.listen-address` / `-web.listen-address` flag."""
    for part in service.get("command") or []:
        match = re.fullmatch(r"--?web\.listen-address=(\S+):\d+", str(part))
        if match:
            return match.group(1)
    raise AssertionError("no web.listen-address flag")


# ---------------------------------------------------------------------------------------- public path


def test_dashboard_reaches_only_caddy_and_public_store(compose: dict[str, Any]) -> None:
    dashboard = compose["services"]["dashboard"]
    assert _networks(dashboard) == {"net_edge", "net_store_read"}
    assert _peers(compose, "dashboard") == {"caddy", "public-store", "blackbox-edge"}
    assert not dashboard.get("ports")
    assert not dashboard.get("volumes")


def test_dashboard_holds_only_read_only_store_credentials(compose: dict[str, Any]) -> None:
    dashboard = compose["services"]["dashboard"]
    assert _secret_names(dashboard) == {"public_store_reader_access_key", "public_store_reader_secret_key"}
    env = dashboard["environment"]
    assert set(env) == {
        "HDT_PUBLIC_STORE_URL",
        "HDT_PUBLIC_STORE_BUCKET",
        "HDT_PUBLIC_STORE_ACCESS_KEY_FILE",
        "HDT_PUBLIC_STORE_SECRET_KEY_FILE",
    }
    assert env["HDT_PUBLIC_STORE_ACCESS_KEY_FILE"].endswith("/public_store_reader_access_key")
    assert env["HDT_PUBLIC_STORE_SECRET_KEY_FILE"].endswith("/public_store_reader_secret_key")
    # Only the publisher holds the write user.
    holders = {
        name
        for name, spec in compose["services"].items()
        if "public_store_writer_secret_key" in _secret_names(spec)
    }
    assert holders == {"public-publisher", "public-store-init"}


def test_reader_policy_allows_only_object_reads() -> None:
    policy = json.loads(READER_POLICY.read_text(encoding="utf-8"))
    actions = {action for statement in policy["Statement"] for action in statement["Action"]}
    effects = {statement["Effect"] for statement in policy["Statement"]}
    assert actions == {"s3:GetObject"}
    assert effects == {"Allow"}


def test_public_side_never_touches_trading_networks(compose: dict[str, Any]) -> None:
    assert _peers(compose, "caddy") == {"dashboard", "blackbox-edge"}
    # The publisher is the only trading-side service next to the store, and it only writes there.
    assert _peers(compose, "public-store") == {"dashboard", "public-publisher", "public-store-init"}
    for name in ("dashboard", "caddy"):
        assert not _peers(compose, name) & PUBLIC_SIDE_FORBIDDEN, name


def test_edge_prober_is_confined(compose: dict[str, Any]) -> None:
    prober = compose["services"]["blackbox-edge"]
    assert _networks(prober) == {"net_edge", "net_probe_edge"}
    assert _peers(compose, "blackbox-edge") == {"caddy", "dashboard", "prometheus"}
    assert {n for n, s in compose["services"].items() if "net_probe_edge" in _networks(s)} == {
        "blackbox-edge",
        "prometheus",
    }
    assert not prober.get("secrets")
    assert not prober.get("ports")
    # It listens on its probe address only: nothing on net_edge can make it send a request.
    assert _listen_address(prober) == _fixed_address(prober, "net_probe_edge")


# ------------------------------------------------------------------------------------------ monitoring


def test_monitoring_components_listen_only_on_net_monitor(compose: dict[str, Any]) -> None:
    for name in MONITORING:
        spec = compose["services"][name]
        assert "net_monitor" in _networks(spec), name
        assert len(_networks(spec)) > 1, name
        assert _listen_address(spec) == _fixed_address(spec, "net_monitor"), name
        assert not spec.get("ports"), name
    # Its /scrape?target= endpoint would let anything on net_monitor aim it at another host (review N16).
    assert "-disable-scrape-endpoint" in compose["services"]["redis-exporter"]["command"]


def test_prometheus_addresses_match_the_fixed_listeners(compose: dict[str, Any]) -> None:
    fixed = {
        str(options["ipv4_address"]): name
        for name, spec in compose["services"].items()
        for options in _network_options(spec).values()
        if isinstance(options, dict) and "ipv4_address" in options
    }
    text = PROMETHEUS_CONF.read_text(encoding="utf-8")
    used = set(re.findall(r"\b(10\.231\.\d+\.\d+):\d+", text))
    assert used, "prometheus.yml addresses no fixed listener"
    for address in used:
        assert address in fixed, f"{address} is not a fixed service address"
        spec = compose["services"][fixed[address]]
        assert _listen_address(spec) == address, fixed[address]
        assert _networks(spec) & _networks(compose["services"]["prometheus"]), fixed[address]


def _scrape_jobs() -> dict[str, dict[str, Any]]:
    scrape = yaml.safe_load(PROMETHEUS_CONF.read_text(encoding="utf-8"))["scrape_configs"]
    return {job["job_name"]: job for job in scrape}


def test_egress_probe_hands_its_unresolvable_target_to_the_proxy() -> None:
    """Review N3: blackbox_exporter resolves a probe target before it uses `proxy_url` unless the module
    skips that phase, and the egress probe's target never resolves, so the probe could never pass."""
    module = yaml.safe_load(BLACKBOX_CONF.read_text(encoding="utf-8"))["modules"]["egress_denied"]["http"]
    assert module["proxy_url"] == "http://egress-proxy:3128"
    assert module["skip_resolve_phase_with_proxy"] is True
    (config,) = _scrape_jobs()["probe-egress"]["static_configs"]
    assert config["targets"] == ["http://egress-denied.invalid/"]


def test_testnet_driver_is_a_one_shot_cli(compose: dict[str, Any]) -> None:
    driver = compose["services"]["testnet-driver"]
    assert driver["profiles"] == ["testnet-driver"]
    assert driver["restart"] == "no"
    assert driver["healthcheck"] == {"disable": True}
    assert "testnet-driver" not in _scrape_jobs()
    assert "testnet-driver" not in ALERTS_CONF.read_text(encoding="utf-8")


# ------------------------------------------------------------------------------------------ hardening


def test_every_container_is_capped(compose: dict[str, Any]) -> None:
    for name, spec in compose["services"].items():
        if spec.get("scale") == 0:
            continue  # build-only image
        assert spec.get("mem_limit"), f"{name} has no mem_limit"
        assert isinstance(spec.get("pids_limit"), int), f"{name} has no pids_limit"
        assert spec["pids_limit"] > 0, f"{name} has no pids_limit"


def test_fixed_addresses_stay_outside_the_dynamic_range(compose: dict[str, Any]) -> None:
    with_fixed = set()
    for name, spec in compose["services"].items():
        for network, options in _network_options(spec).items():
            if not (isinstance(options, dict) and "ipv4_address" in options):
                continue
            with_fixed.add(network)
            (pool,) = compose["networks"][network]["ipam"]["config"]
            address = ipaddress.ip_address(options["ipv4_address"])
            assert address in ipaddress.ip_network(pool["subnet"]), f"{name} on {network}"
            assert address not in ipaddress.ip_network(pool["ip_range"]), f"{name} on {network}"
    assert {"net_proxy", "net_fetch", "net_console", "net_monitor", "net_probe_edge"} <= with_fixed


# ------------------------------------------------------------------------------------ fetch / egress


def test_fetcher_is_sandboxed(compose: dict[str, Any]) -> None:
    fetcher = compose["services"]["fetcher"]
    assert _networks(fetcher) == {"net_fetch"}
    # Design contract section 10: net_fetch holds the council and the fetcher only (plus its proxy).
    assert _peers(compose, "fetcher") == {"council", "egress-fetch"}
    assert not _peers(compose, "fetcher") & FETCHER_FORBIDDEN
    assert not fetcher.get("secrets")
    assert not fetcher.get("volumes")
    assert not fetcher.get("ports")


@pytest.mark.parametrize("proxy", ["egress-proxy", "egress-fetch"])
def test_egress_proxies_cannot_reach_trading_stores(compose: dict[str, Any], proxy: str) -> None:
    assert not _peers(compose, proxy) & TRADING_STORES
    assert not compose["services"][proxy].get("secrets")


def test_only_execution_holds_the_exchange_scope(compose: dict[str, Any]) -> None:
    holders = {
        name
        for name, spec in compose["services"].items()
        if "scope_private_key.execution.json" in _secret_names(spec)
    }
    assert holders == {"execution"}


def test_no_service_mounts_the_docker_socket(compose: dict[str, Any]) -> None:
    for name, spec in compose["services"].items():
        for volume in spec.get("volumes") or []:
            source = volume if isinstance(volume, str) else volume.get("source", "")
            assert "docker.sock" not in str(source), name


def test_squid_matches_requested_names_and_denies_ip_literals_first() -> None:
    text = SQUID_CONF.read_text(encoding="utf-8")
    domain_acls = re.findall(r"^acl \S+ dstdomain .*$", text, re.MULTILINE)
    assert domain_acls
    for line in domain_acls:
        # Without -n Squid also matches the PTR name of an IP destination, which its owner controls.
        assert line.split()[3] == "-n", line
    access = re.findall(r"^http_access (allow|deny) (.*)$", text, re.MULTILINE)
    first_allow = next(i for i, (action, _) in enumerate(access) if action == "allow")
    assert ("deny", "to_ip_literal") in access[:first_allow]
    (literal,) = re.findall(r"^acl to_ip_literal dstdom_regex -n (.*)$", text, re.MULTILINE)
    patterns = [re.compile(pattern) for pattern in literal.split()]
    for host in ("203.0.113.7", "3405803783", "0xcb.0x0.0x71.0x7", "2001:db8::1", "[2001:db8::1]"):
        assert any(p.search(host) for p in patterns), host
    for host in ("fapi.binance.com", "pro-api.coinmarketcap.com", "api.telegram.org", "hc-ping.com"):
        assert not any(p.search(host) for p in patterns), host


def test_squid_clients_match_fixed_addresses(compose: dict[str, Any]) -> None:
    acl = re.compile(r"^acl client_(\w+) src (\S+)$", re.MULTILINE)
    clients = dict(acl.findall(SQUID_CONF.read_text(encoding="utf-8")))
    service_of = {"telegram": "telegram-bot"}
    assert clients, "no client ACLs found"
    for client, address in clients.items():
        name = service_of.get(client, client.replace("_", "-"))
        spec = compose["services"][name]
        fixed = {
            str(options["ipv4_address"])
            for options in (spec.get("networks") or {}).values()
            if isinstance(options, dict) and "ipv4_address" in options
        }
        assert address in fixed, f"{client}: {address} not in {fixed}"
    proxied = {
        name
        for name, spec in compose["services"].items()
        if "net_proxy" in _networks(spec) and name != "egress-proxy"
    }
    assert proxied == {service_of.get(c, c.replace("_", "-")) for c in clients} - {"fetcher"}


# ---------------------------------------------------------------------------------------- published ports


def test_only_caddy_is_public_and_only_on_443(compose: dict[str, Any]) -> None:
    for name, spec in compose["services"].items():
        for port in spec.get("ports") or []:
            text = str(port)
            if name == "caddy":
                assert text == "443:8443"
            else:
                assert text.startswith("${HDT_TAILSCALE_IP"), f"{name} publishes {text} outside Tailscale"
    published = {name for name, spec in compose["services"].items() if spec.get("ports")}
    assert published == {"caddy", "console", "grafana", "langfuse-web"}


def test_networks_are_internal_except_the_edges(compose: dict[str, Any]) -> None:
    for name, spec in compose["networks"].items():
        internal = bool((spec or {}).get("internal"))
        assert internal is (name not in NON_INTERNAL_NETWORKS), name
    admin = compose["networks"]["net_admin"]["driver_opts"]
    assert admin["com.docker.network.bridge.enable_ip_masquerade"] == "false"
    on_ingress = {n for n, s in compose["services"].items() if "net_ingress" in _networks(s)}
    on_egress = {n for n, s in compose["services"].items() if "net_egress" in _networks(s)}
    assert on_ingress == {"caddy"}
    assert on_egress == {"egress-proxy", "egress-fetch"}


# ----------------------------------------------------------------------------------------------- secrets


def _load_script(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_secret_file_has_a_producer(compose: dict[str, Any]) -> None:
    generator = _load_script("gen_prod_secrets")
    known = generator.generated_names() | set(generator.OPERATOR_FILES)
    files = {Path(spec["file"]).name for spec in compose["secrets"].values()}
    assert files - known == set()
    used = set().union(*(_secret_names(spec) for spec in compose["services"].values()))
    assert used == set(compose["secrets"])


def test_secret_generator_refuses_an_empty_secret_file(tmp_path: Path) -> None:
    generator = _load_script("gen_prod_secrets")
    (tmp_path / "pg_password.council").write_text("", encoding="utf-8")
    with pytest.raises(SystemExit, match=re.escape("pg_password.council exists but is empty")):
        generator.main(["--dir", str(tmp_path)])
    # Never silently replaced: the operator decides between restoring the value and rotating it.
    assert (tmp_path / "pg_password.council").read_text(encoding="utf-8") == ""


def test_secret_generator_writes_complete_files_and_keeps_values(tmp_path: Path) -> None:
    generator = _load_script("gen_prod_secrets")
    (tmp_path / "pg_password.council").write_text("kept-council-password\n", encoding="utf-8")
    assert generator.main(["--dir", str(tmp_path)]) == 1  # operator-provided files are missing
    # Exactly the generated files: no temporary file of the atomic writes is left behind.
    assert {path.name for path in tmp_path.iterdir()} == generator.generated_names()
    assert "hdt_council:kept-council-password@" in (tmp_path / "pg_dsn.council").read_text(encoding="utf-8")
    # redis-exporter looks its password up under redis://<user>@<host:port> (it sends no password otherwise).
    monitor = (tmp_path / "redis_password.monitor").read_text(encoding="utf-8").strip()
    exporter_map = json.loads((tmp_path / "redis_password.monitor.json").read_text(encoding="utf-8"))
    assert exporter_map == {"redis://monitor@redis:6379": monitor}
    assert f"user monitor on >{monitor} " in (tmp_path / "redis_users.acl").read_text(encoding="utf-8")
    redis_exporter = _load_compose()["services"]["redis-exporter"]["command"]
    assert "-redis.addr=redis://redis:6379" in redis_exporter
    assert "-redis.user=monitor" in redis_exporter


# ------------------------------------------------------------------------------------------- live test


def _docker(*args: str, check: bool = True, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed docker CLI, arguments built by the test
        ["docker", *args],  # noqa: S607 - docker from PATH, as an operator runs it
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
        cwd=ROOT,
    )


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return _docker("info", "--format", "{{.ServerVersion}}", check=False, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _probe_image() -> str:
    """The pinned Python image of services/base/Dockerfile (has python for listeners and probes)."""
    text = (ROOT / "services" / "base" / "Dockerfile").read_text(encoding="utf-8")
    match = re.search(r"^ARG PYTHON_IMAGE=(\S+)$", text, re.MULTILINE)
    assert match is not None
    return match.group(1)


LISTENER = (
    "import socket, sys\n"
    "s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
    "s.bind(('0.0.0.0', int(sys.argv[1]))); s.listen(16)\n"
    "while True:\n"
    "    c, _ = s.accept(); c.close()\n"
)
PROBE = (
    "import json, socket, sys\n"
    "from concurrent.futures import ThreadPoolExecutor\n"
    "def reach(target):\n"
    "    host, port = target.rsplit(':', 1)\n"
    "    try:\n"
    "        socket.create_connection((host, int(port)), timeout=3).close()\n"
    "        return True\n"
    "    except OSError:\n"
    "        return False\n"
    "with ThreadPoolExecutor(max_workers=32) as pool:\n"
    "    print(json.dumps(dict(zip(sys.argv[1:], pool.map(reach, sys.argv[1:])))))\n"
)
LISTEN_PORTS = {
    "postgres": 5432,
    "redis": 6379,
    "postgres-exporter": 9187,
    "redis-exporter": 9121,
    "alertmanager": 9093,
    "blackbox": 9115,
    "config-api": 8081,
    "console": 8501,
    "execution": 9464,
    "prometheus": 9090,
    "langfuse-web": 3000,
    "egress-proxy": 3128,
    "egress-fetch": 3128,
    "public-store": 9000,
    "public-publisher": 9464,
    "telegram-bot": 9464,
}
INTERNET = "1.1.1.1:443"


class Topology:
    """Networks of the resolved compose model with one stand-in container per service."""

    def __init__(self, model: dict[str, Any], prefix: str, image: str) -> None:
        self.model = model
        self.prefix = prefix
        self.image = image
        self.networks: list[str] = []
        self.containers: list[str] = []
        self.addresses: dict[str, list[str]] = {}

    def net(self, name: str) -> str:
        return f"{self.prefix}_{name}"

    def create_networks(self) -> None:
        for name, spec in self.model["networks"].items():
            args = ["network", "create"]
            if (spec or {}).get("internal"):
                args.append("--internal")
            for key, value in ((spec or {}).get("driver_opts") or {}).items():
                args += ["--opt", f"{key}={value}"]
            _docker(*args, self.net(name))
            self.networks.append(self.net(name))

    def _attach(self, container: str, service: str, first: str) -> None:
        for network in sorted(_networks(self.model["services"][service]) - {first}):
            _docker("network", "connect", "--alias", service, self.net(network), container)

    def listener(self, service: str, port: int) -> None:
        nets = sorted(_networks(self.model["services"][service]))
        container = f"{self.prefix}_{service}"
        _docker(
            "run", "-d", "--name", container, "--network", self.net(nets[0]), "--network-alias", service,
            "--entrypoint", "python", self.image, "-c", LISTENER, str(port),
        )  # fmt: skip
        self.containers.append(container)
        self._attach(container, service, nets[0])
        inspect = json.loads(_docker("inspect", container).stdout)[0]
        self.addresses[service] = [
            f"{net['IPAddress']}:{port}" for net in inspect["NetworkSettings"]["Networks"].values()
        ]

    def probe(self, service: str, targets: list[str]) -> dict[str, bool]:
        nets = sorted(_networks(self.model["services"][service]))
        container = f"{self.prefix}_probe_{service}"
        _docker(
            "create", "--name", container, "--network", self.net(nets[0]),
            "--entrypoint", "python", self.image, "-c", PROBE, *targets,
        )  # fmt: skip
        self.containers.append(container)
        for network in nets[1:]:
            _docker("network", "connect", self.net(network), container)
        _docker("start", "--attach", container, timeout=300)
        logs = _docker("logs", container).stdout.strip().splitlines()
        result: dict[str, bool] = json.loads(logs[-1])
        return result

    def cleanup(self) -> None:
        if self.containers:
            _docker("rm", "-f", *self.containers, check=False)
        for network in self.networks:
            _docker("network", "rm", network, check=False)


@pytest.fixture(scope="module")
def topology() -> Iterator[Topology]:
    if not _docker_available():
        pytest.skip("docker is not available")
    profiles = [arg for profile in PROFILES for arg in ("--profile", profile)]
    resolved = _docker(
        "compose", "-f", str(COMPOSE), "--env-file", str(ENV_EXAMPLE), *profiles, "config", "--format", "json"
    )
    model = json.loads(resolved.stdout)
    image = _probe_image()
    _docker("pull", "-q", image, timeout=600)
    topo = Topology(model, f"hdtiso{secrets.token_hex(3)}", image)
    try:
        topo.create_networks()
        for service, port in LISTEN_PORTS.items():
            topo.listener(service, port)
        yield topo
    finally:
        topo.cleanup()


def _targets(topo: Topology, services: set[str]) -> list[str]:
    names = [f"{service}:{LISTEN_PORTS[service]}" for service in sorted(services)]
    ips = [address for service in sorted(services) for address in topo.addresses[service]]
    return [*names, *ips, INTERNET]


@pytest.mark.docker
@pytest.mark.integration
def test_dashboard_container_reaches_only_public_store(topology: Topology) -> None:
    forbidden = set(LISTEN_PORTS) - {"public-store", "egress-fetch"}
    result = topology.probe("dashboard", ["public-store:9000", *_targets(topology, forbidden)])
    assert result.pop("public-store:9000") is True, "positive control failed: public-store unreachable"
    reachable = sorted(target for target, ok in result.items() if ok)
    assert reachable == [], f"dashboard reaches {reachable}"


@pytest.mark.docker
@pytest.mark.integration
def test_fetcher_container_reaches_only_its_proxy(topology: Topology) -> None:
    forbidden = set(LISTEN_PORTS) - {"egress-fetch"}
    result = topology.probe("fetcher", ["egress-fetch:3128", *_targets(topology, forbidden)])
    assert result.pop("egress-fetch:3128") is True, "positive control failed: egress-fetch unreachable"
    reachable = sorted(target for target, ok in result.items() if ok)
    assert reachable == [], f"fetcher reaches {reachable}"


# ------------------------------------------------------------------------------ live proxy and store

SQUID_CLIENT = (
    "import json, socket, sys\n"
    "proxy, targets = sys.argv[1], sys.argv[2:]\n"
    "status = {}\n"
    "for target in targets:\n"
    "    s = socket.create_connection((proxy, 3128), timeout=20)\n"
    "    s.sendall(f'CONNECT {target} HTTP/1.1\\r\\nHost: {target}\\r\\n\\r\\n'.encode())\n"
    "    status[target] = int(s.recv(200).split(b' ')[1])\n"
    "    s.close()\n"
    "print(json.dumps(status))\n"
)
TEST_NET_3 = "203.0.113.0/24"  # RFC 5737: public (not in to_private), never routed


def _create_network(name: str, *args: str) -> None:
    created = _docker("network", "create", *args, name, check=False)
    if created.returncode != 0:
        pytest.skip(f"cannot create network {name} ({created.stderr.strip()}); is the stack running here?")


def _egress_image() -> str:
    """The egress-proxy image (services/egress-proxy, Squid) built from this checkout."""
    if not _docker_available():
        pytest.skip("docker is not available")
    _docker("build", "-q", "-f", "services/egress-proxy/Dockerfile", "-t", EGRESS_IMAGE, ".", timeout=1200)
    return EGRESS_IMAGE


def _wait_squid_listening(container: str) -> None:
    deadline = time.monotonic() + 60
    listening = ["exec", container, "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/3128"]
    while _docker(*listening, check=False).returncode:
        assert time.monotonic() < deadline, _docker("logs", container, check=False).stderr[-2000:]
        time.sleep(1)


@pytest.mark.docker
@pytest.mark.integration
def test_squid_refuses_an_ip_literal_whose_reverse_name_is_allowlisted(compose: dict[str, Any]) -> None:
    """The fetcher's Squid (real image, deploy/squid) against a server at 203.0.113.7, a public address.

    Docker's DNS gives that address the PTR name `<container>.<prefix>.coindesk.com`, and the fetcher
    allowlist holds `.coindesk.com`: before `dstdomain -n` and the IP-literal deny, Squid matched the PTR
    name and tunnelled `CONNECT 203.0.113.7:443` (review I5). The same server stays reachable by an
    allowlisted name (positive control) and refused by a name outside the allowlist.
    """
    image = _egress_image()
    probe_image = _probe_image()
    (fetch_pool,) = compose["networks"]["net_fetch"]["ipam"]["config"]
    fetcher_ip = _fixed_address(compose["services"]["fetcher"], "net_fetch")
    prefix = f"hdtptr{secrets.token_hex(3)}"
    fetch_net, reverse_net = f"{prefix}_fetch", f"{prefix}.coindesk.com"
    containers = [f"{prefix}_target", f"{prefix}_squid"]
    allowed, other = f"feed.{prefix}.coindesk.com", f"feed.{prefix}.example.org"
    try:
        _create_network(
            fetch_net, "--internal", "--subnet", fetch_pool["subnet"], "--ip-range", fetch_pool["ip_range"]
        )
        _create_network(reverse_net, "--internal", "--subnet", TEST_NET_3)
        _docker(
            "run", "-d", "--name", containers[0], "--network", reverse_net, "--ip", "203.0.113.7",
            "--network-alias", allowed, "--network-alias", other,
            "--entrypoint", "python", probe_image, "-c", LISTENER, "443",
        )  # fmt: skip
        _docker(
            "run", "-d", "--name", containers[1], "--network", fetch_net,
            "-v", f"{SQUID_DIR}:/etc/hdt-squid:ro", "--tmpfs", "/run/hdt-squid:uid=13,gid=13", image,
        )  # fmt: skip
        _docker("network", "connect", reverse_net, containers[1])
        reverse = _docker("exec", containers[1], "getent", "hosts", "203.0.113.7").stdout
        assert reverse.split()[1].endswith(".coindesk.com"), reverse  # the premise of the attack holds
        inspect = json.loads(_docker("inspect", containers[1]).stdout)[0]
        proxy = inspect["NetworkSettings"]["Networks"][fetch_net]["IPAddress"]
        _wait_squid_listening(containers[1])
        targets = ["203.0.113.7:443", "3405803783:443", f"{allowed}:443", f"{other}:443"]
        client = _docker(
            "run", "--rm", "--network", fetch_net, "--ip", fetcher_ip,
            "--entrypoint", "python", probe_image, "-c", SQUID_CLIENT, proxy, *targets,
        )  # fmt: skip
        status = json.loads(client.stdout.strip().splitlines()[-1])
        assert status == {
            "203.0.113.7:443": 403,
            "3405803783:443": 403,  # the same address as one decimal number
            f"{allowed}:443": 200,
            f"{other}:443": 403,
        }
    finally:
        _docker("rm", "-f", *containers, check=False)
        _docker("network", "rm", fetch_net, reverse_net, check=False)


@pytest.mark.docker
@pytest.mark.integration
def test_egress_probe_passes_against_squid_and_fails_without_it(compose: dict[str, Any]) -> None:
    """Review N3 with the real components on the fixed net_proxy addresses: the pinned blackbox exporter
    with deploy/blackbox probes the real Squid (deploy/squid) with the `egress_denied` module and the
    target of prometheus.yml. Squid answers 403, so the probe passes; with Squid stopped it fails, so a
    real proxy outage still pages. The proxy also renders the backup allowlist from a valid endpoint."""
    image = _egress_image()
    prober_image = str(compose["services"]["blackbox"]["image"])
    (pool,) = compose["networks"]["net_proxy"]["ipam"]["config"]
    squid_ip = _fixed_address(compose["services"]["egress-proxy"], "net_proxy")
    prober_ip = _fixed_address(compose["services"]["blackbox"], "net_proxy")
    (target,) = _scrape_jobs()["probe-egress"]["static_configs"][0]["targets"]
    prefix = f"hdtprobe{secrets.token_hex(3)}"
    squid, prober = f"{prefix}_squid", f"{prefix}_blackbox"
    try:
        _create_network(prefix, "--internal", "--subnet", pool["subnet"], "--ip-range", pool["ip_range"])
        _docker(
            "run", "-d", "--name", squid, "--network", prefix, "--ip", squid_ip,
            "--network-alias", "egress-proxy",
            "-e", "HDT_BACKUP_S3_ENDPOINT=https://s3.ap-southeast-1.amazonaws.com:443",
            "-v", f"{SQUID_DIR}:/etc/hdt-squid:ro", "--tmpfs", "/run/hdt-squid:uid=13,gid=13", image,
        )  # fmt: skip
        _docker(
            "run", "-d", "--name", prober, "--network", prefix, "--ip", prober_ip,
            "-v", f"{BLACKBOX_DIR}:/etc/hdt-blackbox:ro", prober_image,
            "--config.file=/etc/hdt-blackbox/blackbox.yml", "--web.listen-address=127.0.0.1:9115",
        )  # fmt: skip
        _wait_squid_listening(squid)
        backup_list = _docker("exec", squid, "cat", "/run/hdt-squid/backup.txt").stdout
        assert backup_list.split() == ["s3.ap-southeast-1.amazonaws.com"]

        url = f"http://127.0.0.1:9115/probe?module=egress_denied&target={target}"

        def probe_success() -> str:
            deadline = time.monotonic() + 30
            while True:
                result = _docker("exec", prober, "wget", "-q", "-O", "-", url, check=False, timeout=60)
                match = re.search(r"^probe_success (\d)$", result.stdout, re.MULTILINE)
                if match:
                    return match.group(1)
                assert time.monotonic() < deadline, result.stderr or _docker("logs", prober).stderr[-2000:]
                time.sleep(1)

        assert probe_success() == "1"
        _docker("stop", squid)
        assert probe_success() == "0"
    finally:
        _docker("rm", "-f", squid, prober, check=False)
        _docker("network", "rm", prefix, check=False)


@pytest.mark.docker
@pytest.mark.integration
@pytest.mark.parametrize(
    "endpoint",
    [
        "https://203.0.113.7",
        "https://3405803783",  # one decimal number
        "https://0xcb.0x0.0x71.0x7",
        "https://[2001:db8::1]",
        "https://s3.example.com:8443",
        "http://s3.example.com",
        "https://s3.example.com/bucket",
    ],
)
def test_egress_proxy_refuses_a_backup_endpoint_squid_would_deny(endpoint: str) -> None:
    """Review N15: squid.conf denies IP literals and every port but 443, so such an endpoint would only
    surface hours later as a stale backup; the proxy refuses to start instead."""
    result = _docker(
        "run", "--rm", "-e", f"HDT_BACKUP_S3_ENDPOINT={endpoint}",
        "-v", f"{SQUID_DIR}:/etc/hdt-squid:ro", "--tmpfs", "/run/hdt-squid:uid=13,gid=13", _egress_image(),
        check=False, timeout=60,
    )  # fmt: skip
    assert result.returncode == 1, result.stderr
    assert "HDT_BACKUP_S3_ENDPOINT must be https://<DNS name>[:443]" in result.stderr


def _mc_versions(container: str, path: str) -> list[dict[str, Any]]:
    listing = _docker("exec", container, "mc", "--json", "ls", "--versions", "--recursive", path).stdout
    return [json.loads(line) for line in listing.splitlines() if line.strip()]


@pytest.mark.docker
@pytest.mark.integration
def test_public_store_keeps_every_published_version(compose: dict[str, Any], tmp_path: Path) -> None:
    """public-store prepared by deploy/public-store/init.sh, written through PublicStore with the
    publisher's credentials as public-publisher does (review M10): overwrites and deletes keep the
    earlier versions, which neither the publisher's key nor an unprivileged root call can remove."""
    if not _docker_available():
        pytest.skip("docker is not available")
    image = str(compose["services"]["public-store"]["image"])
    prefix = f"hdtpub{secrets.token_hex(3)}"
    creds = {
        "public_store_root_user": "hdtroot",
        "public_store_root_password": secrets.token_urlsafe(24),
        "public_store_writer_access_key": "hdtwriter",
        "public_store_writer_secret_key": secrets.token_urlsafe(24),
        "public_store_reader_access_key": "hdtreader",
        "public_store_reader_secret_key": secrets.token_urlsafe(24),
    }
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    for name, value in creds.items():
        (secret_dir / name).write_text(value, encoding="utf-8")
    init = [
        "run", "--rm", "--network", prefix, "-v", f"{secret_dir}:/run/secrets:ro",
        "-v", f"{PUBLIC_STORE_DIR}:/etc/hdt-public-store:ro", "--entrypoint", "sh", image,
        "/etc/hdt-public-store/init.sh",
    ]  # fmt: skip
    root = ["exec", prefix, "mc"]
    try:
        _docker("network", "create", prefix)
        _docker(
            "run", "-d", "--name", prefix, "--network", prefix, "--network-alias", "public-store",
            "-p", "127.0.0.1::9000", "-e", f"MINIO_ROOT_USER={creds['public_store_root_user']}",
            "-e", f"MINIO_ROOT_PASSWORD={creds['public_store_root_password']}", image, "server", "/data",
        )  # fmt: skip
        deadline = time.monotonic() + 60
        while _docker("exec", prefix, "mc", "ready", "local", check=False).returncode != 0:
            assert time.monotonic() < deadline, "MinIO did not start"
            time.sleep(1)
        _docker(*root, "alias", "set", "root", "http://127.0.0.1:9000",
                creds["public_store_root_user"], creds["public_store_root_password"])  # fmt: skip

        # A bucket made before object lock was required cannot get it: init stops and names the fix.
        _docker(*root, "mb", "root/hdt-public")
        refused = _docker(*init, check=False)
        assert refused.returncode == 1
        assert "has no object lock" in refused.stderr
        _docker(*root, "rb", "--force", "root/hdt-public")
        for _ in range(2):  # idempotent: runs on every `docker compose up`
            _docker(*init)
        _docker(*root, "alias", "set", "writer", "http://127.0.0.1:9000",
                creds["public_store_writer_access_key"], creds["public_store_writer_secret_key"])  # fmt: skip

        host_port = _docker("port", prefix, "9000/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1]
        endpoint = f"http://127.0.0.1:{host_port}"
        writer = PublicStore(
            StoreConfig(endpoint, "hdt-public", creds["public_store_writer_access_key"],
                        creds["public_store_writer_secret_key"])
        )  # fmt: skip
        reader = PublicStore(
            StoreConfig(endpoint, "hdt-public", creds["public_store_reader_access_key"],
                        creds["public_store_reader_secret_key"])
        )  # fmt: skip
        with writer, reader:
            writer.put("latest.json", b'{"v": 1}')
            writer.put("latest.json", b'{"v": 2}')
            writer.put("snapshots/1.json", b'{"s": 1}')
            writer.delete("snapshots/1.json")
            assert writer.list("snapshots/") == []
            assert reader.get("latest.json") == b'{"v": 2}'
            with pytest.raises(AccessDeniedError):
                reader.put("latest.json", b'{"v": "forged"}')

        versions = _mc_versions(prefix, "root/hdt-public")
        latest = [v for v in versions if v["key"] == "latest.json"]
        snapshot = [v for v in versions if v["key"] == "snapshots/1.json"]
        assert len(latest) == 2
        assert sorted(bool(v.get("isDeleteMarker")) for v in snapshot) == [False, True]
        oldest = next(v["versionId"] for v in latest if not v.get("isLatest"))
        for who, extra in (("writer", []), ("writer", ["--bypass"]), ("root", [])):
            removed = _docker(
                *root, "rm", "--version-id", oldest, *extra, f"{who}/hdt-public/latest.json", check=False
            )
            assert removed.returncode != 0, (who, extra, removed.stdout)
        assert len([v for v in _mc_versions(prefix, "root/hdt-public") if v["key"] == "latest.json"]) == 2
    except StoreError as exc:
        pytest.fail(f"publisher call refused by the prepared bucket: {exc}")
    finally:
        _docker("rm", "-f", prefix, check=False)
        _docker("network", "rm", prefix, check=False)
