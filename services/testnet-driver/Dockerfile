# testnet-driver: synthetic order lifecycle driver for account=testnet (phase 09).
# Command (docker-compose.yml): python -m hdt.execution.testnet_driver. One-shot CLI (profile testnet-driver,
# exit 0 = every lifecycle passed): no metrics endpoint and no healthcheck. Own Ed25519 key accepted by
# execution for account=testnet only; egress only to the Binance testnet market data, through
# egress-proxy (deploy/squid/allowlist/testnet_driver.txt).
FROM base
LABEL org.opencontainers.image.title="hdt-testnet-driver"
HEALTHCHECK NONE
