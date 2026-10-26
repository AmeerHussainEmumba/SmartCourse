# deploy/

Configuration for the observability stack that `docker-compose.yml`'s `observability` profile mounts in — Grafana dashboards and Prometheus scrape config, provisioned as code rather than clicked together in a UI (RFC §12.3: "dashboards in Grafana provisioned as code in the repository").

## What's here (and what isn't, yet)

- **`grafana/`** — will hold dashboard JSON and datasource provisioning files, mounted at `/etc/grafana/provisioning` (see the `grafana` service in `docker-compose.yml`).
- **`prometheus/`** — will hold `prometheus.yml` scrape config, mounted at `/etc/prometheus` (see the `prometheus` service in `docker-compose.yml`).

Both directories are currently empty placeholders. Observability is **M4 scope** (`docs/rfc/smartcourse-rfc.md` §21.0/§21) — the metrics worth dashboarding (`outbox_backlog`, `kafka_consumer_lag`, RED per endpoint, etc., RFC §12.3) mostly don't exist yet because the systems that would emit them (the outbox relay, Kafka consumers) are M3 scope. The Compose service definitions already exist so the profile is ready to use the moment there's something to look at.

## Why this isn't dashboards-later-as-an-afterthought

The directories and Compose wiring exist now, from M0, on purpose — same reasoning as the outbox table existing before the relay does (§21.0). When M3/M4 land, dashboard and scrape config drop straight into these directories instead of requiring new infrastructure decisions at that point.
