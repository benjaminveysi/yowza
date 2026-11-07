# Daemon Deployments

## yowzad (Go REST Daemon)
For containerized deployments (Docker/Kubernetes), Yowza! provides `cmd/yowzad`. This is a lightweight Go web server that wraps the Yowza! engine and exposes an HTTP REST API.
- `POST /put`
- `GET /get`
- `DELETE /delete`
- `GET /scan`

All endpoints are protected by Ed25519 authentication. The daemon integrates seamlessly with the Cloud Syncer to back up its state to S3 or Redis asynchronously.

## yowzad_native (Pure C TCP Daemon)
For ultra-low resource environments (e.g., embedded IoT devices, routers), Yowza! includes a pure C implementation: `src/yowzad.c`.
This daemon compiles to a tiny binary (often under 50KB), uses raw POSIX sockets for a custom TCP binary protocol, and leverages the Native POSIX Storage Driver for disk persistence. It avoids the Go runtime entirely, resulting in virtually zero overhead.
