# Unified production releases

Production runs in the `phenogram` namespace of the unified cluster. The
namespace-scoped `KUBECONFIG` secret includes a restricted SSH API transport;
the Kubernetes API remains private and certificate verification stays enabled.

The production workflow retains the Rust, JavaScript, image and chart checks.
It updates immutable images on the four existing application workloads. It
preserves their database credentials, claims, volumes, placement, TLS bindings
and other runtime configuration. PostgreSQL is checked but never modified by
the release credential. Helm templates remain available for validation; the
old complete installation and rollback path is no longer the production deploy
operation. Configuration and storage changes require a separate reviewed change.

Run the workflow manually with `preflight_only: true` to check the actual
restricted connection, admit the exact existing image updates without applying
them, and check the live application/database response. This does not publish
images or restart applications. Pushes to `master`, and manual runs with
`preflight_only: false`, retain the complete build and validation gates.

Every application write tests the freshly read workload UID, resource version
and previous image. Admission must preserve the rest of the migrated runtime
specification. A failed or uncertain rollout stops for inspection; it never
reapplies an old source-cluster manifest or rolls database contents backward.
