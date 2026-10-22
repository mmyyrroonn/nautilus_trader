# Arcus

Native Rust market data and execution adapter for Arcus perpetual futures, with
Python configuration and LiveNode factories.

See the [integration guide](../../../docs/integrations/arcus.md) for supported
operations, account setup, recovery behavior, and examples.

The protocol implementation follows the [official Arcus API](https://docs.arcus.xyz/).
It uses exact decimal prices and quantities, reusable Ed25519 signing keys,
bounded internal queues, and asynchronous REST and WebSocket transports.

Run the focused Rust tests with `cargo test -p nautilus-arcus` and the protocol
microbenchmarks with `cargo bench -p nautilus-arcus --bench protocol`.
