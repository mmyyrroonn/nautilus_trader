# Protocol evidence fixtures

See `docs/plans/backpack-protocol-evidence.md` for facts, unknowns and native regression links.
`manifest.json` records SHA-256, source, UTC collection date, applicable API version and transformations.

- `official-schema`: selected unchanged embedded OpenAPI operations/schemas and stream descriptions.
- `official-example`: published sample values with comments removed; never relabeled as an account capture.
- `official-summary`: cited official product semantics, with collection provenance.
- `synthetic`: fictional account/test values and adverse controls; never observed private traffic.
- `public-observed`: bounded unsigned production market subscription, preserving original text frames.

`collection.json` includes the original API HTML SHA-256 and HTTP Date. `public_subscription.json`
records the exact public request, original response text, UTC receive times and finite capture bounds.
`public_mark_price.json` is one unmodified captured text payload with a final newline. The observation
contained no independent ACK in its eight frames; absence in a finite capture is not a venue guarantee.
Tests perform no network access and never open environment or credential files.
