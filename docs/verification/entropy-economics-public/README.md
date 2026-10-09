# Entropy Economic Protocol Sources

This archive preserves fifteen original public captures used for native issue 102: official
Hyperliquid documentation, the official Python SDK and the requested issue. It contains no real
private account observation or venue action. The SDK revision is
`2fdb18f9517675ea03695a0962bd19eece9c83f0`.

[manifest.json](manifest.json) maps original capture names to the immutable `.txt` data files.
It preserves the original URLs, UTC capture intervals, byte counts and SHA256 values. The original
[provenance.json](provenance.json) is unchanged; its `file` fields refer to the historical capture
names. [SHA256SUMS.txt](SHA256SUMS.txt) verifies the actual archive paths. The original checksum
file is separately preserved as [original-SHA256SUMS.txt](original-SHA256SUMS.txt).

[research-notes.txt](research-notes.txt) preserves the original preparation notes byte for byte.
They precede implementation and are not the final capability contract. For example, the final
implementation preserves numeric JSON monetary literals as Unknown rather than normalizing them;
supported exact string decimals are normalized without floating-point conversion.

Use the [native API guide](../entropy-economics-api-20261008.md) for current policy, report,
attribution, receipt and recovery semantics. Official source captures and synthetic local tests
do not prove actual private settlements or application entry/close results.

The pinned official SDK [MIT license](sdk-LICENSE.txt) accompanies the captured SDK source files.
