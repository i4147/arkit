# `arkit` Documentation

Welcome to the technical documentation for **`arkit`** — a universal, streaming, memory-bounded, and security-hardened Python archive toolkit.

## Guides & Reference

1. [**Architecture & Memory Efficiency**](./architecture_and_memory.md)
   - How `arkit` achieves `O(chunk_size)` streaming memory across 20 archive and stream formats, bounded parallel Brotli pipelines, single-pass `tar.*` streaming, zero-staging conversions, and how legacy prototype bottlenecks were eliminated.
2. [**Formats, Codecs & Plugins**](./formats_and_codecs.md)
   - Complete matrix of supported container and stream formats, magic-byte & deep-tar sniffing, compression level ranges, multi-threaded codecs, optional dependency extras, and runtime `register_codec` / `register_format` extensibility.
3. [**Security & Crash Safety**](./security.md)
   - Threat model and built-in defenses against Zip/Tar Slip path traversal, symlink/hardlink escapes, special device nodes, decompression bombs (`SafetyPolicy`), atomic temp-file replacement (`atomicio`), SHA-256 split manifests, and deterministic builds.
4. [**Python API Reference**](./api_reference.md)
   - Detailed reference for functional operations, object-oriented `Archive`, `ArchiveProcessor`, `batch_extract`, `walk` (`VirtualPath`), `AdvancedWalker` (`VirtualFile`), `ParallelBrotliCompressor`, `split`/`merge`, `atomicio`, data models, and exceptions.
5. [**CLI Reference**](./cli_reference.md)
   - Complete command-line guide covering all 13 `arkit` subcommands, flags, byte-size units, `--dry-run`, `--deterministic`, and `--json` machine-readable output.

## Additional Resources

- **Runnable Examples**: [`../examples/`](../examples/README.md)
- **Empirical Benchmarks**: [`../benchmark/results.md`](../benchmark/results.md)
