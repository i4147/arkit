# `arkit` Examples

Self-contained, runnable Python scripts demonstrating every major subsystem of `arkit`. Each script creates its own temporary workspace and cleans up after itself.

## Example Catalog

| Script | Topics Covered |
| :--- | :--- |
| [`01_quickstart_and_stats.py`](./01_quickstart_and_stats.py) | Creating `.tar.zst` and `.zip` archives, `CompressionStats`, `info()`, `validate()`, cross-format `convert()`, `diff_archives()`, and OO `Archive` handles. |
| [`02_streaming_and_custom_codecs.py`](./02_streaming_and_custom_codecs.py) | Constant-RAM streaming (`open_stream_writer` / `open_stream_reader`), `ZstdTarFile`, framed Snappy & Brotli streams, and registering custom codec/format plugins. |
| [`03_in_place_processing_and_batch.py`](./03_in_place_processing_and_batch.py) | Transforming members inside compressed archives via `process_archive()` and `ArchiveProcessor.process_streams()`, plus concurrent recursive `batch_extract()`. |
| [`04_virtual_walkers_and_nested_edits.py`](./04_virtual_walkers_and_nested_edits.py) | Read-only lazy `walk()` + `VirtualPath` search across nested archives without disk extraction, and `AdvancedWalker` (`VirtualFile`) two-way atomic nested archive editing. |
| [`05_security_sandboxing_and_split.py`](./05_security_sandboxing_and_split.py) | `SafetyPolicy` bomb & path-traversal protection, deterministic reproducible archives, `ParallelBrotliCompressor` (`PBRT0001`), `split_file`/`merge_files` with `.sha256` manifests, and crash-safe `atomic_open`/`atomic_write`. |

## Running

```bash
python examples/01_quickstart_and_stats.py
python examples/02_streaming_and_custom_codecs.py
python examples/03_in_place_processing_and_batch.py
python examples/04_virtual_walkers_and_nested_edits.py
python examples/05_security_sandboxing_and_split.py
```
