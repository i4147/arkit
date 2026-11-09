"""Production command-line interface for ``arkit``.

Exposes subcommands ``compress``, ``extract``, ``batch-extract``, ``list``,
``info``, ``validate``, ``convert``, ``diff``, ``walk``, ``split``, ``merge``,
``pbrt``, and ``codecs`` with human-readable size parsing, security flags,
reproducible build mode, ``--dry-run``, and structured ``--json`` output.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from . import __version__
from .batch import batch_extract, scan_archives
from .codecs import codec_available, level_bounds, missing_codecs
from .exceptions import ArchiveError
from .formats import FORMATS, Format, find_archive_format
from .models import SafetyPolicy
from .operations import (
    _iter_source_files,
    compress,
    convert,
    decompress,
    diff_archives,
    info,
    list_members,
    validate,
)
from .parallel_brotli import ParallelBrotliCompressor
from .split import merge_files, split_file
from .walker import DEFAULT_SKIP_DIRS, walk

__all__ = ["build_parser", "main", "parse_byte_size"]

_SIZE_UNITS: dict[str, int] = {
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}


def parse_byte_size(raw: str) -> int:
    """Parse human-readable byte sizes such as ``'512K'``, ``'64M'``, ``'2GiB'``, or ``'1048576'``."""
    text = raw.strip().lower().replace("_", "")
    if not text:
        raise ValueError("Empty size string")
    split_idx = len(text)
    for i, ch in enumerate(text):
        if not (ch.isdigit() or ch == "."):
            split_idx = i
            break
    num_part = text[:split_idx]
    unit_part = text[split_idx:].strip() or "b"
    if not num_part or unit_part not in _SIZE_UNITS:
        raise ValueError(f"Invalid byte size {raw!r}; examples: 64K, 10M, 2G")
    value = float(num_part) * _SIZE_UNITS[unit_part]
    result = int(value)
    if result <= 0:
        raise ValueError(f"Byte size must be positive: {raw!r}")
    return result


def _format_bytes(n: int) -> str:
    val = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(val) < 1024.0 or unit == "TiB":
            return f"{int(val)} B" if unit == "B" else f"{val:.2f} {unit}"
        val /= 1024.0
    return f"{n} B"


def _emit(
    payload: Any,
    *,
    as_json: bool,
    text_lines: Sequence[str] | None = None,
    quiet: bool = False,
) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, default=str))
    elif not quiet and text_lines is not None:
        for line in text_lines:
            print(line)


def build_parser() -> argparse.ArgumentParser:
    """Construct the ``arkit`` CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="arkit",
        description="Universal, streaming, memory-efficient, and security-hardened Python archive utility.",
    )
    parser.add_argument("--version", action="version", version=f"arkit {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress non-error output")

    sub = parser.add_subparsers(dest="command", required=True)

    # 1. compress / pack
    p_comp = sub.add_parser("compress", aliases=["pack"], help="Create an archive or compress a file")
    p_comp.add_argument("source", help="Source file or directory")
    p_comp.add_argument("output", help="Destination archive/compressed file path")
    p_comp.add_argument("-f", "--format", default=None, help="Explicit output format (e.g. tar.zst, zip, 7z, gz)")
    p_comp.add_argument("-l", "--level", type=int, default=None, help="Compression level")
    p_comp.add_argument("-t", "--threads", type=int, default=None, help="Worker thread count (e.g. zstd, pbrt)")
    p_comp.add_argument("-i", "--include", action="append", default=None, help="Glob pattern to include (repeatable)")
    p_comp.add_argument("-x", "--exclude", action="append", default=None, help="Glob pattern to exclude (repeatable)")
    p_comp.add_argument("--include-root", action="store_true", help="Include source directory name as top-level prefix")
    p_comp.add_argument("--follow-symlinks", action="store_true", help="Follow filesystem symlinks when packing")
    p_comp.add_argument("--deterministic", action="store_true", help="Create reproducible byte-identical archive")
    p_comp.add_argument("--checksum", action="store_true", help="Compute SHA-256 digest of output archive")
    p_comp.add_argument("--dry-run", action="store_true", help="Preview files to pack without writing archive")
    p_comp.add_argument("--json", action="store_true", help="Output structured JSON")

    # 2. extract / decompress
    p_ext = sub.add_parser("extract", aliases=["decompress"], help="Safely extract an archive or stream")
    p_ext.add_argument("archive", help="Path to archive or compressed file")
    p_ext.add_argument("-o", "--output", default=".", help="Extraction destination directory (default: .)")
    p_ext.add_argument("-f", "--format", default=None, help="Explicit input format override")
    p_ext.add_argument("--output-name", default=None, help="Output filename override for single-stream formats")
    p_ext.add_argument("-m", "--member", action="append", default=None, help="Specific member(s) to extract")
    p_ext.add_argument("-i", "--include", action="append", default=None, help="Glob pattern to include")
    p_ext.add_argument("-x", "--exclude", action="append", default=None, help="Glob pattern to exclude")
    p_ext.add_argument("--strip-components", type=int, default=0, help="Strip N leading path components")
    p_ext.add_argument("--no-overwrite", action="store_true", help="Skip existing destination files")
    p_ext.add_argument("--preserve-permissions", action="store_true", help="Restore POSIX file mode bits")
    p_ext.add_argument("--no-preserve-timestamps", action="store_true", help="Do not restore modification times")
    p_ext.add_argument("--allow-symlinks", action="store_true", help="Allow safe relative symlinks inside root")
    p_ext.add_argument("--unsafe", action="store_true", help="Disable SafetyPolicy path/bomb checks")
    p_ext.add_argument("--max-size", default=None, help="Max total extracted bytes (e.g. 2G, 500M)")
    p_ext.add_argument("--max-file-size", default=None, help="Max single-file size (e.g. 1G)")
    p_ext.add_argument("--max-files", type=int, default=500_000, help="Max extracted member count")
    p_ext.add_argument("--max-ratio", type=float, default=1000.0, help="Max decompression expansion ratio")
    p_ext.add_argument("--password", default=None, help="Password for encrypted ZIP/7z archives")
    p_ext.add_argument("--nested", action="store_true", help="Recursively extract nested archives")
    p_ext.add_argument("--remove-source", action="store_true", help="Delete source archive after successful extraction")
    p_ext.add_argument("--dry-run", action="store_true", help="List members that would be extracted without writing")
    p_ext.add_argument("--json", action="store_true", help="Output structured JSON")

    # 3. batch-extract
    p_batch = sub.add_parser("batch-extract", help="Concurrently extract multiple archives or scan directories")
    p_batch.add_argument("targets", nargs="*", default=["."], help="Archive files or directories to scan")
    p_batch.add_argument("-o", "--output", default=None, help="Optional root output directory")
    p_batch.add_argument("-w", "--workers", type=int, default=4, help="Concurrent worker count (default: 4)")
    p_batch.add_argument("--no-nested", action="store_true", help="Do not expand nested archives")
    p_batch.add_argument("--max-depth", type=int, default=3, help="Max nested archive depth (default: 3)")
    p_batch.add_argument("--remove-source", action="store_true", help="Remove archives after successful extraction")
    p_batch.add_argument("--no-check", action="store_true", help="Skip pre-extraction integrity check")
    p_batch.add_argument("--json", action="store_true", help="Output structured JSON")

    # 4. list / ls
    p_ls = sub.add_parser("list", aliases=["ls"], help="List archive members without extracting")
    p_ls.add_argument("archive", help="Path to archive")
    p_ls.add_argument("-f", "--format", default=None, help="Format override")
    p_ls.add_argument("-l", "--long", action="store_true", help="Show sizes and timestamps")
    p_ls.add_argument("-p", "--pattern", default=None, help="Filter member names by glob")
    p_ls.add_argument("--json", action="store_true", help="Output structured JSON")

    # 5. info
    p_info = sub.add_parser("info", help="Display archive format, sizes, ratio, and summary")
    p_info.add_argument("archive", help="Path to archive")
    p_info.add_argument("-f", "--format", default=None, help="Format override")
    p_info.add_argument("--checksum", action="store_true", help="Include SHA-256 digest")
    p_info.add_argument("--json", action="store_true", help="Output structured JSON")

    # 6. validate / test
    p_val = sub.add_parser("validate", aliases=["test"], help="Stream-verify archive integrity and CRCs")
    p_val.add_argument("archives", nargs="+", help="One or more archives to validate")
    p_val.add_argument("-f", "--format", default=None, help="Format override")
    p_val.add_argument("--json", action="store_true", help="Output structured JSON")

    # 7. convert
    p_conv = sub.add_parser("convert", help="Stream-convert an archive to another format")
    p_conv.add_argument("source", help="Source archive path")
    p_conv.add_argument("output", help="Destination archive path")
    p_conv.add_argument("-f", "--format", default=None, help="Target format override")
    p_conv.add_argument("-l", "--level", type=int, default=None, help="Target compression level")
    p_conv.add_argument("-t", "--threads", type=int, default=None, help="Worker thread count")
    p_conv.add_argument("--deterministic", action="store_true", help="Reproducible output archive")
    p_conv.add_argument("--checksum", action="store_true", help="Compute SHA-256 of converted archive")
    p_conv.add_argument("--json", action="store_true", help="Output structured JSON")

    # 8. diff
    p_diff = sub.add_parser("diff", help="Compare two archives by streaming member hashes")
    p_diff.add_argument("archive_a", help="First archive")
    p_diff.add_argument("archive_b", help="Second archive")
    p_diff.add_argument("--json", action="store_true", help="Output structured JSON")

    # 9. walk
    p_walk = sub.add_parser("walk", help="Walk directories and nested archives lazily")
    p_walk.add_argument("root", nargs="?", default=".", help="Root directory or archive (default: .)")
    p_walk.add_argument("-d", "--max-depth", type=int, default=3, help="Max archive nesting depth")
    p_walk.add_argument("--files-only", action="store_true", help="Yield only file entries")
    p_walk.add_argument("--follow-symlinks", action="store_true", help="Follow filesystem symlinks")
    p_walk.add_argument("--skip-default-dirs", action="store_true", help="Skip .git, __pycache__, .venv, etc.")
    p_walk.add_argument("-p", "--pattern", default=None, help="Glob pattern filter")
    p_walk.add_argument("-g", "--grep", default=None, help="Search substring inside walked files & nested archives")
    p_walk.add_argument("--json", action="store_true", help="Output structured JSON")

    # 10. split
    p_split = sub.add_parser("split", help="Split a file or archive into fixed-size parts")
    p_split.add_argument("source", help="File to split")
    p_split.add_argument("-s", "--size", required=True, help="Max part size (e.g. 10M, 512K, 1G)")
    p_split.add_argument("-o", "--output-dir", default=None, help="Destination directory for parts")
    p_split.add_argument("--prefix", default=None, help="Filename prefix for parts")
    p_split.add_argument("--manifest", action="store_true", help="Generate .sha256 checksum manifest")
    p_split.add_argument("--json", action="store_true", help="Output structured JSON")

    # 11. merge
    p_merge = sub.add_parser("merge", help="Merge split part files back into a single file")
    p_merge.add_argument("parts", nargs="+", help="Part files to merge")
    p_merge.add_argument("-o", "--output", required=True, help="Destination merged file path")
    p_merge.add_argument("--sort", action="store_true", help="Sort part files by filename before merging")
    p_merge.add_argument("--verify-manifest", action="store_true", help="Verify part SHA-256 digests against manifest")
    p_merge.add_argument("--json", action="store_true", help="Output structured JSON")

    # 12. pbrt
    p_pbrt = sub.add_parser("pbrt", help="Parallel chunked Brotli (PBRT0001) compress/decompress")
    p_pbrt.add_argument("input", help="Input file path")
    p_pbrt.add_argument("output", help="Output file path")
    p_pbrt.add_argument("-d", "--decompress", action="store_true", help="Decompress a .pbrt container")
    p_pbrt.add_argument("-q", "--quality", type=int, default=4, help="Brotli quality 0..11 (default: 4)")
    p_pbrt.add_argument("-c", "--chunk-size", default="4M", help="Chunk size (default: 4M, min: 64K)")
    p_pbrt.add_argument("-w", "--workers", type=int, default=None, help="Worker count")
    p_pbrt.add_argument("--backend", choices=("thread", "process"), default="thread", help="Execution pool backend")
    p_pbrt.add_argument("--json", action="store_true", help="Output structured JSON")

    # 13. codecs
    p_codecs = sub.add_parser("codecs", help="Report available compression codecs and registered formats")
    p_codecs.add_argument("--json", action="store_true", help="Output structured JSON")

    for sp in set(sub.choices.values()):
        v_flags = ("-v", "--verbose") if "-v" not in sp._option_string_actions else ("--verbose",)
        q_flags = ("-q", "--quiet") if "-q" not in sp._option_string_actions else ("--quiet",)
        sp.add_argument(
            *v_flags,
            action="store_true",
            default=argparse.SUPPRESS,
            help="Enable verbose debug logging",
        )
        sp.add_argument(
            *q_flags,
            action="store_true",
            default=argparse.SUPPRESS,
            help="Suppress non-error output",
        )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``arkit`` command-line tool."""
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else (logging.ERROR if args.quiet else logging.WARNING)
    logging.basicConfig(level=log_level, format="[%(levelname)s] %(message)s")

    try:
        cmd: str = args.command

        if cmd in ("compress", "pack"):
            src = Path(args.source)
            if args.dry_run:
                planned = [
                    arc
                    for _fp, arc in _iter_source_files(
                        src,
                        include_root=args.include_root,
                        follow_symlinks=args.follow_symlinks,
                        include=args.include,
                        exclude=args.exclude,
                    )
                ]
                _emit(
                    {"dry_run": True, "output": args.output, "files": planned, "count": len(planned)},
                    as_json=args.json,
                    text_lines=[f"[dry-run] Would pack {len(planned)} file(s) -> {args.output}", *planned],
                )
                return 0

            stats = compress(
                src,
                args.output,
                format=args.format,
                level=args.level,
                threads=args.threads,
                include_root=args.include_root,
                follow_symlinks=args.follow_symlinks,
                include=args.include,
                exclude=args.exclude,
                deterministic=args.deterministic,
                compute_sha256=args.checksum,
            )
            payload: dict[str, Any] = {
                "source_bytes": stats.source_bytes,
                "result_bytes": stats.result_bytes,
                "ratio": round(stats.ratio, 4),
                "space_freed": stats.space_freed,
                "space_change_percent": round(stats.space_change_percent, 2),
                "elapsed_seconds": round(stats.elapsed_seconds, 4),
                "sha256": stats.sha256,
                "output": str(args.output),
            }
            lines = [] if args.quiet else [
                f"Compressed {args.source} -> {args.output}",
                f"  Input:     {_format_bytes(stats.source_bytes)} ({stats.source_bytes:,} B)",
                f"  Output:    {_format_bytes(stats.result_bytes)} ({stats.result_bytes:,} B)",
                f"  Ratio:     {stats.ratio:.3f} ({stats.space_change_percent:.2f}% reduction)",
                f"  Elapsed:   {stats.elapsed_seconds:.3f}s",
                *([f"  SHA-256:   {stats.sha256}"] if stats.sha256 else []),
            ]
            _emit(payload, as_json=args.json, text_lines=lines)
            return 0

        if cmd in ("extract", "decompress"):
            arch_path = Path(args.archive)
            if args.dry_run:
                members = list_members(arch_path, fmt=args.format)
                names = [m.name for m in members]
                _emit(
                    {"dry_run": True, "archive": str(arch_path), "members": names, "count": len(names)},
                    as_json=args.json,
                    text_lines=[f"[dry-run] Would extract {len(names)} member(s) -> {args.output}", *names],
                )
                return 0

            policy = SafetyPolicy(
                allow_symlinks=args.allow_symlinks,
                max_total_size=parse_byte_size(args.max_size) if args.max_size else 10 * (1024**3),
                max_file_size=parse_byte_size(args.max_file_size) if args.max_file_size else 4 * (1024**3),
                max_files=args.max_files,
                max_ratio=args.max_ratio,
            )

            if args.nested:
                from .batch import extract_archive

                ok = extract_archive(
                    arch_path,
                    output_dir=args.output,
                    extract_nested=True,
                    remove_source=args.remove_source,
                    safe=not args.unsafe,
                    policy=policy,
                )
                if not ok:
                    return 1
                out_res = Path(args.output)
            else:
                out_res = decompress(
                    arch_path,
                    args.output,
                    format=args.format,
                    output_name=args.output_name,
                    safe=not args.unsafe,
                    policy=policy,
                    strip_components=args.strip_components,
                    overwrite=not args.no_overwrite,
                    preserve_permissions=args.preserve_permissions,
                    preserve_timestamps=not args.no_preserve_timestamps,
                    members=args.member,
                    include=args.include,
                    exclude=args.exclude,
                    password=args.password,
                )
                if args.remove_source:
                    arch_path.unlink(missing_ok=True)

            _emit(
                {"archive": str(arch_path), "destination": str(out_res)},
                as_json=args.json,
                text_lines=[] if args.quiet else [f"Extracted {arch_path} -> {out_res}"],
            )
            return 0

        if cmd == "batch-extract":
            results = batch_extract(
                args.targets,
                output_dir=args.output,
                workers=args.workers,
                extract_nested=not args.no_nested,
                max_nested_depth=args.max_depth,
                remove_source=args.remove_source,
                check_first=not args.no_check,
            )
            ok_count = sum(1 for v in results.values() if v)
            fail_count = len(results) - ok_count
            _emit(
                {
                    "total": len(results),
                    "succeeded": ok_count,
                    "failed": fail_count,
                    "results": {str(k): v for k, v in results.items()},
                },
                as_json=args.json,
                text_lines=[] if args.quiet else [
                    f"Batch extraction complete: {ok_count} succeeded, {fail_count} failed (total {len(results)})"
                ],
            )
            return 0 if fail_count == 0 else 1

        if cmd in ("list", "ls"):
            import fnmatch as _fnm

            members = list_members(args.archive, fmt=args.format)
            if args.pattern:
                members = [m for m in members if _fnm.fnmatch(m.name, args.pattern)]
            if args.json:
                _emit(
                    [
                        {
                            "name": m.name,
                            "size": m.size,
                            "compressed_size": m.compressed_size,
                            "mtime": m.mtime.isoformat() if m.mtime else None,
                        }
                        for m in members
                    ],
                    as_json=True,
                )
            elif args.long:
                for m in members:
                    mt_str = m.mtime.strftime("%Y-%m-%d %H:%M:%S") if m.mtime else "                   "
                    print(f"{m.size:>12,}  {mt_str}  {m.name}")
            else:
                for m in members:
                    print(m.name)
            return 0

        if cmd == "info":
            ai = info(args.archive, fmt=args.format, compute_sha256=args.checksum)
            info_payload: dict[str, Any] = {
                "path": str(ai.path) if ai.path else None,
                "format": ai.format.format_id,
                "size": ai.size,
                "payload_size": ai.payload_size,
                "member_count": ai.member_count,
                "expansion_ratio": ai.ratio,
                "space_saving_percent": round(ai.space_saving_percent, 2),
                "sha256": ai.sha256,
            }
            lines = [
                f"Archive:       {ai.path}",
                f"Format:        {ai.format.format_id} ({ai.format.ext})",
                f"Archive size:  {_format_bytes(ai.size)} ({ai.size:,} B)",
                f"Payload size:  {_format_bytes(ai.payload_size)} ({ai.payload_size:,} B)",
                f"Members:       {ai.member_count}",
                f"Space saved:   {ai.space_saving_percent:.2f}% (expansion {ai.ratio:.2f}x)",
                *([f"SHA-256:       {ai.sha256}"] if ai.sha256 else []),
            ]
            _emit(info_payload, as_json=args.json, text_lines=lines)
            return 0

        if cmd in ("validate", "test"):
            reports = []
            all_healthy = True
            lines = []
            for arch in args.archives:
                rep = validate(arch, format=args.format)
                all_healthy = all_healthy and rep.healthy
                reports.append(
                    {
                        "archive": arch,
                        "healthy": rep.healthy,
                        "format": rep.format,
                        "checked_members": rep.checked_members,
                        "checked_bytes": rep.checked_bytes,
                        "errors": list(rep.errors),
                    }
                )
                status = "OK" if rep.healthy else f"FAIL ({'; '.join(rep.errors)})"
                lines.append(f"[{status}] {arch} (format={rep.format}, members={rep.checked_members})")
            _emit(reports if len(reports) > 1 else reports[0], as_json=args.json, text_lines=lines)
            return 0 if all_healthy else 1

        if cmd == "convert":
            stats = convert(
                args.source,
                args.output,
                output_format=args.format,
                level=args.level,
                threads=args.threads,
                deterministic=args.deterministic,
                compute_sha256=args.checksum,
            )
            payload = {
                "source": args.source,
                "output": args.output,
                "source_bytes": stats.source_bytes,
                "result_bytes": stats.result_bytes,
                "ratio": round(stats.ratio, 4),
                "elapsed_seconds": round(stats.elapsed_seconds, 4),
                "sha256": stats.sha256,
            }
            lines = [] if args.quiet else [
                f"Converted {args.source} -> {args.output} ({_format_bytes(stats.source_bytes)} -> {_format_bytes(stats.result_bytes)} in {stats.elapsed_seconds:.3f}s)"
            ]
            _emit(payload, as_json=args.json, text_lines=lines)
            return 0

        if cmd == "diff":
            dr = diff_archives(args.archive_a, args.archive_b)
            diff_payload: dict[str, Any] = {
                "identical": dr.identical,
                "added": list(dr.added),
                "removed": list(dr.removed),
                "modified": list(dr.modified),
                "unchanged_count": len(dr.unchanged),
            }
            lines = (
                ["Archives are identical."]
                if dr.identical
                else [
                    *(f"+ {n}" for n in dr.added),
                    *(f"- {n}" for n in dr.removed),
                    *(f"M {n}" for n in dr.modified),
                ]
            )
            _emit(diff_payload, as_json=args.json, text_lines=lines)
            return 0 if dr.identical else 1

        if cmd == "walk":
            records = []
            lines = []
            needle = args.grep.encode("utf-8") if args.grep else None
            for vp in walk(
                args.root,
                max_depth=args.max_depth,
                include_dirs=not args.files_only,
                follow_symlinks=args.follow_symlinks,
                skip_dirs=DEFAULT_SKIP_DIRS if args.skip_default_dirs else None,
                pattern=args.pattern,
            ):
                if needle is not None:
                    if not vp.is_file():
                        continue
                    if vp.depth < args.max_depth and find_archive_format(vp.name) is not None:
                        continue
                    try:
                        data = vp.read_bytes()
                    except Exception:
                        continue
                    if needle not in data:
                        continue
                records.append(
                    {
                        "path": str(vp),
                        "kind": vp.kind,
                        "depth": vp.depth,
                        "size": vp.size,
                    }
                )
                sz = f"{vp.size:>10}" if vp.size is not None else "         ?"
                lines.append(f"{sz}  [d={vp.depth}]  {vp}")
            _emit(records, as_json=args.json, text_lines=lines)
            return 0

        if cmd == "split":
            part_bytes = parse_byte_size(args.size)
            parts = split_file(
                args.source,
                part_bytes,
                out_dir=args.output_dir,
                prefix=args.prefix,
                manifest=args.manifest,
            )
            _emit(
                {"source": args.source, "part_size": part_bytes, "parts": [str(p) for p in parts]},
                as_json=args.json,
                text_lines=[f"Created {len(parts)} part(s):", *(f"  {p}" for p in parts)],
            )
            return 0

        if cmd == "merge":
            merged = merge_files(
                args.parts,
                args.output,
                sort=args.sort,
                verify_manifest=args.verify_manifest,
            )
            _emit(
                {"output": str(merged), "parts_merged": len(args.parts)},
                as_json=args.json,
                text_lines=[f"Merged {len(args.parts)} part(s) -> {merged}"],
            )
            return 0

        if cmd == "pbrt":
            if args.decompress:
                out_p = ParallelBrotliCompressor.decompress_file(
                    args.input,
                    args.output,
                    workers=args.workers or 1,
                )
                _emit(
                    {"mode": "decompress", "input": args.input, "output": str(out_p)},
                    as_json=args.json,
                    text_lines=[f"Decompressed PBRT {args.input} -> {out_p}"],
                )
            else:
                csize = parse_byte_size(args.chunk_size)
                pbc = ParallelBrotliCompressor(
                    quality=args.quality,
                    chunk_size=csize,
                    workers=args.workers,
                    backend=args.backend,
                )
                out_p = pbc.compress_file(args.input, args.output)
                _emit(
                    {
                        "mode": "compress",
                        "input": args.input,
                        "output": str(out_p),
                        "quality": args.quality,
                        "chunk_size": csize,
                        "workers": pbc.workers,
                    },
                    as_json=args.json,
                    text_lines=[f"Compressed PBRT {args.input} -> {out_p} (workers={pbc.workers})"],
                )
            return 0

        if cmd == "codecs":
            codec_names = ("gz", "bz2", "xz", "zst", "br", "lz4", "snappy", "pbrt", "7z")
            report_codecs = []
            lines = ["Codecs:"]
            for cname in codec_names:
                avail = codec_available(cname)
                lo, hi, df = level_bounds(cname)
                report_codecs.append(
                    {
                        "codec": cname,
                        "available": avail,
                        "min_level": lo,
                        "max_level": hi,
                        "default_level": df,
                    }
                )
                status = "available" if avail else "missing"
                lines.append(f"  {cname:<8} {status:<10} levels={lo}..{hi} (default {df})")
            lines.append(f"\nRegistered formats ({len(FORMATS)}): {', '.join(sorted(FORMATS))}")
            _emit(
                {
                    "codecs": report_codecs,
                    "missing": missing_codecs(),
                    "formats": sorted(FORMATS),
                    "enum_formats": [f.format_id for f in Format],
                },
                as_json=args.json,
                text_lines=lines,
            )
            return 0

    except ArchiveError as exc:
        if getattr(args, "json", False):
            print(json.dumps({"error": type(exc).__name__, "message": str(exc)}))
        else:
            print(f"arkit error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        if getattr(args, "json", False):
            print(json.dumps({"error": type(exc).__name__, "message": str(exc)}))
        else:
            print(f"arkit error ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 2

    _ = scan_archives
    return 0
