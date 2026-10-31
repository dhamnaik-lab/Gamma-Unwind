# Source snapshot, integrity and recovery

**Canonical script:** `E:\TA Projects\Gamma Unwind\scripts\Create-GammaUnwind-SourceSnapshot.ps1`

**Archive directory:** `E:\TA Projects\Gamma Unwind Source Code Audit` — intentionally **outside** the project tree.

## Snapshot workflow

1. Confirm the installed source folder is `E:\TA Projects\Gamma Unwind` (not the old v0.8.9 distribution folder).
2. Run the installed script in PowerShell 7, first with `-Preview` and review included/skipped file counts.
3. Run without `-Preview`; the script creates a timestamped source ZIP, `__SOURCE_SNAPSHOT__/MANIFEST.json`, and internal `__SOURCE_SNAPSHOT__/SHA256SUMS.txt`.
4. Record the printed whole-ZIP SHA-256 in your secure release ledger. **R3 no longer creates a `.zip.sha256` sidecar.** It does not remove older R1/R2 sidecars.
5. Review skipped entries. The snapshot is **not** a runtime database, credential, Python installation or user-data backup.

```powershell
Set-Location -LiteralPath 'E:\TA Projects\Gamma Unwind'
$runner = '.\scripts\Create-GammaUnwind-SourceSnapshot.ps1'
& $runner -Preview
& $runner
```

## Authority and integrity

The uploaded R2 snapshot `Gamma Unwind Source Code 2026-10-31_023834_017_IST_7f5baf5a.zip` was verified with 221 included SHA-256 source hashes, 222 SHA256SUMS lines (source files plus manifest) and whole-archive hash `7408392DDBB5B4E50854D796303328D90A4734654ED5351BB34038583D29BF44`. That archived snapshotter is **R2**, not the newer installed R3. The user confirmed R3 installation, but a new R3 ZIP and its checksum were not supplied here for independent verification.

Within R3, the checksum list covers each source entry and the internal JSON manifest. **It cannot include a hash of itself or the complete final ZIP**, which is why the complete ZIP SHA-256 is printed only to the terminal.

## Recovery procedure (review first)

- **Do not overwrite the installed application using an archive merely because its hash matches.** First verify the archive is trusted, the expected edition/version and source tree are understood, and it contains only intended source files.
- Compare archived paths and SHA-256 with the installed tree and classify expected differences. Do not restore live `.env`, token vaults, databases, logs, or Python runtime from a source snapshot.
- Recover into a **new disposable directory**, never directly over the installed tree. Verify internal checksums after extraction. Reject path traversal, unexpected symbolic links, duplicate paths and secret-bearing files.
- Test restored sources offline before any individually authorized application deployment. Keep R10H Apply on HOLD.

## Archive retention

Keep at least a verified pre-change snapshot and the last accepted audit snapshot. Record ZIP filename, whole SHA-256, captured source count, date/time IST, script revision and acceptance evidence. Do not rename a ZIP and reuse a historical hash without recalculating it.

## Security note

The inclusion filter removes many known credential/data formats, but **source code, sample configs and the original DOCX guide can still contain sensitive text**. Review before sharing; do not upload private source snapshots publicly.
