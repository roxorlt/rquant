# Historical worktree materials — 2026-11-03

This archive preserves useful materials from 22 retired rQuant scratch directories. It is historical evidence, not the current installation or release.

`public-materials.tar.gz` contains scripts, documentation, historical evidence, two visually reviewed market-data UI screenshots and sanitized configuration. The manifest lists every preserved original, SHA256, publication decision and every tar member. Raw environment recipes, database samples, release binaries and uncertain credential-bearing records remain in the verified private local backup. No `.env` or private-key files were read or included.

## Verify and restore

1. Download this directory and run `shasum -a 256 -c SHA256SUMS`.
2. Extract `public-materials.tar.gz` into a new empty directory. The tar contains only relative regular files. Verify the extracted files against `manifest.json` → `public_archive_members`.
3. For exact originals, use `data/archives/worktree-cleanup/2026-11-03/original-materials.tar.gz` in the local rQuant project. Compare its SHA256 with `manifest.json` → `private_backup.sha256`, extract into a new empty directory and check all hashes in `original-manifest.json`. This backup includes the withheld originals; GitHub alone is not an exact copy of those private files.
4. `source-recovery.json` reconstructs the three retained source trees without duplicating files already in GitHub main history. In a full clone of `roxorlt/rquant`, for each snapshot create an empty destination, then run `git cat-file blob <git_blob> > <path>` for each mapped row and apply its `git_mode`. Compare SHA256 against the map. Overlay the snapshot's `override_paths` from the private backup `materials/<root>/<prefix>/...`. The rq-rahi snapshot uses main commit `7cc7ab87` plus its two historical metadata overrides; its original branch HEAD was `22a83d61226f9e8e3425885bb6269b4d9f0f6f1c`.

For example, after extracting the package into an empty directory, replace `/path/to/full/rquant-clone` below with a full (not shallow) repository clone:

```sh
python3 - <<'PY'
from pathlib import Path
import hashlib, json, os, subprocess
repository = Path('/path/to/full/rquant-clone')
for snapshot in json.loads(Path('source-recovery.json').read_text())['snapshots']:
    destination = Path('restored') / snapshot['root'] / snapshot['prefix']
    for item in snapshot['files']:
        path = destination / item['path']
        path.parent.mkdir(parents=True, exist_ok=True)
        content = subprocess.check_output(['git', '-C', str(repository), 'cat-file', 'blob', item['git_blob']])
        assert hashlib.sha256(content).hexdigest() == item['sha256']
        path.write_bytes(content)
        path.chmod(int(item['git_mode'], 8) & 0o777)
print('Mapped historical source files restored; overlay originals from the private backup when needed.')
PY
```

Generated pytest trees, virtual environments, caches and synthetic private-key fixtures are intentionally omitted. Recreate dependencies from the historical lockfile when needed. The local backup has been checked by reading every tar member and verifying the complete member set and every SHA256; the public package was checked in the same way. Source directories may only be removed after the publishing task also downloads the GitHub archive and verifies these hashes.
