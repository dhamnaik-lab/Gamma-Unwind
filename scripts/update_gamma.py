"""GAMMA-UNWIND in-place update (T-131, D119 Task 1, D133 Q1/Q4; CLIENT-PACKAGE-SPEC section 5).

Run by `Update GAMMA.cmd` (Windows, the bundled python/) or `./update.sh` (Linux / macOS, the .venv). Standard
library only: this script must run on a package whose own libraries are about to be replaced.

Sequence:
  1. read MANIFEST.json (none, or a .git folder = a repo checkout: "run git pull", exit 1)
  2. fetch <RELEASES_URL>/latest.json; nothing newer = exit 1
  3. download this OS's ZIP to data/update/<ver>/, check its sha256 + size, unpack it, check every file against
     the NEW MANIFEST.json both ways (a file not listed, a listed file missing, a sha / size mismatch = exit 2)
  4. the bundled python/ (Windows) or requirements-client.txt (Linux / macOS) differs = exit 5: the running
     interpreter cannot replace itself, download the full ZIP instead
  5. write data/STOP + data/QUIT and wait for the port to be free (still busy = exit 4, D31)
  6. swap, FILE by file: every file of the OLD MANIFEST moves to data/update/backup-<oldver>/, every file of the NEW
     one moves in. Never touched: .env, data/, logs/, .venv/, python/, and any file the MANIFEST does not list.
     config/strategy.yaml is replaced by the release copy; the previous copy is kept as
     config/strategy.yaml.bak-<oldver> and the path is printed (D133 Q4). __pycache__ folders are deleted.
  7. start, then print the version /api/status reports
  8. a failure during 6 or 7 restores the backup (journal data/update/journal.json; a run after an interrupted
     swap restores it first) and starts the previous version again = exit 6

Exit codes: 0 updated; 1 already current / declined / a repo checkout; 2 verify failed; 3 network (or no releases
address in this copy); 4 the service did not stop, the port is still busy; 5 the new version needs a different
bundled Python or library set (download the full ZIP); 6 the update failed and the previous version was restored.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

# D133 Q1: the releases site (a neutral GitHub organisation's Pages site holding latest.json + SHA256SUMS). ONE
# constant, filled in by the owner; tools/build_client_package.py refuses a release build while it is blank, and
# check_client_package.py rule L13 fails a ZIP whose copy is blank. Never an address holding an owner identity (L4).
RELEASES_URL = "https://gamma-unwind.github.io/releases"

EXIT_UPDATED = 0
EXIT_NOTHING = 1
EXIT_VERIFY = 2
EXIT_NETWORK = 3
EXIT_BUSY = 4
EXIT_PYTHON = 5
EXIT_RESTORED = 6

# never moved by the swap, whatever a MANIFEST lists (the client's settings, data, logs and interpreters)
KEEP_PREFIXES = ("data/", "logs/", ".venv/", "python/")
KEEP_FILES = frozenset({".env"})
YAML = "config/strategy.yaml"
DEFAULT_PORT = 8760
MAX_LATEST_BYTES = 1_000_000
ROOT = Path(__file__).resolve().parent.parent


def _default_fetch(url: str) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "GAMMA-UNWIND-updater", "Cache-Control": "no-cache"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read(MAX_LATEST_BYTES + 1)


def _default_download(url: str, dest: Path) -> None:
    part = dest.with_name(dest.name + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "GAMMA-UNWIND-updater"})
    with urllib.request.urlopen(req, timeout=60) as r, part.open("wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)
    part.replace(dest)


def _default_port_busy(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def _default_start(root: Path, os_kind: str) -> None:
    if os_kind == "win64":
        flags = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
        subprocess.Popen([str(root / "python" / "pythonw.exe"), str(root / "run_gamma.pyw")], cwd=root,
                         creationflags=flags, close_fds=True)  # fmt: skip
    else:
        subprocess.run(["bash", str(root / "start.sh")], cwd=root, timeout=120, check=False)


def _default_status_version(port: int) -> str | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=3) as r:
            return str(json.loads(r.read()).get("version") or "") or None
    except (OSError, ValueError):
        return None


def _default_ask(question: str) -> bool:
    try:
        return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


def _default_move(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)


@dataclass
class Deps:
    """Everything that reaches outside the package folder; the tests inject fakes (no network, no real service)."""

    fetch: Callable[[str], bytes] = _default_fetch
    download: Callable[[str, Path], None] = _default_download
    port_busy: Callable[[int], bool] = _default_port_busy
    start: Callable[[Path, str], None] = _default_start
    status_version: Callable[[int], str | None] = _default_status_version
    ask: Callable[[str], bool] = _default_ask
    move: Callable[[Path, Path], None] = _default_move
    sleep: Callable[[float], None] = time.sleep
    say: Callable[[str], None] = print
    stop_wait_s: float = 60.0
    start_wait_s: float = 120.0
    poll_s: float = 1.0
    log: list[str] = field(default_factory=list)


class VerifyError(Exception):
    """The download or its contents do not match what was published (exit 2)."""


# ------------------------------------------------------------------------------------------ small helpers
def vkey(version: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", version))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def kept(rel: str) -> bool:
    return rel in KEEP_FILES or rel.startswith(KEEP_PREFIXES)


def read_port(root: Path) -> int:
    env = root / ".env"
    try:
        text = env.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return DEFAULT_PORT
    m = re.search(r"^\s*GAMMA_PORT\s*=\s*(\d{2,5})\b", text, re.MULTILINE)
    return int(m.group(1)) if m else DEFAULT_PORT


def manifest_files(manifest: dict) -> dict[str, dict]:
    return {str(f["path"]): f for f in manifest.get("files", []) if isinstance(f, dict) and "path" in f}


def interpreter_set(manifest: dict, os_kind: str) -> dict[str, str]:
    """What the running interpreter depends on: python/** on Windows, requirements-client.txt elsewhere."""
    files = manifest_files(manifest)
    if os_kind == "win64":
        return {p: str(f.get("sha256")) for p, f in files.items() if p.startswith("python/")}
    return {p: str(f.get("sha256")) for p, f in files.items() if p == "requirements-client.txt"}


def safe_member(name: str) -> bool:
    pp = PurePosixPath(name)
    return not (name.startswith(("/", "\\")) or ".." in pp.parts or re.match(r"^[A-Za-z]:", name))


# ------------------------------------------------------------------------------------------ the updater
class Updater:
    def __init__(self, root: Path, releases_url: str, deps: Deps, assume_yes: bool) -> None:
        self.root = root
        self.releases_url = releases_url.strip().rstrip("/")
        self.d = deps
        self.assume_yes = assume_yes
        self.data = root / "data"
        self.work = self.data / "update"
        self.journal_path = self.work / "journal.json"
        self.port = read_port(root)

    def say(self, msg: str) -> None:
        self.d.log.append(msg)
        self.d.say(msg)

    # ---- the service
    def _signal_stop(self) -> None:
        self.data.mkdir(parents=True, exist_ok=True)
        for name in ("STOP", "QUIT"):
            (self.data / name).write_bytes(b"")

    def _clear_stop_files(self) -> None:
        for name in ("STOP", "QUIT"):
            try:
                (self.data / name).unlink()
            except FileNotFoundError:
                pass

    def stop_service(self) -> bool:
        """True once the port is free. Writes STOP + QUIT only when something holds the port."""
        if not self.d.port_busy(self.port):
            return True
        self.say(f"Stopping GAMMA-UNWIND (port {self.port}) ...")
        self._signal_stop()
        waited = 0.0
        while waited < self.d.stop_wait_s:
            self.d.sleep(self.d.poll_s)
            waited += self.d.poll_s
            if not self.d.port_busy(self.port):
                return True
        self._clear_stop_files()
        return False

    def start_service(self, os_kind: str, want: str) -> str | None:
        self._clear_stop_files()
        self.d.start(self.root, os_kind)
        waited = 0.0
        seen: str | None = None
        while waited <= self.d.start_wait_s:
            seen = self.d.status_version(self.port)
            if seen == want:
                return seen
            self.d.sleep(self.d.poll_s)
            waited += self.d.poll_s
        return seen

    # ---- journal + rollback
    def _write_journal(self, journal: dict) -> None:
        self.work.mkdir(parents=True, exist_ok=True)
        tmp = self.journal_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(journal, indent=1), encoding="utf-8")
        os.replace(tmp, self.journal_path)

    def rollback(self, journal: dict) -> list[str]:
        """Undo a swap from its journal; idempotent (a move is undone only when its target holds the file and its
        source does not). Returns the paths it could not restore."""
        root, stage, backup = self.root, Path(journal["stage"]), Path(journal["backup"])
        problems: list[str] = []
        for rel in journal.get("created", []):
            try:
                (root / rel).unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                problems.append(f"{rel}: {exc}")
        for rel in reversed(journal["in"]):
            if (root / rel).exists() and not (stage / rel).exists():
                try:
                    self.d.move(root / rel, stage / rel)
                except OSError as exc:
                    problems.append(f"{rel}: {exc}")
        for rel in reversed(journal["out"]):
            if (backup / rel).exists() and not (root / rel).exists():
                try:
                    self.d.move(backup / rel, root / rel)
                except OSError as exc:
                    problems.append(f"{rel}: {exc}")
        if not problems:
            self.journal_path.unlink(missing_ok=True)
        return problems

    def recover_interrupted(self) -> None:
        if not self.journal_path.is_file():
            return
        journal = json.loads(self.journal_path.read_text(encoding="utf-8"))
        self.say(
            f"An earlier update to {journal.get('new')} was interrupted: restoring {journal.get('old')} first."
        )
        problems = self.rollback(journal)
        if problems:
            raise OSError(f"could not restore {len(problems)} file(s): {problems[:3]}")

    # ---- steps
    def fetch_latest(self) -> dict:
        body = self.d.fetch(f"{self.releases_url}/latest.json")
        if len(body) > MAX_LATEST_BYTES:
            raise ValueError("latest.json is too large")
        latest = json.loads(body)
        if (
            not isinstance(latest, dict)
            or not isinstance(latest.get("assets"), dict)
            or not latest.get("version")
        ):
            raise ValueError("latest.json has no version / assets")
        return latest

    def download_and_verify(self, asset: dict, version: str, os_kind: str, stage: Path) -> tuple[Path, dict]:
        url = str(asset.get("url") or "")
        if not url.startswith("https://"):
            raise VerifyError(f"the {os_kind} download address is not https: {url[:80]!r}")
        want_sha = str(asset.get("sha256") or "").lower()
        if not re.fullmatch(r"[0-9a-f]{64}", want_sha):
            raise VerifyError(f"latest.json carries no sha256 for {os_kind}")
        zpath = stage / url.rsplit("/", 1)[-1]
        self.say(f"Downloading {zpath.name} ...")
        self.d.download(url, zpath)  # OSError = network (the caller maps it to exit 3)
        got = sha256_file(zpath)
        if got != want_sha or (
            asset.get("bytes") is not None and zpath.stat().st_size != int(asset["bytes"])
        ):
            raise VerifyError(
                f"{zpath.name}: sha256 / size differ from latest.json (download damaged or altered)"
            )
        tree = stage / "tree"
        with zipfile.ZipFile(zpath) as zf:
            names = [i.filename for i in zf.infolist() if not i.is_dir()]
            bad = [n for n in names if not safe_member(n)]
            if bad:
                raise VerifyError(f"unsafe path in the ZIP: {bad[0]!r}")
            tops = {n.split("/", 1)[0] for n in names}
            if len(tops) != 1:
                raise VerifyError(f"the ZIP has {len(tops)} top folders, expected one")
            zf.extractall(tree)
            if os.name != "nt":  # extractall drops the mode bits; the .sh launchers must stay executable
                for info in zf.infolist():
                    mode = (info.external_attr >> 16) & 0o777
                    if mode and not info.is_dir():
                        os.chmod(tree / info.filename, mode)
        new_root = tree / tops.pop()
        try:
            new = json.loads((new_root / "MANIFEST.json").read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise VerifyError(f"the new MANIFEST.json is unreadable: {exc}") from exc
        listed = manifest_files(new)
        on_disk = {p.relative_to(new_root).as_posix() for p in new_root.rglob("*") if p.is_file()}
        on_disk.discard("MANIFEST.json")
        extra, missing = sorted(on_disk - set(listed)), sorted(set(listed) - on_disk)
        if extra:
            raise VerifyError(f"in the ZIP, not in its MANIFEST: {extra[0]}")
        if missing:
            raise VerifyError(f"in the MANIFEST, not in the ZIP: {missing[0]}")
        for rel in sorted(on_disk):
            f, p = listed[rel], new_root / rel
            if f.get("size") != p.stat().st_size or f.get("sha256") != sha256_file(p):
                raise VerifyError(f"sha256 / size differ from the MANIFEST: {rel}")
        if str(new.get("version")) != version or str(new.get("os")) != os_kind:
            raise VerifyError(
                f"the ZIP is {new.get('version')} / {new.get('os')}, expected {version} / {os_kind}"
            )
        return new_root, new

    def swap(self, old: dict, new: dict, new_root: Path, backup: Path) -> dict:
        old_ver, new_ver = str(old.get("version")), str(new.get("version"))
        if backup.exists():
            shutil.rmtree(backup)
        out = [r for r in sorted(manifest_files(old)) if not kept(r) and (self.root / r).is_file()]
        if (self.root / "MANIFEST.json").is_file():
            out.append("MANIFEST.json")
        incoming = [*(r for r in sorted(manifest_files(new)) if not kept(r)), "MANIFEST.json"]
        journal = {"old": old_ver, "new": new_ver, "stage": str(new_root), "backup": str(backup),
                   "out": out, "in": incoming, "created": []}  # fmt: skip
        self._write_journal(journal)
        for rel in out:
            self.d.move(self.root / rel, backup / rel)
        for rel in incoming:
            self.d.move(new_root / rel, self.root / rel)
        if (backup / YAML).is_file() and (self.root / YAML).is_file():
            bak = f"{YAML}.bak-{old_ver}"
            journal["created"].append(bak)
            self._write_journal(journal)
            shutil.copyfile(backup / YAML, self.root / bak)
        for rel in out:  # folders the old version had and the new one does not
            parent = (self.root / rel).parent
            while parent != self.root and parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
                parent = parent.parent
        return journal

    def drop_pycache(self) -> None:
        for top in self.root.iterdir():
            if not top.is_dir() or f"{top.name}/".startswith(KEEP_PREFIXES):
                continue
            for d in sorted(top.rglob("__pycache__"), reverse=True):
                shutil.rmtree(d, ignore_errors=True)

    def restore(self, journal: dict, os_kind: str, reason: str) -> int:
        self.say(f"Update failed ({reason}); restoring {journal['old']} ...")
        if not self.stop_service():
            self.say(
                "GAMMA-UNWIND did not stop, so the previous version cannot be restored now. Close it (tray > "
                f"Quit), then run the update again: it restores {journal['old']} first."
            )
            return EXIT_RESTORED
        problems = self.rollback(journal)
        if problems:
            self.say(
                f"Could not restore {len(problems)} file(s): {problems[:3]}. The previous files are in "
                f"{journal['backup']}; run the update again to retry the restore."
            )
            return EXIT_RESTORED
        seen = self.start_service(os_kind, journal["old"])
        self.say(f"Restored {journal['old']}; /api/status reports version {seen or '(no answer yet)'}.")
        return EXIT_RESTORED

    # ---- the whole run
    def run(self) -> int:
        manifest_path = self.root / "MANIFEST.json"
        if (self.root / ".git").exists() or not manifest_path.is_file():
            self.say(
                "This folder is a source checkout, not a client package (no MANIFEST.json): run git pull."
            )
            return EXIT_NOTHING
        old = json.loads(manifest_path.read_text(encoding="utf-8"))
        old_ver, os_kind = str(old.get("version")), str(old.get("os"))
        if not self.releases_url.startswith("https://"):
            self.say(
                "This copy has no releases address (RELEASES_URL), so it cannot look for updates. "
                "Download the new ZIP by hand."
            )
            return EXIT_NETWORK
        try:
            self.recover_interrupted()
        except OSError as exc:
            self.say(f"Could not restore the interrupted update: {exc}")
            return EXIT_RESTORED
        try:
            latest = self.fetch_latest()
        except (OSError, ValueError) as exc:
            self.say(f"Could not read the releases site: {exc}")
            return EXIT_NETWORK
        new_ver = str(latest["version"])
        if vkey(new_ver) <= vkey(old_ver):
            self.say(f"GAMMA-UNWIND {old_ver} is already current (latest published: {new_ver}).")
            return EXIT_NOTHING
        asset = latest["assets"].get(os_kind)
        if not isinstance(asset, dict):
            self.say(f"latest.json {new_ver} has no {os_kind} package.")
            return EXIT_VERIFY
        self.say(f"GAMMA-UNWIND {old_ver} -> {new_ver} is available.")
        if not self.assume_yes and not self.d.ask(
            "Updating stops GAMMA-UNWIND for about a minute (no alerts meanwhile). Update now?"
        ):
            self.say("Nothing changed.")
            return EXIT_NOTHING
        stage = self.work / new_ver
        if stage.exists():
            shutil.rmtree(stage)
        stage.mkdir(parents=True)
        try:
            new_root, new = self.download_and_verify(asset, new_ver, os_kind, stage)
        except VerifyError as exc:
            shutil.rmtree(stage, ignore_errors=True)
            self.say(f"Verify failed: {exc}. Nothing changed.")
            return EXIT_VERIFY
        except (OSError, zipfile.BadZipFile) as exc:
            shutil.rmtree(stage, ignore_errors=True)
            if isinstance(exc, zipfile.BadZipFile):
                self.say(f"Verify failed: the download is not a ZIP ({exc}). Nothing changed.")
                return EXIT_VERIFY
            self.say(f"Download failed: {exc}. Nothing changed.")
            return EXIT_NETWORK
        if interpreter_set(old, os_kind) != interpreter_set(new, os_kind):
            shutil.rmtree(stage, ignore_errors=True)
            what = (
                "bundled Python (python\\)"
                if os_kind == "win64"
                else "library list (requirements-client.txt)"
            )
            self.say(
                f"{new_ver} needs a different {what}, which cannot be replaced while it runs this update. "
                "Download the full ZIP and follow the Updating chapter of the Setup Guide. Nothing changed."
            )
            return EXIT_PYTHON
        if not self.stop_service():
            shutil.rmtree(stage, ignore_errors=True)
            self.say(
                f"GAMMA-UNWIND did not stop: port {self.port} is still in use (D31). Close it (tray > Quit, "
                "or Stop GAMMA) and run the update again. Nothing changed."
            )
            return EXIT_BUSY
        backup = self.work / f"backup-{old_ver}"
        yaml_edited = False
        yf = manifest_files(old).get(YAML)
        if yf is not None and (self.root / YAML).is_file():
            yaml_edited = sha256_file(self.root / YAML) != yf.get("sha256")
        journal: dict = {}
        try:
            journal = self.swap(old, new, new_root, backup)
            self.drop_pycache()
        except Exception as exc:  # noqa: BLE001 -- ANY failure after the swap began is rolled back
            if not journal and self.journal_path.is_file():
                journal = json.loads(self.journal_path.read_text(encoding="utf-8"))
            if not journal:
                self.say(f"Update failed before any file moved ({exc}). Nothing changed.")
                self.start_service(os_kind, old_ver)
                return EXIT_RESTORED
            return self.restore(journal, os_kind, f"{exc.__class__.__name__}: {exc}")
        self.say(f"Installed {new_ver}; starting ...")
        seen = self.start_service(os_kind, new_ver)
        if seen != new_ver:
            return self.restore(journal, os_kind, f"{new_ver} did not answer on port {self.port}")
        self.journal_path.unlink(missing_ok=True)
        shutil.rmtree(stage, ignore_errors=True)
        self.say(f"GAMMA-UNWIND updated {old_ver} -> {new_ver}; /api/status reports version {seen}.")
        bak = f"{YAML}.bak-{old_ver}"
        if (self.root / bak).is_file():
            if yaml_edited:
                self.say(
                    f"{YAML} was replaced by the {new_ver} copy. Your edits are in {bak}: re-apply the "
                    "settings you changed (Setup Guide, entry settings chapter), then Stop and Start."
                )
            else:
                self.say(f"{YAML} was replaced by the {new_ver} copy (the previous copy is {bak}).")
        self.say(f"The previous version is kept in {backup.relative_to(self.root).as_posix()}/.")
        return EXIT_UPDATED


def run(root: Path = ROOT, *, releases_url: str = RELEASES_URL, deps: Deps | None = None,
        assume_yes: bool = False) -> int:  # fmt: skip
    return Updater(root, releases_url, deps or Deps(), assume_yes).run()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Update this GAMMA-UNWIND package in place (exit codes: see top)."
    )
    ap.add_argument("--yes", action="store_true", help="do not ask before stopping the service")
    a = ap.parse_args(argv)
    return run(ROOT, assume_yes=a.yes)


if __name__ == "__main__":
    sys.exit(main())
