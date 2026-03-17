#!/usr/bin/env python3
"""
Sync Claude Code conversation history across machines via Google Drive.

Drive folder structure (organized by normalized git remote, not local path):
  claude-code-history/
    github.com__flashinfer-ai__flashinfer/    # normalized remote URL
      _metadata.json                           # {remote_url, local_paths: [...]}
      abc123.jsonl
      def456.jsonl
    github.com__NVIDIA__cutlass/
      ...

On push: resolves each local project dir to its git remote, uploads under that key.
On pull: for each remote folder, finds the local project dir whose repo matches,
         downloads into it. Skips repos not cloned locally.

Setup:
  1. Enable Google Drive API, create OAuth credentials (desktop app)
  2. pip install google-auth google-auth-oauthlib google-api-python-client
  3. Place credentials.json in this repo (gitignored)
  4. First run will open browser for OAuth consent

Usage:
  python sync_claude_history.py          # bidirectional sync
  python sync_claude_history.py --pull   # only download newer remote files
  python sync_claude_history.py --push   # only upload newer local files
  python sync_claude_history.py --dry-run
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
DRIVE_FOLDER_NAME = "claude-code-history"
SCRIPT_DIR = Path(__file__).parent
TOKEN_PATH = SCRIPT_DIR / "token.json"
CREDENTIALS_PATH = SCRIPT_DIR / "credentials.json"
SERVICE_ACCOUNT_PATH = SCRIPT_DIR / "service-account.json"


# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------

def get_drive_service():
    """Authenticate with Google Drive.

    Tries service account first (headless-friendly), falls back to OAuth.
    """
    # Option 1: Service account (no browser needed, works on all machines)
    if SERVICE_ACCOUNT_PATH.exists():
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
        )
        return build("drive", "v3", credentials=creds)

    # Option 2: OAuth (needs browser on first run per machine)
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_PATH.exists():
                print(f"ERROR: No auth credentials found.")
                print(f"Place one of these in {SCRIPT_DIR}:")
                print(f"  service-account.json  (recommended for headless)")
                print(f"  credentials.json      (OAuth, needs browser once)")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_PATH), SCOPES
            )
            try:
                creds = flow.run_local_server(port=0)
            except OSError:
                # Headless: no browser available, use console-based flow
                print("No browser available. Visit this URL on any device:")
                auth_url, _ = flow.authorization_url(prompt="consent")
                print(f"\n  {auth_url}\n")
                code = input("Enter the authorization code: ").strip()
                flow.fetch_token(code=code)
                creds = flow.credentials
        TOKEN_PATH.write_text(creds.to_json())
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(service, folder_name, parent_id=None):
    q = (
        f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        f" and trashed=false"
    )
    if parent_id:
        q += f" and '{parent_id}' in parents"
    results = service.files().list(q=q, fields="files(id,name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        meta["parents"] = [parent_id]
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


def list_drive_folders(service, parent_id):
    """List subfolders. Returns {name: id}."""
    folders = {}
    page_token = None
    while True:
        results = (
            service.files()
            .list(
                q=f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="nextPageToken, files(id, name)",
                pageSize=1000,
                pageToken=page_token,
            )
            .execute()
        )
        for f in results.get("files", []):
            folders[f["name"]] = f["id"]
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    return folders


def list_remote_files(service, folder_id):
    """List all files in a Drive folder. Returns {name: {id, modifiedTime, md5, size}}."""
    remote = {}
    page_token = None
    while True:
        results = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false"
                f" and mimeType!='application/vnd.google-apps.folder'",
                fields="nextPageToken, files(id, name, modifiedTime, md5Checksum, size)",
                pageSize=1000,
                pageToken=page_token,
            )
            .execute()
        )
        for f in results.get("files", []):
            remote[f["name"]] = f
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    return remote


def upload_string(service, content: str, name: str, folder_id: str, existing_id=None):
    """Upload a string as a file to Drive."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    try:
        media = MediaFileUpload(tmp_path, mimetype="application/json")
        if existing_id:
            service.files().update(fileId=existing_id, media_body=media).execute()
        else:
            service.files().create(
                body={"name": name, "parents": [folder_id]},
                media_body=media,
            ).execute()
    finally:
        os.unlink(tmp_path)


def download_file(service, file_id: str, local_path: Path):
    """Download a file from Drive."""
    request = service.files().get_media(fileId=file_id)
    with open(local_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def download_string(service, file_id: str) -> str:
    """Download a file from Drive as a string."""
    import io
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8")


# ---------------------------------------------------------------------------
# Git remote / local project resolution
# ---------------------------------------------------------------------------

def normalize_git_url(url: str) -> str:
    """Normalize git remote URL to a stable folder name.

    git@github.com:flashinfer-ai/flashinfer.git -> github.com__flashinfer-ai__flashinfer
    https://github.com/flashinfer-ai/flashinfer.git -> github.com__flashinfer-ai__flashinfer
    """
    url = url.strip()
    # SSH format: git@host:org/repo.git
    m = re.match(r"git@([^:]+):(.+?)(?:\.git)?$", url)
    if m:
        host, path = m.group(1), m.group(2)
        return f"{host}__{path.replace('/', '__')}"
    # HTTPS format: https://host/org/repo.git
    m = re.match(r"https?://([^/]+)/(.+?)(?:\.git)?$", url)
    if m:
        host, path = m.group(1), m.group(2)
        return f"{host}__{path.replace('/', '__')}"
    # Fallback: sanitize
    return re.sub(r"[^\w.-]", "__", url)


def resolve_claude_project_path(project_dir_name: str) -> str | None:
    """Convert claude project dir name to local filesystem path.

    Claude encodes /sgl-workspace/cutlass/examples/python/CuTeDSL/blackwell/flash-attention
    as -sgl-workspace-cutlass-examples-python-CuTeDSL-blackwell-flash-attention

    The problem: real directory names can contain hyphens (e.g. flash-attention),
    and Claude also maps underscores to hyphens. We try all possible split points
    and both - and _ variants, checking which paths exist on disk.
    """
    encoded = project_dir_name.lstrip("-")
    segments = encoded.split("-")

    def _resolve(pos: int, current_path: str) -> str | None:
        """Recursively try combining segments with / or - or _ at each split point."""
        if pos == len(segments):
            if Path(current_path).exists():
                return current_path
            return None

        # Option 1: next segment is a new directory component (use /)
        candidate = current_path + "/" + segments[pos]
        result = _resolve(pos + 1, candidate)
        if result:
            return result

        # Option 2: next segment continues current component with hyphen (use -)
        candidate = current_path + "-" + segments[pos]
        result = _resolve(pos + 1, candidate)
        if result:
            return result

        # Option 3: next segment continues current component with underscore (use _)
        candidate = current_path + "_" + segments[pos]
        result = _resolve(pos + 1, candidate)
        if result:
            return result

        return None

    if not segments:
        return None

    # Start with /first-segment as the root
    return _resolve(1, "/" + segments[0])


def find_git_root(path: str) -> str | None:
    """Walk up from path to find the nearest git root."""
    p = Path(path)
    while p != p.parent:
        if (p / ".git").exists():
            return str(p)
        p = p.parent
    return None


def get_git_remote(repo_path: str) -> str | None:
    """Get a remote URL for a local git repo. Tries origin first, then first available."""
    try:
        # Try origin first
        result = subprocess.run(
            ["git", "-C", repo_path, "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()

        # No origin — list all remotes and pick the first
        result = subprocess.run(
            ["git", "-C", repo_path, "remote"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            remotes = result.stdout.strip().split("\n")
            for remote_name in remotes:
                remote_name = remote_name.strip()
                if not remote_name:
                    continue
                url_result = subprocess.run(
                    ["git", "-C", repo_path, "remote", "get-url", remote_name],
                    capture_output=True, text=True, timeout=5,
                )
                if url_result.returncode == 0:
                    return url_result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def rel_path_to_drive_subfolder(rel_path: str) -> str:
    """Convert a relative path within a repo to a Drive subfolder name.

    '.' (repo root) -> '_root'
    'flash_attn/cute' -> 'flash_attn__cute'
    """
    if rel_path == ".":
        return "_root"
    return rel_path.replace("/", "__")


def drive_subfolder_to_rel_path(subfolder: str) -> str:
    """Inverse of rel_path_to_drive_subfolder."""
    if subfolder == "_root":
        return "."
    return subfolder.replace("__", "/")


REPO_CACHE_PATH = SCRIPT_DIR / ".repo_cache.json"


def scan_local_git_repos() -> dict:
    """Scan the parent directory of this script's repo for all git repos.

    Returns: {normalized_git_url: (git_root, raw_url)}
    """
    scan_root = SCRIPT_DIR.parent
    repos = {}
    for dirpath, dirnames, _ in os.walk(scan_root):
        if ".git" in dirnames:
            git_root = dirpath
            git_url = get_git_remote(git_root)
            if git_url:
                key = normalize_git_url(git_url)
                repos[key] = (git_root, git_url)
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
    return repos


def load_repo_cache() -> dict:
    """Load cached project_dir_name -> {git_root, git_url, rel_path} mapping."""
    if REPO_CACHE_PATH.exists():
        try:
            return json.loads(REPO_CACHE_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_repo_cache(cache: dict):
    """Save project_dir_name -> resolved info cache."""
    REPO_CACHE_PATH.write_text(json.dumps(cache, indent=2))


def build_local_index() -> dict:
    """Scan all local claude project dirs.

    Returns: {normalized_git_url: [(project_dir, git_url, git_root, rel_path), ...]}
    - project_dir: Path to ~/.claude/projects/<name>
    - git_url: raw git remote URL
    - git_root: absolute path to the git root
    - rel_path: path from git_root to the project dir (e.g. '.' or 'flash_attn/cute')
    Projects without a git remote are grouped under key None.
    """
    repo_cache = load_repo_cache()
    cache_changed = False

    index = {}
    if not CLAUDE_PROJECTS_DIR.exists():
        return index
    for d in CLAUDE_PROJECTS_DIR.iterdir():
        if not d.is_dir() or d.name.startswith("."):
            continue

        fs_path = resolve_claude_project_path(d.name)
        git_root = find_git_root(fs_path) if fs_path else None
        git_url = get_git_remote(git_root) if git_root else None

        # Fallback: check cache from a previous run
        if not git_url and d.name in repo_cache:
            cached = repo_cache[d.name]
            cached_root = cached.get("git_root")
            if cached_root and Path(cached_root).is_dir():
                git_root = cached_root
                git_url = get_git_remote(git_root)
                fs_path = cached_root
                rel_path_cached = cached.get("rel_path", ".")
                if rel_path_cached != ".":
                    candidate = os.path.join(git_root, rel_path_cached)
                    if Path(candidate).is_dir():
                        fs_path = candidate

        if git_url and git_root and fs_path:
            key = normalize_git_url(git_url)
            rel_path = os.path.relpath(fs_path, git_root)
            if d.name not in repo_cache or repo_cache[d.name].get("git_root") != git_root:
                repo_cache[d.name] = {
                    "git_root": git_root,
                    "git_url": git_url,
                    "rel_path": rel_path,
                }
                cache_changed = True
        else:
            key = None
            rel_path = None

        index.setdefault(key, []).append((d, git_url, git_root, rel_path))

    if cache_changed:
        save_repo_cache(repo_cache)
    return index


def resolve_unmatched_projects(index):
    """Resolve unresolved projects by scanning local sibling repos for matching git remotes.

    For projects from other machines where the encoded path doesn't exist locally,
    scan repos in the parent directory of this script to find one whose git remote
    matches. The project dir name ends with the repo name, so we match on that.
    Once matched, we try to reconstruct the relative path within the repo.
    """
    unresolved = index.pop(None, [])
    if not unresolved:
        return

    local_repos = scan_local_git_repos()
    if not local_repos:
        index.setdefault(None, []).extend(unresolved)
        return

    # Build reverse index: repo basename -> [(normalized_url, git_root, raw_url)]
    # to match project dir names that end with the repo name
    repos_by_name = {}
    for norm_url, (git_root, raw_url) in local_repos.items():
        basename = Path(git_root).name.lower()
        repos_by_name.setdefault(basename, []).append((norm_url, git_root, raw_url))

    repo_cache = load_repo_cache()
    cache_changed = False
    still_unresolved = []

    for project_dir, _, _, _ in unresolved:
        segments = project_dir.name.lstrip("-").split("-")
        matched = False

        # Try matching the tail of the project dir name against repo basenames
        # e.g. -mlx-devbox-users-foo-playground-sglang -> try "sglang"
        # e.g. -mlx-devbox-...-flash-attention-fp4 -> try "fp4", "attention-fp4", "flash-attention-fp4"
        for i in range(len(segments) - 1, max(0, len(segments) - 5) - 1, -1):
            candidate_name = "-".join(segments[i:]).lower()
            if candidate_name in repos_by_name:
                norm_url, git_root, raw_url = repos_by_name[candidate_name][0]

                # Reconstruct relative path: everything between repo name and
                # the end of the project dir path. The segments before the repo
                # name are the machine path, segments after (if any) are subdirs.
                # For now, assume repo root unless we can resolve further.
                rel_path = "."

                # Try to resolve subdir within the repo from remaining segments
                # The repo name matched at position i, so segments after a possible
                # repo-name match could be subdirs
                # e.g. -...-flash-attention-fp4-flash-attn-cute
                #   repo = flash-attention-fp4 (matched at i)
                #   remaining after repo = flash-attn-cute -> flash_attn/cute
                repo_segments = candidate_name.split("-")
                repo_end_idx = i + len(repo_segments)
                if repo_end_idx < len(segments):
                    remaining = segments[repo_end_idx:]
                    # Try to resolve remaining as a subpath within the repo
                    from sync_claude_history import resolve_claude_project_path
                    # Build candidate subpaths
                    sub_encoded = "-".join(remaining)
                    # Try each combo of - / _ / / for the remaining segments
                    def _resolve_sub(pos, current):
                        if pos == len(remaining):
                            full = os.path.join(git_root, current) if current else git_root
                            if Path(full).is_dir():
                                return current or "."
                            return None
                        seg = remaining[pos]
                        for sep in ["/", "-", "_"]:
                            cand = (current + sep + seg) if current else seg
                            result = _resolve_sub(pos + 1, cand)
                            if result:
                                return result
                        return None

                    resolved_sub = _resolve_sub(0, "")
                    if resolved_sub:
                        rel_path = resolved_sub

                key = norm_url
                index.setdefault(key, []).append(
                    (project_dir, raw_url, git_root, rel_path)
                )
                repo_cache[project_dir.name] = {
                    "git_root": git_root,
                    "git_url": raw_url,
                    "rel_path": rel_path,
                }
                cache_changed = True
                matched = True
                break

        if not matched:
            still_unresolved.append((project_dir, None, None, None))

    if still_unresolved:
        index.setdefault(None, []).extend(still_unresolved)

    if cache_changed:
        save_repo_cache(repo_cache)


# ---------------------------------------------------------------------------
# File-level sync logic
# ---------------------------------------------------------------------------

def is_empty_conversation(jsonl_path: Path) -> bool:
    """Check if a conversation is empty (no assistant response).

    Empty conversations are created when you open Claude and immediately exit,
    or only type /resume. They contain only file-history-snapshot, user meta,
    and local-command entries, but no assistant messages.
    """
    try:
        with open(jsonl_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("type") == "assistant":
                    return False
    except (OSError, UnicodeDecodeError):
        pass
    return True


def local_file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def format_size(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes}B"
    elif nbytes < 1024 * 1024:
        return f"{nbytes / 1024:.0f}KB"
    else:
        return f"{nbytes / (1024 * 1024):.1f}MB"


def format_time(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def get_conversation_title(jsonl_path: Path) -> str | None:
    """Extract the conversation title from a JSONL file.

    Looks for custom-title (user rename) first, falls back to slug (auto-generated).
    """
    custom_title = None
    slug = None
    try:
        with open(jsonl_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("type") == "custom-title":
                    custom_title = entry.get("customTitle")
                if slug is None and "slug" in entry:
                    slug = entry["slug"]
    except (OSError, UnicodeDecodeError):
        pass
    return custom_title or slug


def inject_custom_title(jsonl_path: Path, session_id: str, title: str):
    """Set the custom-title in a JSONL file. Replaces existing custom-title if present."""
    lines = []
    found = False
    new_entry = json.dumps({
        "type": "custom-title",
        "customTitle": title,
        "sessionId": session_id,
    })
    try:
        with open(jsonl_path, "r") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    lines.append(line)
                    continue
                try:
                    entry = json.loads(stripped)
                except json.JSONDecodeError:
                    lines.append(line)
                    continue
                if entry.get("type") == "custom-title":
                    if entry.get("customTitle") == title:
                        return  # already correct, don't rewrite
                    # Replace with new title
                    lines.append(new_entry + "\n")
                    found = True
                else:
                    lines.append(line)
    except (OSError, UnicodeDecodeError):
        return

    if not found:
        lines.append(new_entry + "\n")

    with open(jsonl_path, "w") as f:
        f.writelines(lines)


def sync_files(service, folder_id, local_dir: Path, args, indent="    "):
    """Sync .jsonl files between local_dir and a Drive folder. Returns (pushed, pulled, skipped)."""
    remote_files = list_remote_files(service, folder_id)
    local_jsons = {p.name: p for p in sorted(local_dir.glob("*.jsonl"))
                   if not is_empty_conversation(p)}

    pushed = pulled = skipped = 0

    # Load/create titles mapping: {session_id: title}
    titles_file = remote_files.get("_titles.json")
    remote_titles = {}
    if titles_file:
        try:
            remote_titles = json.loads(download_string(service, titles_file["id"]))
        except (json.JSONDecodeError, Exception):
            pass
    local_titles = {}
    titles_changed = False

    # Sync files that exist locally
    for fname, local_path in local_jsons.items():
        local_md5 = local_file_md5(local_path)
        local_mtime = local_path.stat().st_mtime
        local_size = local_path.stat().st_size

        if fname in remote_files:
            remote = remote_files[fname]
            if local_md5 == remote.get("md5Checksum", ""):
                skipped += 1
                continue

            remote_mtime = datetime.fromisoformat(
                remote["modifiedTime"].replace("Z", "+00:00")
            ).timestamp()

            if local_mtime > remote_mtime and not args.pull_only:
                action = "WOULD PUSH" if args.dry_run else "PUSHED"
                if not args.dry_run:
                    media = MediaFileUpload(str(local_path))
                    service.files().update(fileId=remote["id"], media_body=media).execute()
                print(f"{indent}[{action}] {fname} ({format_size(local_size)}, {format_time(local_mtime)})")
                pushed += 1
            elif remote_mtime > local_mtime and not args.push_only:
                remote_size = int(remote.get("size", 0))
                action = "WOULD PULL" if args.dry_run else "PULLED"
                if not args.dry_run:
                    download_file(service, remote["id"], local_path)
                print(f"{indent}[{action}] {fname} ({format_size(remote_size)}, {format_time(remote_mtime)})")
                pulled += 1
            else:
                skipped += 1
        elif not args.pull_only:
            action = "WOULD PUSH NEW" if args.dry_run else "PUSHED NEW"
            if not args.dry_run:
                media = MediaFileUpload(str(local_path))
                service.files().create(
                    body={"name": fname, "parents": [folder_id]},
                    media_body=media,
                ).execute()
            print(f"    [{action}] {fname} ({format_size(local_size)}, {format_time(local_mtime)})")
            pushed += 1

    # Extract titles from local files we just pushed (or all local files for title sync)
    if not args.pull_only:
        for fname, local_path in local_jsons.items():
            session_id = fname.replace(".jsonl", "")
            title = get_conversation_title(local_path)
            if title:
                if remote_titles.get(session_id) != title:
                    remote_titles[session_id] = title
                    titles_changed = True

    # Pull files that exist only on remote
    if not args.push_only:
        for fname, remote in remote_files.items():
            if fname.startswith("_") or not fname.endswith(".jsonl"):
                continue
            if fname not in local_jsons:
                remote_size = int(remote.get("size", 0))
                remote_mtime = datetime.fromisoformat(
                    remote["modifiedTime"].replace("Z", "+00:00")
                ).timestamp()
                action = "WOULD PULL NEW" if args.dry_run else "PULLED NEW"
                if not args.dry_run:
                    download_file(service, remote["id"], local_dir / fname)
                    # Inject saved title into the downloaded conversation
                    session_id = fname.replace(".jsonl", "")
                    saved_title = remote_titles.get(session_id)
                    if saved_title:
                        inject_custom_title(local_dir / fname, session_id, saved_title)
                print(f"{indent}[{action}] {fname} ({format_size(remote_size)}, {format_time(remote_mtime)})")
                pulled += 1

    # Also inject titles into existing local files that were pulled (updated)
    if not args.push_only and not args.dry_run:
        for fname, local_path in local_jsons.items():
            session_id = fname.replace(".jsonl", "")
            saved_title = remote_titles.get(session_id)
            if saved_title:
                inject_custom_title(local_path, session_id, saved_title)

    # Upload updated titles mapping
    if titles_changed and not args.dry_run:
        upload_string(
            service,
            json.dumps(remote_titles, indent=2),
            "_titles.json",
            folder_id,
            existing_id=titles_file["id"] if titles_file else None,
        )

    return pushed, pulled, skipped


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sync Claude Code history via Google Drive")
    parser.add_argument("--pull", dest="pull_only", action="store_true", help="Only download")
    parser.add_argument("--push", dest="push_only", action="store_true", help="Only upload")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if not CLAUDE_PROJECTS_DIR.exists():
        print(f"No Claude projects dir at {CLAUDE_PROJECTS_DIR}")
        sys.exit(1)

    service = get_drive_service()
    root_folder_id = get_or_create_folder(service, DRIVE_FOLDER_NAME)

    # Build local index: git_url_key -> [(project_dir, raw_git_url), ...]
    local_index = build_local_index()

    # Resolve unmatched projects (from other machines) by scanning sibling repos
    if None in local_index:
        resolve_unmatched_projects(local_index)

    git_projects = {k: v for k, v in local_index.items() if k is not None}
    no_git = local_index.get(None, [])

    print(f"Found {sum(len(v) for v in git_projects.values())} projects with git remotes, "
          f"{len(no_git)} without")
    for d, _, _, _ in no_git:
        print(f"  [SKIP no git] {d.name}")

    # --- PUSH: upload organized by git remote + relative path subfolder ---
    # Drive structure:
    #   claude-code-history/
    #     github.com__org__repo/
    #       _metadata.json
    #       _root/                    # conversations at repo root
    #         abc.jsonl
    #       flash_attn__cute/         # conversations in flash_attn/cute/
    #         def.jsonl
    if not args.pull_only:
        for url_key, entries in sorted(git_projects.items()):
            repo_folder_id = get_or_create_folder(service, url_key, root_folder_id)
            raw_url = entries[0][1]

            # Update metadata
            meta = {
                "remote_url": raw_url,
                "normalized_key": url_key,
                "subfolders": [
                    {"rel_path": rel, "local_project_dir": str(d)}
                    for d, _, _, rel in entries
                ],
            }
            repo_files = list_remote_files(service, repo_folder_id)
            upload_string(
                service,
                json.dumps(meta, indent=2),
                "_metadata.json",
                repo_folder_id,
                existing_id=repo_files.get("_metadata.json", {}).get("id"),
            )

            B = "  ╠═══════════════════════════════════════════════════════════════════════════"
            if url_key == sorted(git_projects.keys())[0]:
                print(B)
            git_root = entries[0][2]
            print(f"  ║ {raw_url}")
            print(f"  ║   ╰─> {git_root}")
            print(f"  ║ ----------------------------------------------------------------------")
            for project_dir, _, _, rel_path in entries:
                subfolder_name = rel_path_to_drive_subfolder(rel_path)
                subfolder_id = get_or_create_folder(
                    service, subfolder_name, repo_folder_id
                )

                local_jsonls = [p for p in project_dir.glob("*.jsonl") if not is_empty_conversation(p)]
                local_count = len(local_jsonls)
                local_size = sum(p.stat().st_size for p in local_jsonls)
                remote_sub_files = list_remote_files(service, subfolder_id)
                remote_count = sum(1 for k in remote_sub_files if k.endswith(".jsonl"))
                remote_size = sum(
                    int(f.get("size", 0)) for k, f in remote_sub_files.items()
                    if k.endswith(".jsonl")
                )

                subdir_label = "." if rel_path == "." else rel_path
                print(f"  ║ {subdir_label:<35s} {local_count:>2} local ({format_size(local_size):>8})  {remote_count:>2} remote ({format_size(remote_size):>8})")

                if args.verbose:
                    for jsonl_path in sorted(local_jsonls):
                        sid = jsonl_path.stem
                        title = get_conversation_title(jsonl_path)
                        size = format_size(jsonl_path.stat().st_size)
                        mtime = format_time(jsonl_path.stat().st_mtime)
                        title_str = f'"{title}"' if title else "(untitled)"
                        print(f"  ║   ╰─ {sid[:8]}…  {title_str:<30s} {size:>8}  {mtime}")

                pushed, pulled, skipped = sync_files(
                    service, subfolder_id, project_dir, args, indent="  ║   "
                )
                if pushed or pulled:
                    if args.dry_run:
                        print(f"  ║   => would push {pushed}, would pull {pulled}, {skipped} unchanged")
                    else:
                        print(f"  ║   => {pushed} pushed, {pulled} pulled, {skipped} unchanged")
            print(B)

    # --- PULL: download from remote into matching local project dirs ---
    if not args.push_only:
        remote_repo_folders = list_drive_folders(service, root_folder_id)

        # Build reverse index: for each local git root, map rel_path -> project_dir
        # so we can match remote subfolders to local dirs
        local_by_url = {}  # url_key -> {rel_path: (project_dir, git_root)}
        for url_key, entries in git_projects.items():
            for project_dir, _, git_root, rel_path in entries:
                local_by_url.setdefault(url_key, {})[rel_path] = (
                    project_dir, git_root
                )

        pull_printed_first = False
        for url_key, repo_folder_id in sorted(remote_repo_folders.items()):
            # Already synced in push phase (bidirectional)?
            if url_key in git_projects and not args.pull_only:
                continue

            # Read metadata to get the raw URL
            repo_files = list_remote_files(service, repo_folder_id)
            meta_file = repo_files.get("_metadata.json")
            raw_url = url_key
            if meta_file:
                meta = json.loads(download_string(service, meta_file["id"]))
                raw_url = meta.get("remote_url", url_key)

            if url_key not in local_by_url:
                remote_subfolders = list_drive_folders(service, repo_folder_id)
                total_convos = 0
                total_size = 0
                for sf_name, sf_id in remote_subfolders.items():
                    sf_files = list_remote_files(service, sf_id)
                    total_convos += sum(1 for k in sf_files if k.endswith(".jsonl"))
                    total_size += sum(
                        int(f.get("size", 0)) for k, f in sf_files.items()
                        if k.endswith(".jsonl")
                    )
                B = "  ╠═══════════════════════════════════════════════════════════════════════════"
                if not pull_printed_first:
                    print(B)
                    pull_printed_first = True
                print(f"  ║ {raw_url}  (no local clone)")
                print(f"  ║ ----------------------------------------------------------------------")
                print(f"  ║ {'.':<35s} {total_convos:>2} remote ({format_size(total_size):>8})")
                continue

            local_map = local_by_url[url_key]
            remote_subfolders = list_drive_folders(service, repo_folder_id)

            B = "  ╠═══════════════════════════════════════════════════════════════════════════"
            pull_git_root = None
            for rp, (pd, gr) in local_map.items():
                pull_git_root = gr
                break
            if not pull_printed_first:
                print(B)
                pull_printed_first = True
            print(f"  ║ {raw_url}")
            if pull_git_root:
                print(f"  ║   ╰─> {pull_git_root}")
            print(f"  ║ ----------------------------------------------------------------------")
            for subfolder_name, subfolder_id in remote_subfolders.items():
                rel_path = drive_subfolder_to_rel_path(subfolder_name)

                remote_sub_files = list_remote_files(service, subfolder_id)
                remote_count = sum(1 for k in remote_sub_files if k.endswith(".jsonl"))
                remote_size = sum(
                    int(f.get("size", 0)) for k, f in remote_sub_files.items()
                    if k.endswith(".jsonl")
                )

                subdir_label = "." if rel_path == "." else rel_path

                if rel_path in local_map:
                    project_dir, _ = local_map[rel_path]
                    local_count = len(list(project_dir.glob("*.jsonl")))
                    local_size = sum(p.stat().st_size for p in project_dir.glob("*.jsonl"))

                    print(f"  ║ {subdir_label:<35s} {local_count:>2} local ({format_size(local_size):>8})  {remote_count:>2} remote ({format_size(remote_size):>8})")

                    if args.verbose:
                        for jsonl_path in sorted(project_dir.glob("*.jsonl")):
                            sid = jsonl_path.stem
                            title = get_conversation_title(jsonl_path)
                            size = format_size(jsonl_path.stat().st_size)
                            mtime = format_time(jsonl_path.stat().st_mtime)
                            title_str = f'"{title}"' if title else "(untitled)"
                            print(f"  ║   ╰─ {sid[:8]}…  {title_str:<30s} {size:>8}  {mtime}")

                    pushed, pulled, skipped = sync_files(
                        service, subfolder_id, project_dir, args, indent="  ║   "
                    )
                    if pushed or pulled:
                        if args.dry_run:
                            print(f"  ║   => would push {pushed}, would pull {pulled}, {skipped} unchanged")
                        else:
                            print(f"  ║   => {pushed} pushed, {pulled} pulled, {skipped} unchanged")
                else:
                    print(f"  ║ {subdir_label:<35s} {'--':>17}  {remote_count:>2} remote ({format_size(remote_size):>8})  (no local project)")
            print(B)

    print("Done.")


if __name__ == "__main__":
    main()
