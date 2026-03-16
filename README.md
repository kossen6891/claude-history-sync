# Claude Code History Sync

Sync Claude Code conversation history across machines via Google Drive, organized by git remote URL so conversations follow the repo regardless of local clone path.

## How it works

```
Drive: claude-code-history/
  github.com__org__repo/
    _metadata.json
    _root/              # conversations opened at repo root
      abc123.jsonl
    src__subdir/         # conversations opened in src/subdir/
      def456.jsonl
```

- **Push**: Scans `~/.claude/projects/`, resolves each to its git remote, uploads conversations to the matching Drive folder + subfolder by relative path.
- **Pull**: For each Drive folder, finds the local repo with the same git remote, downloads conversations into the correct `~/.claude/projects/` dir based on the relative path within the repo.
- **Conflict resolution**: MD5 checksums skip identical files; when files differ, newer modification time wins.
- Projects without a git remote are skipped (no way to match across machines).

## Setup (one-time)

### 1. Google Cloud Console

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create or select a project
3. **APIs & Services > Library** > search "Google Drive API" > **Enable**
4. **APIs & Services > Credentials** > **Create Credentials > OAuth client ID**
5. If prompted, configure **OAuth consent screen**: User type = External, add your email as test user
6. Application type: **Desktop app** > Create > **Download JSON**
7. Save as `credentials.json` in this directory

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. First run (needs browser once)

```bash
python sync_claude_history.py
```

Opens a browser for OAuth consent, saves `token.json` locally. Subsequent runs reuse the token.

**Headless machines**: If no browser is available, it prints a URL to open on any device. Paste the authorization code back into the terminal.

**Multiple machines**: Either run the OAuth flow on each machine, or copy `token.json` from a machine that has it.

## Usage

```bash
python sync_claude_history.py          # bidirectional sync (newer wins)
python sync_claude_history.py --push   # upload only
python sync_claude_history.py --pull   # download only
python sync_claude_history.py --dry-run  # preview what would happen
python sync_claude_history.py -v       # verbose output
```

### Example output

```
Found 4 projects with git remotes, 1 without
  [SKIP no git] -sgl-workspace-cutlass-code-agent
  ╠═══════════════════════════════════════════════════════════════════════════
  ║ git@github.com:user/flashinfer.git
  ║   ╰─> /sgl-workspace/flashinfer
  ║ ----------------------------------------------------------------------
  ║ .                                    1 local (   4.8MB)   1 remote (   3.8MB)
  ║   [WOULD PUSH] df9a6a22-...-.jsonl (4.8MB, 2026-03-16 23:14)
  ║   => 1 pushed, 0 pulled, 0 unchanged
  ╠═══════════════════════════════════════════════════════════════════════════
  ║ git@github.com:org/flash-attention.git
  ║   ╰─> /home/user/repos/flash-attention
  ║ ----------------------------------------------------------------------
  ║ .                                    0 local (      0B)   0 remote (      0B)
  ║ flash_attn/cute                      5 local ( 161.8MB)   5 remote ( 161.8MB)
  ╠═══════════════════════════════════════════════════════════════════════════
Done.
```

## Storage

Google Drive free tier: 15GB. Claude conversation files are typically 1-50MB each. Monitor usage at [drive.google.com/settings/storage](https://drive.google.com/settings/storage).
