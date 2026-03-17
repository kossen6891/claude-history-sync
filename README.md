<p align="center">
  <img src="assets/claude-app-icon.png" width="120" alt="Claude">
  <br><br>
  <h1 align="center">🔄 Claude Code History Sync 🔄</h1>
  <h3 align="center">🤩 Never lose a conversation again! Sync across all your machines via Google Drive ☁️</h3>
</p>

<p align="center">
  <a href="#setup-one-time">🔧 Setup</a> •
  <a href="#-usage">🚀 Usage</a> •
  <a href="#-how-it-works">🧠 How it works</a> •
  <a href="#-storage">💾 Storage</a>
</p>

---

> 🤔 Ever SSH into a different machine and can't find that conversation where Claude wrote you the perfect kernel?

Conversations are organized by **git remote URL**, so they follow the repo — not the local path. Clone `flashinfer` at `/home/alice/flashinfer` on your laptop and `/workspace/flashinfer` on a GPU box? ✨ Same conversations, synced automatically.

🏷️ Conversation names (from `/rename`) are preserved across machines — no more mystery slugs like `fuzzy-dancing-penguin`!

## 🧠 How it works

```
☁️ Google Drive: claude-code-history/
  📁 github.com__org__repo/
     📁 _root/              ← conversations opened at repo root
        💬 abc123.jsonl
     📁 src__subdir/         ← conversations opened in src/subdir/
        💬 def456.jsonl
     📄 _titles.json         ← 🏷️ conversation names
```

| | |
|---|---|
| ⬆️ **Push** | Scans `~/.claude/projects/`, resolves each to its git remote, uploads to the matching Drive folder + subfolder by relative path |
| ⬇️ **Pull** | Finds the local repo with the same git remote, downloads into the correct `~/.claude/projects/` dir |
| 🔄 **Sync** | MD5 checksums skip identical files; when files differ, newer modification time wins |
| 🏷️ **Names** | Conversation titles (from `/rename`) are synced via `_titles.json` and injected on pull |

> 🙈 Projects without a git remote are skipped (no way to match across machines).
>
> 🗑️ Empty conversations (immediate exit, `/resume` only) are automatically skipped.

## 🔧 Setup (one-time)

### 1️⃣ Google Cloud Console

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create or select a project
3. **APIs & Services → Library** → search "Google Drive API" → **Enable**
4. **APIs & Services → Credentials** → **Create Credentials → OAuth client ID**
5. If prompted, configure **OAuth consent screen**: User type = External, add your email as test user
6. Application type: **Desktop app** → Create → **Download JSON**
7. Save as `credentials.json` in this directory

### 2️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 3️⃣ First run

```bash
python sync_claude_history.py
```

Opens a browser for OAuth consent, saves `token.json` locally. Subsequent runs reuse the token. 🎉

> 🖥️ **Headless / SSH**: No browser? No problem! It prints a URL — open on any device, paste the auth code back.
>
> 🌐 **Web VS Code** (code-server, Codespaces): Forward port `8090` first (Ctrl+Shift+P → "Forward a Port" → `8090`), then run the script and open the printed URL in your local browser.
>
> 💻 **Multiple machines**: Either run the OAuth flow on each machine, or copy `token.json` from one that has it.

## 🚀 Usage

```bash
python sync_claude_history.py              # 🔄 bidirectional sync (newer wins)
python sync_claude_history.py --push       # ⬆️  upload only
python sync_claude_history.py --pull       # ⬇️  download only
python sync_claude_history.py --dry-run    # 👀 preview what would happen
python sync_claude_history.py -v           # 📋 verbose: list each conversation
```

### Example output

**Default** (`--dry-run`):

```
Found 3 projects with git remotes, 1 without
  [SKIP no git] -home-user-scratch
  ╠═══════════════════════════════════════════════════════════════════════════
  ║ git@github.com:user/my-project.git
  ║   ╰─> /home/user/my-project
  ║ ----------------------------------------------------------------------
  ║ .                                    2 local (   8.3MB)   1 remote (   3.1MB)
  ║   [WOULD PUSH] a1b2c3d4-...-e5f6.jsonl (5.2MB, 2026-03-16 14:30)
  ║   => would push 1, would pull 0, 1 unchanged
  ╠═══════════════════════════════════════════════════════════════════════════
  ║ git@github.com:org/mono-repo.git
  ║   ╰─> /home/user/mono-repo
  ║ ----------------------------------------------------------------------
  ║ .                                    1 local (   2.1MB)   1 remote (   2.1MB)
  ║ src/frontend                         3 local (  45.6MB)   2 remote (  12.0MB)
  ║   [WOULD PUSH] f7e8d9c0-...-a1b2.jsonl (33.6MB, 2026-03-16 18:05)
  ║   => would push 1, would pull 0, 2 unchanged
  ╠═══════════════════════════════════════════════════════════════════════════
Done.
```

**Verbose** (`--dry-run -v`):

```
Found 3 projects with git remotes, 1 without
  [SKIP no git] -home-user-scratch
  ╠═══════════════════════════════════════════════════════════════════════════
  ║ git@github.com:user/my-project.git
  ║   ╰─> /home/user/my-project
  ║ ----------------------------------------------------------------------
  ║ .                                    2 local (   8.3MB)   1 remote (   3.1MB)
  ║   ╰─ a1b2c3d4…  "refactor-auth-module"            5.2MB  2026-03-16 14:30
  ║   ╰─ b2c3d4e5…  "fix-login-bug"                   3.1MB  2026-03-15 09:12
  ║   [WOULD PUSH] a1b2c3d4-...-e5f6.jsonl (5.2MB, 2026-03-16 14:30)
  ║   => would push 1, would pull 0, 1 unchanged
  ╠═══════════════════════════════════════════════════════════════════════════
  ║ git@github.com:org/mono-repo.git
  ║   ╰─> /home/user/mono-repo
  ║ ----------------------------------------------------------------------
  ║ .                                    1 local (   2.1MB)   1 remote (   2.1MB)
  ║   ╰─ c3d4e5f6…  "update-ci-pipeline"              2.1MB  2026-03-14 11:00
  ║ src/frontend                         3 local (  45.6MB)   2 remote (  12.0MB)
  ║   ╰─ d4e5f6a7…  (untitled)                        1.2MB  2026-03-10 16:45
  ║   ╰─ e5f6a7b8…  "debug-react-ssr"                11.8MB  2026-03-13 20:30
  ║   ╰─ f7e8d9c0…  "perf-optimize-bundle"           33.6MB  2026-03-16 18:05
  ║   [WOULD PUSH] f7e8d9c0-...-a1b2.jsonl (33.6MB, 2026-03-16 18:05)
  ║   => would push 1, would pull 0, 2 unchanged
  ╠═══════════════════════════════════════════════════════════════════════════
Done.
```

## 💾 Storage

Google Drive free tier gives **15GB**. Claude conversation files are typically 1–50MB each. Monitor usage at [drive.google.com/settings/storage](https://drive.google.com/settings/storage).
