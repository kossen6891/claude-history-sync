# CLAUDE.md

## Project Overview

CLI tool to sync Claude Code conversation history across machines via Google Drive, organized by git remote URL.

## Development Rules

### Testing

Every change must be tested with all flag combinations before committing:

```bash
python sync_claude_history.py --dry-run       # verify no actual uploads/downloads
python sync_claude_history.py --dry-run -v    # verify verbose conversation listing
python sync_claude_history.py --push          # verify upload only
python sync_claude_history.py --pull          # verify download only
python sync_claude_history.py                 # verify bidirectional sync
```

Check that:
- Output formatting (borders, alignment, tree characters) renders correctly and is not corrupted
- Conversation titles are extracted and injected correctly
- No doubled `╠═══` borders between repos
- Conversation counts and sizes are accurate
- Subdir paths resolve correctly for repos with hyphens/underscores in directory names

### Security (CRITICAL)

- **NEVER** commit `credentials.json`, `token.json`, or `service-account.json`
- Verify `.gitignore` blocks these before every push
- If credentials are accidentally committed, scrub with `git filter-branch` and force push immediately

### Commits

- Write concise commit messages documenting the functional change, not implementation details
- For related changes to one feature, squash into one commit instead of committing multiple times for compact history
- Update `CHANGELOG.md` with every commit under the current version section
- Keep changelog entries as single-line bullet points
- Update `README.md` when adding new flags, features, or changing output format

### Code Style

- Keep the output formatting consistent: `╠═══` borders, `║` left edge, `╰─` tree branches
- Use `format_size()` and `format_time()` for all human-readable output
- The `sync_files()` function takes an `indent` parameter — pass the correct prefix to maintain alignment
