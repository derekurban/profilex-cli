# Command Reference

## `profilex add <tool> <profile> [--isolated]`

Create profile, install shim, and by default wire shared session/history storage for the tool.
On first use via the shim, the tool's native auth flow runs.

- `--isolated` keeps session/history storage private for this profile.

## `profilex list [--tool claude|codex] [--json]`

List profiles with status and default marker.

## `profilex use <tool> <profile>`

Set default profile for a tool.

## `profilex run <tool> [profile] -- [tool args...]`

Run a tool in selected/default profile context.

Examples:

```bash
profilex run claude personal -- --model sonnet
profilex run codex -- --profile deep-review
```

## `profilex rename <tool> <old-profile> <new-profile>`

Rename profile and move profile directory.

## `profilex remove <tool> <profile> [--purge]`

Remove profile from registry.

`--purge` also deletes profile directory.

## `profilex uninstall [--purge]`

Uninstall ProfileX from the local machine.

- Removes the installed `profilex` binary when it can be resolved automatically.
- Removes ProfileX-generated shims by default.
- `--purge` also removes ProfileX state (`~/.profilex` or `PROFILEX_HOME`/`--root`).

## `profilex shim install [--dir <path>]`

Generate launcher shims for all profiles.

## `profilex shim uninstall --all [--dir <path>]`

Remove all ProfileX-generated shims in a directory.

## `profilex shim uninstall <tool> <profile> [--dir <path>]`

Remove one specific generated shim.

## `profilex usage export [--out <file>] [--deep] [--max-files <n>] [--timezone <tz>] [--cost-mode <mode>]`

Export a unified local usage bundle JSON that ProfileX-UI can ingest directly.

Defaults:

- `--out ./public/local-unified-usage.json`
- `--max-files 5000`
- `--cost-mode auto`

Options:

- `--deep` expands scan to broader home-directory candidates
- `--cost-mode` accepts `auto|calculate|display`
