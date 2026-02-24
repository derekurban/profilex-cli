# ProfileX Architecture

ProfileX has three core layers:

1. **CLI layer (`internal/cli`)**
   - Parses commands/flags and dispatches to app logic.
2. **App layer (`internal/app`)**
   - Implements profile lifecycle and orchestration.
3. **Adapter layer (`internal/adapters`)**
   - Encapsulates tool-specific behavior.
   - Claude: `CLAUDE_CONFIG_DIR`
   - Codex: `CODEX_HOME`
4. **Persistence layer (`internal/store`)**
   - Reads/writes `state.json`.

## Data model

`state.json`:

- version
- defaults map (`tool -> profile`)
- profile list (`tool`, `name`, `dir`, `created_at`)

## Runtime model

- `profilex run ...` resolves tool + profile context.
- Adapter injects environment variable for that profile directory.
- Tool is launched normally (`claude` or `codex`) with isolated config context.

## Shims

`internal/shim` generates launcher scripts:

- Unix/macOS/Linux: `claude-<profile>`, `codex-<profile>`
- Windows: `claude-<profile>.cmd`, `codex-<profile>.cmd`

Each shim:

```bash
# 1) Resolve and export profile env from ProfileX
profilex shim env <tool> <profile>

# 2) Launch the tool directly with that env
<tool> "$@"
```
