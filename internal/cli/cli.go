package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/derekurban/profilex-cli/internal/adapters"
	"github.com/derekurban/profilex-cli/internal/app"
	"github.com/derekurban/profilex-cli/internal/shim"
	"github.com/derekurban/profilex-cli/internal/store"
)

// version is injected at build time via ldflags.
var version = "dev"

const ownershipMarkerMagic = "profilex-owned-binary-v1"

// Run parses os.Args[1:] and dispatches to the appropriate command.
// Returns the process exit code.
func Run(args []string) int {
	rootDir, args := extractFlag(args, "--root")
	cmd := "tui"
	rest := []string{}
	if len(args) > 0 {
		cmd = args[0]
		rest = args[1:]
	}

	if cmd == "-h" || cmd == "--help" || cmd == "help" {
		printHelp()
		return 0
	}
	if cmd == "--version" || cmd == "version" {
		fmt.Printf("profilex %s\n", resolvedVersion())
		return 0
	}

	var err error
	switch cmd {
	case "add":
		err = cmdAdd(rootDir, rest)
	case "remove":
		err = cmdRemove(rootDir, rest)
	case "uninstall":
		err = cmdUninstall(rootDir, rest)
	case "list":
		err = cmdList(rootDir, rest)
	case "run":
		err = cmdRun(rootDir, rest)
	case "use":
		err = cmdUse(rootDir, rest)
	case "rename":
		err = cmdRename(rootDir, rest)
	case "shim":
		err = cmdShim(rootDir, rest)
	case "usage":
		err = cmdUsage(rootDir, rest)
	case "settings":
		err = cmdSettings(rootDir, rest)
	case "tui":
		err = cmdTUI(rootDir, rest)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printHelp()
		return 1
	}

	if err != nil {
		var codeErr app.ExitCodeError
		if errors.As(err, &codeErr) {
			return codeErr.Code
		}
		fmt.Fprintf(os.Stderr, "%s %s\n", Red("error:"), err)
		return 1
	}
	return 0
}

func printHelp() {
	fmt.Printf(`%s - profile manager for Claude Code & Codex CLI

%s
  profilex                         Launch interactive TUI
  profilex <command> [options]

%s
  add <tool> <profile> [--isolated] [--no-shared-skills]  Create a new profile and install its shim
  remove <tool> <profile>       Remove a profile and its shim
  uninstall [--purge]           Uninstall profilex from this machine
  list [--tool <t>] [--json]    List all profiles with auth status
  use <tool> <profile>          Set the default profile for a tool
  rename <tool> <old> <new>     Rename a profile
  run <tool> [profile] -- ...   Run a tool with the given profile
  settings <subcommand>         Manage settings snapshots/presets/apply
  shim install [--dir <d>]      Reinstall shims for all profiles
  shim uninstall [--all]        Remove shims
  tui                           Launch interactive terminal UI
  usage export [options]        Export unified local usage bundle (for ProfileX-UI)

%s
  --root <dir>     Override state directory (default: ~/.profilex)

%s
  profilex add claude work
  profilex add codex personal
  profilex list
  profilex use claude work
  claude-work                   %s
`,
		Bold("profilex"),
		Bold("Usage:"),
		Bold("Commands:"),
		Bold("Global options:"),
		Bold("Examples:"),
		Dim("(runs Claude Code with the 'work' profile)"),
	)
}

// --- add ---

func cmdAdd(rootDir string, args []string) error {
	isolated, args := extractBool(args, "--isolated")
	noSharedSkills, args := extractBool(args, "--no-shared-skills")

	if hasHelp(args) || len(args) < 2 {
		fmt.Printf("Usage: profilex add <tool> <profile> [--isolated] [--no-shared-skills]\n\n")
		fmt.Printf("Supported tools: %s\n", strings.Join(toolNames(), ", "))
		fmt.Printf("\n")
		fmt.Printf("  --isolated          Keep session/history storage private for this profile\n")
		fmt.Printf("  --no-shared-skills  Keep skills private for this profile\n")
		return nil
	}

	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	profile, created, err := mgr.EnsureProfile(tool, args[1])
	if err != nil {
		return err
	}

	if !created {
		fmt.Printf("%s Profile %s already exists\n", Yellow("!"), Bold(string(tool)+"/"+profile.Name))
		return nil
	}

	sharedDir := ""
	sharedErr := error(nil)
	if !isolated {
		sharedDir, sharedErr = mgr.EnableSharedSessions(profile)
	}
	sharedSkillsDir := ""
	sharedSkillsErr := error(nil)
	if !noSharedSkills {
		sharedSkillsDir, sharedSkillsErr = mgr.EnableSharedSkills(profile)
	}

	shimPath, shimErr := installShimForProfile(profile)

	fmt.Printf("%s Created profile %s\n", Green("✓"), Bold(string(tool)+"/"+profile.Name))
	fmt.Printf("   📁 Config: %s\n", Dim(profile.Dir))

	if isolated {
		fmt.Printf("   🔒 Sessions: isolated (no shared session link)\n")
	} else if sharedErr != nil {
		fmt.Printf("   %s Shared sessions not enabled: %v\n", Yellow("⚠"), sharedErr)
	} else {
		fmt.Printf("   🔁 Shared sessions: %s\n", Dim(sharedDir))
	}

	if noSharedSkills {
		fmt.Printf("   🔒 Skills: isolated (no shared skills link)\n")
	} else if sharedSkillsErr != nil {
		fmt.Printf("   %s Shared skills not enabled: %v\n", Yellow("⚠"), sharedSkillsErr)
	} else {
		fmt.Printf("   🧠 Shared skills: %s\n", Dim(sharedSkillsDir))
	}

	if shimErr != nil {
		fmt.Printf("   %s Could not install shim: %v\n", Yellow("⚠"), shimErr)
	} else {
		fmt.Printf("   🔗 Shim:   %s\n", Cyan(shimPath))
	}

	shimName := shim.Name(tool, profile.Name)
	fmt.Println()
	fmt.Printf("   💡 Run %s to launch %s with this profile.\n", Bold(shimName), tool)
	fmt.Printf("      On first run you'll be prompted to authenticate.\n")

	return nil
}

// --- remove ---

func cmdRemove(rootDir string, args []string) error {
	purge, args := extractBool(args, "--purge")

	if hasHelp(args) || len(args) < 2 {
		fmt.Printf("Usage: profilex remove <tool> <profile> [--purge]\n")
		return nil
	}

	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	// Remove shim first (best-effort)
	shimDir, _ := shim.DefaultShimDir()
	if shimDir != "" {
		_ = shim.Remove(shimDir, store.Profile{Tool: tool, Name: args[1]})
	}

	if err := mgr.RemoveProfile(tool, args[1], purge); err != nil {
		return err
	}

	shimName := shim.Name(tool, args[1])
	fmt.Printf("%s Removed profile %s\n", Green("✓"), Bold(string(tool)+"/"+args[1]))
	fmt.Printf("   Shim %s has been uninstalled.\n", Cyan(shimName))
	if purge {
		fmt.Printf("   Profile directory purged from disk.\n")
	}

	return nil
}

// --- uninstall ---

func cmdUninstall(rootDir string, args []string) error {
	purge, args := extractBool(args, "--purge")

	if hasHelp(args) {
		fmt.Printf("Usage: profilex uninstall [--purge]\n\n")
		fmt.Printf("  --purge  Remove profile state directory (~/.profilex)\n")
		return nil
	}
	if len(args) > 0 {
		return fmt.Errorf("unknown argument(s): %s", strings.Join(args, " "))
	}

	var summary []string

	shimDir, err := shim.DefaultShimDir()
	if err != nil {
		return err
	}
	removed, err := shim.RemoveAll(shimDir)
	if err != nil {
		return err
	}
	summary = append(summary, fmt.Sprintf("Removed %d profilex shim(s) from %s", len(removed), shimDir))

	if purge {
		stateRoot, err := resolveRootDir(rootDir)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(stateRoot); err != nil {
			return fmt.Errorf("remove state dir %s: %w", stateRoot, err)
		}
		summary = append(summary, fmt.Sprintf("Removed state directory %s", stateRoot))
	}

	binCandidates := profilexBinaryCandidates()
	binRemoved := []string{}
	for _, candidate := range binCandidates {
		removed, err := removeOwnedBinary(candidate)
		if err != nil {
			return fmt.Errorf("remove binary %s: %w", candidate, err)
		}
		if removed {
			binRemoved = append(binRemoved, candidate)
		}
	}
	if len(binRemoved) == 0 {
		summary = append(summary, "No installer-managed profilex binary found to remove")
	} else {
		for _, path := range binRemoved {
			summary = append(summary, "Removed binary "+path)
		}
	}

	fmt.Printf("%s Uninstall complete\n", Green("✓"))
	for _, line := range summary {
		fmt.Printf("   - %s\n", line)
	}
	if len(binRemoved) == 0 {
		fmt.Printf("   - %s\n", "If profilex is still on PATH, remove it manually from your install directory.")
	}
	return nil
}

// --- list ---

func cmdList(rootDir string, args []string) error {
	toolFlag, args := extractFlag(args, "--tool")
	jsonOut, _ := extractBool(args, "--json")

	if hasHelp(args) {
		fmt.Printf("Usage: profilex list [--tool <tool>] [--json]\n")
		return nil
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	st, err := mgr.Load()
	if err != nil {
		return err
	}

	var filter *store.Tool
	if toolFlag != "" {
		t, e := parseTool(toolFlag)
		if e != nil {
			return e
		}
		filter = &t
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rows, err := mgr.StatusRows(ctx, filter)
	if err != nil {
		return err
	}

	if jsonOut {
		payload := map[string]any{"defaults": st.Defaults, "profiles": rows}
		b, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	if len(rows) == 0 {
		fmt.Printf("No profiles found.\n\n")
		fmt.Printf("💡 Get started: %s\n", Bold("profilex add claude <profile-name>"))
		return nil
	}

	fmt.Printf("%s\n\n", Bold("📋 Profiles"))

	// Group by tool
	var currentTool store.Tool
	var hints []string
	for _, r := range rows {
		if r.Profile.Tool != currentTool {
			if currentTool != "" {
				fmt.Println()
			}
			currentTool = r.Profile.Tool
			fmt.Printf("  %s\n", Bold(string(currentTool)))
		}

		isDefault := st.Defaults[r.Profile.Tool] == r.Profile.Name
		var icon, status, suffix string

		if r.Error != "" {
			icon = Yellow("⚠")
			status = Dim("error")
		} else if r.Status.LoggedIn {
			icon = Green("●")
			status = Green("logged in")
		} else {
			icon = Dim("○")
			status = Dim("not authenticated")
		}

		if isDefault {
			suffix = " " + Cyan("(default)")
		}

		shimName := shim.Name(r.Profile.Tool, r.Profile.Name)
		fmt.Printf("    %s %-20s %s%s\n", icon, r.Profile.Name, status, suffix)
		hints = append(hints, shimName)
	}

	fmt.Println()
	if len(hints) > 0 {
		fmt.Printf("💡 Run %s to launch with that profile.\n", Bold(hints[0]))
	}

	return nil
}

// --- use ---

func cmdUse(rootDir string, args []string) error {
	if hasHelp(args) || len(args) < 2 {
		fmt.Printf("Usage: profilex use <tool> <profile>\n")
		return nil
	}

	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	if err := mgr.SetDefault(tool, args[1]); err != nil {
		return err
	}

	fmt.Printf("%s Default for %s set to %s\n", Green("✓"), Bold(string(tool)), Bold(args[1]))
	return nil
}

// --- rename ---

func cmdRename(rootDir string, args []string) error {
	if hasHelp(args) || len(args) < 3 {
		fmt.Printf("Usage: profilex rename <tool> <old-name> <new-name>\n")
		return nil
	}

	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	oldName, newName := args[1], args[2]

	// Remove old shim, install new one
	shimDir, _ := shim.DefaultShimDir()
	if shimDir != "" {
		_ = shim.Remove(shimDir, store.Profile{Tool: tool, Name: oldName})
	}

	if err := mgr.RenameProfile(tool, oldName, newName); err != nil {
		return err
	}

	// Install new shim
	st, _ := mgr.Load()
	if st != nil {
		if _, p := store.FindProfile(st, tool, newName); p != nil {
			if path, err := installShimForProfile(*p); err == nil {
				fmt.Printf("   🔗 Shim updated: %s\n", Cyan(path))
			}
		}
	}

	fmt.Printf("%s Renamed %s to %s\n", Green("✓"),
		Bold(string(tool)+"/"+oldName),
		Bold(string(tool)+"/"+newName))

	return nil
}

// --- run ---

func cmdRun(rootDir string, args []string) error {
	if hasHelp(args) || len(args) < 1 {
		fmt.Printf("Usage: profilex run <tool> [profile] -- [tool args...]\n")
		return nil
	}

	// Split on "--"
	pre, toolArgs := splitDash(args)

	if len(pre) < 1 || len(pre) > 2 {
		return fmt.Errorf("usage: profilex run <tool> [profile] -- [tool args...]")
	}

	tool, err := parseTool(pre[0])
	if err != nil {
		return err
	}

	profileOptional := ""
	if len(pre) == 2 {
		profileOptional = pre[1]
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	st, err := mgr.Load()
	if err != nil {
		return err
	}

	profile, err := mgr.ResolveProfile(st, tool, profileOptional)
	if err != nil {
		return err
	}

	return mgr.RunTool(context.Background(), profile, toolArgs)
}

// --- shim ---

func cmdShim(rootDir string, args []string) error {
	if len(args) == 0 || hasHelp(args) {
		fmt.Printf("Usage:\n")
		fmt.Printf("  profilex shim install [--dir <d>]\n")
		fmt.Printf("  profilex shim env <tool> <profile>\n")
		fmt.Printf("  profilex shim uninstall [--all] [<tool> <profile>]\n")
		return nil
	}

	sub := args[0]
	rest := args[1:]

	switch sub {
	case "install":
		return cmdShimInstall(rootDir, rest)
	case "env":
		return cmdShimEnv(rootDir, rest)
	case "uninstall":
		return cmdShimUninstall(rootDir, rest)
	default:
		return fmt.Errorf("unknown shim subcommand: %s", sub)
	}
}

func cmdShimEnv(rootDir string, args []string) error {
	if hasHelp(args) || len(args) != 2 {
		fmt.Printf("Usage: profilex shim env <tool> <profile>\n")
		return nil
	}

	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	st, err := mgr.Load()
	if err != nil {
		return err
	}

	profile, err := mgr.ResolveProfile(st, tool, args[1])
	if err != nil {
		return err
	}

	adapter, err := adapters.Get(tool)
	if err != nil {
		return err
	}

	// Output plain KEY=VALUE lines for shim scripts to import.
	fmt.Printf("%s=%s\n", adapter.EnvVar(), profile.Dir)
	fmt.Printf("PROFILEX_TOOL=%s\n", tool)
	fmt.Printf("PROFILEX_PROFILE=%s\n", profile.Name)
	fmt.Printf("PROFILEX_SHIM_NAME=%s\n", shim.Name(tool, profile.Name))
	return nil
}

func cmdShimInstall(rootDir string, args []string) error {
	dir, _ := extractFlag(args, "--dir")
	if dir == "" {
		d, err := shim.DefaultShimDir()
		if err != nil {
			return err
		}
		dir = d
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}

	st, err := mgr.Load()
	if err != nil {
		return err
	}

	bin := resolveProfileXBin()
	count := 0
	failures := 0
	for _, p := range st.Profiles {
		path, err := shim.Install(dir, p, bin)
		if err != nil {
			failures++
			fmt.Printf("   %s Failed to install shim for %s/%s: %v\n", Yellow("!"), p.Tool, p.Name, err)
			continue
		}
		fmt.Printf("   -> %s\n", Cyan(path))
		count++
	}

	fmt.Printf("\n%s Installed %d shim(s) in %s\n", Green("ok"), count, Dim(dir))
	if failures > 0 {
		return fmt.Errorf("failed to install %d shim(s)", failures)
	}
	return nil
}

func cmdShimUninstall(rootDir string, args []string) error {
	all, args := extractBool(args, "--all")
	dir, args := extractFlag(args, "--dir")

	if dir == "" {
		d, err := shim.DefaultShimDir()
		if err != nil {
			return err
		}
		dir = d
	}

	if all {
		removed, err := shim.RemoveAll(dir)
		if err != nil {
			return err
		}
		for _, p := range removed {
			fmt.Printf("   removed: %s\n", p)
		}
		fmt.Printf("%s Removed %d shim(s)\n", Green("✓"), len(removed))
		return nil
	}

	if len(args) != 2 {
		return fmt.Errorf("provide <tool> <profile> or use --all")
	}

	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}

	p := store.Profile{Tool: tool, Name: args[1]}
	if err := shim.Remove(dir, p); err != nil {
		return err
	}

	fmt.Printf("%s Removed shim %s\n", Green("✓"), Cyan(shim.Name(tool, args[1])))
	return nil
}

// --- helpers ---

func resolvedVersion() string {
	v := strings.TrimSpace(version)
	if v == "" {
		return "dev"
	}
	return v
}

func newManager(rootDir string) (*app.Manager, error) {
	if strings.TrimSpace(rootDir) == "" {
		return app.NewDefaultManager()
	}
	abs := rootDir
	if !filepath.IsAbs(rootDir) {
		cwd, _ := os.Getwd()
		abs = filepath.Join(cwd, rootDir)
	}
	return app.NewManager(abs)
}

func parseTool(raw string) (store.Tool, error) {
	tool, ok := store.IsSupportedTool(raw)
	if !ok {
		return "", fmt.Errorf("unsupported tool %q (supported: %s)", raw, strings.Join(toolNames(), ", "))
	}
	return tool, nil
}

func toolNames() []string {
	names := make([]string, len(store.SupportedTools))
	for i, t := range store.SupportedTools {
		names[i] = string(t)
	}
	return names
}

func installShimForProfile(profile store.Profile) (string, error) {
	dir, err := shim.DefaultShimDir()
	if err != nil {
		return "", err
	}
	return shim.Install(dir, profile, resolveProfileXBin())
}

func resolveProfileXBin() string {
	if exe, err := os.Executable(); err == nil && strings.TrimSpace(exe) != "" {
		return exe
	}
	if bin, err := exec.LookPath("profilex"); err == nil && strings.TrimSpace(bin) != "" {
		return bin
	}
	return "profilex"
}

func resolveRootDir(rootDir string) (string, error) {
	if strings.TrimSpace(rootDir) == "" {
		return store.DefaultRoot()
	}
	abs := rootDir
	if !filepath.IsAbs(rootDir) {
		cwd, _ := os.Getwd()
		abs = filepath.Join(cwd, rootDir)
	}
	return abs, nil
}

func profilexBinaryCandidates() []string {
	var candidates []string

	if exe, err := os.Executable(); err == nil && isProfileXBinaryPath(exe) {
		candidates = append(candidates, exe)
	}
	if lp, err := exec.LookPath("profilex"); err == nil && isProfileXBinaryPath(lp) {
		candidates = append(candidates, lp)
	}
	if lp, err := exec.LookPath("profilex.exe"); err == nil && isProfileXBinaryPath(lp) {
		candidates = append(candidates, lp)
	}

	home, err := os.UserHomeDir()
	if err == nil && strings.TrimSpace(home) != "" {
		installDir := os.Getenv("PROFILEX_INSTALL_DIR")
		if strings.TrimSpace(installDir) == "" {
			installDir = filepath.Join(home, ".local", "bin")
		}
		candidates = append(candidates,
			filepath.Join(installDir, "profilex"),
			filepath.Join(installDir, "profilex.exe"),
		)
	}

	return uniquePaths(candidates)
}

func isProfileXBinaryPath(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	return base == "profilex" || base == "profilex.exe"
}

func removeOwnedBinary(path string) (bool, error) {
	if !isInstallerManagedBinary(path) {
		return false, nil
	}
	removed, err := removeFileWithWindowsFallback(path)
	if err != nil {
		return false, err
	}
	if removed {
		_ = os.Remove(ownershipMarkerPath(path))
	}
	return removed, nil
}

func isInstallerManagedBinary(binaryPath string) bool {
	markerPath := ownershipMarkerPath(binaryPath)
	content, err := os.ReadFile(markerPath)
	if err != nil {
		return false
	}

	lines := strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != ownershipMarkerMagic {
		return false
	}
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "path=") {
			continue
		}
		markerPath := strings.TrimSpace(strings.TrimPrefix(line, "path="))
		if markerPath == "" {
			return false
		}
		return equalFilePath(binaryPath, markerPath)
	}

	// Backward-compatible marker support: if a marker exists with a valid magic
	// header but no explicit path field, treat it as managed.
	return true
}

func ownershipMarkerPath(binaryPath string) string {
	base := filepath.Base(binaryPath)
	return filepath.Join(filepath.Dir(binaryPath), "."+base+".profilex-owner")
}

func equalFilePath(a, b string) bool {
	na := normalizedPathForCompare(a)
	nb := normalizedPathForCompare(b)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(na, nb)
	}
	return na == nb
}

func normalizedPathForCompare(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}

func uniquePaths(paths []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if strings.TrimSpace(p) == "" {
			continue
		}
		key := normalizedPathForCompare(p)
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, p)
	}
	return out
}

func removeFileWithWindowsFallback(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		return false, nil
	}

	if err := os.Remove(path); err == nil {
		return true, nil
	} else if runtime.GOOS != "windows" {
		return false, err
	}

	escaped := strings.ReplaceAll(path, `"`, `""`)
	cmd := exec.Command("cmd", "/C", fmt.Sprintf(`ping 127.0.0.1 -n 2 >nul & del /f /q "%s"`, escaped))
	if err := cmd.Start(); err != nil {
		return false, err
	}
	_ = cmd.Process.Release()
	return true, nil
}

// extractFlag extracts "--flag value" from args, returning the value and remaining args.
func extractFlag(args []string, flag string) (string, []string) {
	for i := 0; i < len(args); i++ {
		if args[i] == flag && i+1 < len(args) {
			val := args[i+1]
			remaining := make([]string, 0, len(args)-2)
			remaining = append(remaining, args[:i]...)
			remaining = append(remaining, args[i+2:]...)
			return val, remaining
		}
	}
	return "", args
}

// extractBool extracts a boolean "--flag" from args.
func extractBool(args []string, flag string) (bool, []string) {
	for i := 0; i < len(args); i++ {
		if args[i] == flag {
			remaining := make([]string, 0, len(args)-1)
			remaining = append(remaining, args[:i]...)
			remaining = append(remaining, args[i+1:]...)
			return true, remaining
		}
	}
	return false, args
}

// splitDash splits args on "--", returning pre and post.
func splitDash(args []string) ([]string, []string) {
	for i, a := range args {
		if a == "--" {
			return args[:i], args[i+1:]
		}
	}
	return args, nil
}

func hasHelp(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" {
			return true
		}
	}
	return false
}
