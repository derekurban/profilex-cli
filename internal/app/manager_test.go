package app

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/derekurban/profilex-cli/internal/store"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEnsureProfileCreatesAndSetsDefault(t *testing.T) {
	m := newTestManager(t)
	p, created, err := m.EnsureProfile(store.ToolClaude, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatalf("expected profile to be created")
	}
	if p.Tool != store.ToolClaude || p.Name != "personal" {
		t.Fatalf("unexpected profile: %+v", p)
	}
	if _, err := os.Stat(p.Dir); err != nil {
		t.Fatalf("expected profile dir to exist: %v", err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Defaults[store.ToolClaude]; got != "personal" {
		t.Fatalf("unexpected default: %q", got)
	}

	_, created2, err := m.EnsureProfile(store.ToolClaude, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatalf("expected existing profile not to be created again")
	}
}

func TestSetDefaultAndResolveProfile(t *testing.T) {
	m := newTestManager(t)
	if _, _, err := m.EnsureProfile(store.ToolCodex, "work"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.EnsureProfile(store.ToolCodex, "client"); err != nil {
		t.Fatal(err)
	}

	if err := m.SetDefault(store.ToolCodex, "client"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := m.ResolveProfile(st, store.ToolCodex, "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "client" {
		t.Fatalf("expected default resolve to client, got %s", resolved.Name)
	}

	explicit, err := m.ResolveProfile(st, store.ToolCodex, "work")
	if err != nil {
		t.Fatal(err)
	}
	if explicit.Name != "work" {
		t.Fatalf("expected explicit profile work, got %s", explicit.Name)
	}
}

func TestRenameProfile(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolClaude, "old")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetDefault(store.ToolClaude, "old"); err != nil {
		t.Fatal(err)
	}
	if err := m.RenameProfile(store.ToolClaude, "old", "new"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.Defaults[store.ToolClaude] != "new" {
		t.Fatalf("default should have been moved to new name")
	}
	_, found := store.FindProfile(st, store.ToolClaude, "new")
	if found == nil {
		t.Fatalf("expected renamed profile to exist")
	}
	if _, err := os.Stat(filepath.Join(m.Root(), "profiles", "claude", "new")); err != nil {
		t.Fatalf("renamed profile dir missing: %v", err)
	}
	if _, err := os.Stat(p.Dir); !os.IsNotExist(err) {
		t.Fatalf("old profile dir should be gone")
	}
}

func TestRemoveProfileAndReassignDefault(t *testing.T) {
	m := newTestManager(t)
	if _, _, err := m.EnsureProfile(store.ToolCodex, "a"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.EnsureProfile(store.ToolCodex, "b"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetDefault(store.ToolCodex, "a"); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveProfile(store.ToolCodex, "a", false); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, p := store.FindProfile(st, store.ToolCodex, "a"); p != nil {
		t.Fatalf("profile a should be removed")
	}
	if st.Defaults[store.ToolCodex] != "b" {
		t.Fatalf("default should move to remaining profile b, got %q", st.Defaults[store.ToolCodex])
	}
}

func TestRemoveProfilePurgeDeletesDir(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolClaude, "trashme")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveProfile(store.ToolClaude, "trashme", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p.Dir); !os.IsNotExist(err) {
		t.Fatalf("expected profile dir to be deleted with purge")
	}
}

func TestRemoveProfileRejectsUnsafePersistedDir(t *testing.T) {
	m := newTestManager(t)
	if _, _, err := m.EnsureProfile(store.ToolClaude, "work"); err != nil {
		t.Fatal(err)
	}

	unsafeDir := t.TempDir()
	sentinel := filepath.Join(unsafeDir, "keep.txt")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	st.Profiles[0].Dir = unsafeDir
	if err := m.Save(st); err != nil {
		t.Fatal(err)
	}

	if err := m.RemoveProfile(store.ToolClaude, "work", true); err == nil {
		t.Fatalf("expected remove with purge to reject unsafe profile directory")
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("unsafe directory should not be deleted: %v", err)
	}

	after, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, p := store.FindProfile(after, store.ToolClaude, "work"); p == nil {
		t.Fatalf("profile should remain after rejected unsafe remove")
	}
}

func TestGetProfileRejectsUnsafePersistedDir(t *testing.T) {
	m := newTestManager(t)
	if _, _, err := m.EnsureProfile(store.ToolCodex, "main"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	st.Profiles[0].Dir = t.TempDir()
	if err := m.Save(st); err != nil {
		t.Fatal(err)
	}

	loaded, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.GetProfile(loaded, store.ToolCodex, "main"); err == nil {
		t.Fatalf("expected unsafe persisted path to be rejected")
	}
}

func TestStatusRowsSurfaceUnsafeDirErrors(t *testing.T) {
	m := newTestManager(t)
	if _, _, err := m.EnsureProfile(store.ToolClaude, "x"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	st.Profiles[0].Dir = t.TempDir()
	if err := m.Save(st); err != nil {
		t.Fatal(err)
	}

	rows, err := m.StatusRows(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if rows[0].Error == "" {
		t.Fatalf("expected unsafe directory error in status row")
	}
}

func TestEnableSharedSessionsCreatesLink(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "main")
	if err != nil {
		t.Fatal(err)
	}

	sharedDir, err := m.EnableSharedSessions(p)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(sharedDir); err != nil {
		t.Fatalf("shared directory should exist: %v", err)
	}

	mount := filepath.Join(p.Dir, "sessions")
	resolved, err := filepath.EvalSymlinks(mount)
	if err != nil {
		if runtime.GOOS == "windows" {
			// Junctions can behave differently on some CI setups; ensure mount exists at least.
			if _, statErr := os.Stat(mount); statErr != nil {
				t.Fatalf("mount should exist: %v", statErr)
			}
			return
		}
		t.Fatalf("expected mounted shared sessions dir: %v", err)
	}

	expected, err := filepath.EvalSymlinks(sharedDir)
	if err != nil {
		expected, _ = filepath.Abs(sharedDir)
	}
	actual, _ := filepath.Abs(resolved)
	if !samePath(actual, expected) {
		t.Fatalf("mount should resolve to shared dir: got %q want %q", actual, expected)
	}
}

func TestEnableSharedSessionsRejectsNonEmptyLocalDir(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolClaude, "main")
	if err != nil {
		t.Fatal(err)
	}

	projectsDir := filepath.Join(p.Dir, "projects")
	if err := os.MkdirAll(projectsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projectsDir, "keep.jsonl"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := m.EnableSharedSessions(p); err == nil {
		t.Fatalf("expected non-empty local session dir to be rejected")
	}
}

func TestDisableSharedSessionsRemovesMount(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.EnableSharedSessions(p); err != nil {
		t.Fatal(err)
	}
	enabled, err := m.SharedSessionsEnabled(p)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Fatalf("expected shared sessions to be enabled")
	}

	if err := m.DisableSharedSessions(p); err != nil {
		t.Fatal(err)
	}
	enabled, err = m.SharedSessionsEnabled(p)
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatalf("expected shared sessions to be disabled")
	}

	mount := filepath.Join(p.Dir, "sessions")
	info, err := os.Lstat(mount)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("expected local sessions dir after disable")
	}
}

func TestEnableSharedSkillsUsesSinglePathAcrossTools(t *testing.T) {
	m := newTestManager(t)
	codexProfile, _, err := m.EnsureProfile(store.ToolCodex, "codex-main")
	if err != nil {
		t.Fatal(err)
	}
	claudeProfile, _, err := m.EnsureProfile(store.ToolClaude, "claude-main")
	if err != nil {
		t.Fatal(err)
	}

	codexShared, err := m.EnableSharedSkills(codexProfile)
	if err != nil {
		t.Fatal(err)
	}
	claudeShared, err := m.EnableSharedSkills(claudeProfile)
	if err != nil {
		t.Fatal(err)
	}

	if !samePath(codexShared, claudeShared) {
		t.Fatalf("expected single shared skills path, got %q and %q", codexShared, claudeShared)
	}
	if _, err := os.Stat(codexShared); err != nil {
		t.Fatalf("shared skills directory should exist: %v", err)
	}

	codexOn, err := m.SharedSkillsEnabled(codexProfile)
	if err != nil {
		t.Fatal(err)
	}
	claudeOn, err := m.SharedSkillsEnabled(claudeProfile)
	if err != nil {
		t.Fatal(err)
	}
	if !codexOn || !claudeOn {
		t.Fatalf("expected shared skills to be enabled for both profiles")
	}
}

func TestEnableSharedSkillsRejectsNonEmptyLocalDir(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "main")
	if err != nil {
		t.Fatal(err)
	}

	skillsDir := filepath.Join(p.Dir, "skills")
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillsDir, "SKILL.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := m.EnableSharedSkills(p); err == nil {
		t.Fatalf("expected non-empty local skills dir to be rejected")
	}
}

func TestDisableSharedSkillsRemovesMount(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolClaude, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.EnableSharedSkills(p); err != nil {
		t.Fatal(err)
	}

	enabled, err := m.SharedSkillsEnabled(p)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Fatalf("expected shared skills to be enabled")
	}

	if err := m.DisableSharedSkills(p); err != nil {
		t.Fatal(err)
	}
	enabled, err = m.SharedSkillsEnabled(p)
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatalf("expected shared skills to be disabled")
	}

	mount := filepath.Join(p.Dir, "skills")
	info, err := os.Lstat(mount)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("expected local skills dir after disable")
	}
}

func TestRenameProfileAlsoUpdatesSettingsSyncBinding(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "old")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Dir, "config.toml"), []byte("model = \"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SnapshotSettings(store.ToolCodex, "old", "preset1"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, "old", "preset1", true); err != nil {
		t.Fatal(err)
	}

	if err := m.RenameProfile(store.ToolCodex, "old", "new"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	_, binding := store.FindSettingsSync(st, store.ToolCodex, "new")
	if binding == nil || binding.Preset != "preset1" {
		t.Fatalf("expected sync binding to follow rename")
	}
}

func TestRemoveProfileAlsoRemovesSettingsSyncBinding(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Dir, "config.toml"), []byte("model = \"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SnapshotSettings(store.ToolCodex, "target", "preset1"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, "target", "preset1", true); err != nil {
		t.Fatal(err)
	}

	if err := m.RemoveProfile(store.ToolCodex, "target", false); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, binding := store.FindSettingsSync(st, store.ToolCodex, "target"); binding != nil {
		t.Fatalf("sync binding should be removed with profile")
	}
}
