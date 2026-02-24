package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/derekurban/profilex-cli/internal/store"
)

func TestSnapshotAndApplySettingsPresetCodexKeepsAuthIsolated(t *testing.T) {
	m := newTestManager(t)
	p1, _, err := m.EnsureProfile(store.ToolCodex, "personal1")
	if err != nil {
		t.Fatal(err)
	}
	p2, _, err := m.EnsureProfile(store.ToolCodex, "personal2")
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(p1.Dir, "config.toml"), []byte("model = \"gpt-5.3-codex\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p1.Dir, "auth.json"), []byte("AUTH-P1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p2.Dir, "config.toml"), []byte("model = \"old\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p2.Dir, "auth.json"), []byte("AUTH-P2"), 0o644); err != nil {
		t.Fatal(err)
	}

	updated, err := m.SnapshotSettings(store.ToolCodex, "personal1", "full-access")
	if err != nil {
		t.Fatal(err)
	}
	if updated != 0 {
		t.Fatalf("expected no synced profiles to be updated, got %d", updated)
	}
	if err := m.ApplySettingsPreset(store.ToolCodex, "full-access", "personal2"); err != nil {
		t.Fatal(err)
	}

	gotCfg, err := os.ReadFile(filepath.Join(p2.Dir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotCfg) != "model = \"gpt-5.3-codex\"\n" {
		t.Fatalf("unexpected copied config: %q", string(gotCfg))
	}

	gotAuth, err := os.ReadFile(filepath.Join(p2.Dir, "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotAuth) != "AUTH-P2" {
		t.Fatalf("auth should remain profile-local, got %q", string(gotAuth))
	}
}

func TestSnapshotDoesNotAutoApplyToSyncedProfiles(t *testing.T) {
	m := newTestManager(t)
	p1, _, err := m.EnsureProfile(store.ToolCodex, "personal1")
	if err != nil {
		t.Fatal(err)
	}
	p2, _, err := m.EnsureProfile(store.ToolCodex, "personal2")
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(p1.Dir, "config.toml"), []byte("model = \"a\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SnapshotSettings(store.ToolCodex, "personal1", "low-thinking"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, "personal2", "low-thinking", true); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(p1.Dir, "config.toml"), []byte("model = \"b\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	updated, err := m.SnapshotSettings(store.ToolCodex, "personal1", "low-thinking")
	if err != nil {
		t.Fatal(err)
	}
	if updated != 0 {
		t.Fatalf("expected zero auto-applied profiles, got %d", updated)
	}

	gotCfg, err := os.ReadFile(filepath.Join(p2.Dir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotCfg) != "model = \"a\"\n" {
		t.Fatalf("expected target config to remain unchanged after new snapshot, got %q", string(gotCfg))
	}
}

func TestApplySyncedSettingsForProfile(t *testing.T) {
	m := newTestManager(t)
	p1, _, err := m.EnsureProfile(store.ToolCodex, "source")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.EnsureProfile(store.ToolCodex, "target"); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(p1.Dir, "config.toml"), []byte("model = \"sync\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SnapshotSettings(store.ToolCodex, "source", "shared-preset"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, "target", "shared-preset", true); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	target, err := m.GetProfile(st, store.ToolCodex, "target")
	if err != nil {
		t.Fatal(err)
	}

	applied, preset, err := m.ApplySyncedSettings(target)
	if err != nil {
		t.Fatal(err)
	}
	if applied || preset != "" {
		t.Fatalf("expected no auto-apply, got applied=%v preset=%q", applied, preset)
	}
}

func TestSnapshotFromNativeDefaultAlias(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "target")
	if err != nil {
		t.Fatal(err)
	}

	nativeDir := filepath.Join(t.TempDir(), "native-codex")
	if err := os.MkdirAll(nativeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nativeDir, "config.toml"), []byte("model = \"native\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROFILEX_NATIVE_CODEX_HOME", nativeDir)

	if _, err := m.SnapshotSettings(store.ToolCodex, "default", "native-snap"); err != nil {
		t.Fatal(err)
	}
	if err := m.ApplySettingsPreset(store.ToolCodex, "native-snap", p.Name); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(p.Dir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "model = \"native\"\n" {
		t.Fatalf("expected native config to be copied, got %q", string(got))
	}
}

func TestApplyToNativeDefaultAlias(t *testing.T) {
	m := newTestManager(t)
	profile, _, err := m.EnsureProfile(store.ToolCodex, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profile.Dir, "config.toml"), []byte("model = \"from-profile\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	nativeDir := filepath.Join(t.TempDir(), "native-codex")
	if err := os.MkdirAll(nativeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROFILEX_NATIVE_CODEX_HOME", nativeDir)

	if _, err := m.SnapshotSettings(store.ToolCodex, profile.Name, "preset-a"); err != nil {
		t.Fatal(err)
	}
	if err := m.ApplySettingsPreset(store.ToolCodex, "preset-a", "native"); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(nativeDir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "model = \"from-profile\"\n" {
		t.Fatalf("expected native config update, got %q", string(got))
	}
}

func TestSyncMappingToNativeDefaultAlias(t *testing.T) {
	m := newTestManager(t)
	p, _, err := m.EnsureProfile(store.ToolCodex, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Dir, "config.toml"), []byte("model = \"v1\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	nativeDir := filepath.Join(t.TempDir(), "native-codex")
	if err := os.MkdirAll(nativeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROFILEX_NATIVE_CODEX_HOME", nativeDir)

	if _, err := m.SnapshotSettings(store.ToolCodex, p.Name, "preset-native-sync"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, "default", "preset-native-sync", true); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(p.Dir, "config.toml"), []byte("model = \"v2\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	updated, err := m.SnapshotSettings(store.ToolCodex, p.Name, "preset-native-sync")
	if err != nil {
		t.Fatal(err)
	}
	if updated != 0 {
		t.Fatalf("expected no auto-applied targets, got %d", updated)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, sync := store.FindSettingsSync(st, store.ToolCodex, "default"); sync == nil {
		t.Fatalf("expected sync mapping to canonical default alias")
	}

	got, err := os.ReadFile(filepath.Join(nativeDir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "model = \"v1\"\n" {
		t.Fatalf("expected native config to remain unchanged after new snapshot, got %q", string(got))
	}
}

func TestRenameSettingsPresetUpdatesSyncBindings(t *testing.T) {
	m := newTestManager(t)
	profile, _, err := m.EnsureProfile(store.ToolCodex, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profile.Dir, "config.toml"), []byte("model = \"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SnapshotSettings(store.ToolCodex, profile.Name, "template-a"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, profile.Name, "template-a", true); err != nil {
		t.Fatal(err)
	}

	if err := m.RenameSettingsPreset(store.ToolCodex, "template-a", "template-b"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, p := store.FindSettingsPreset(st, store.ToolCodex, "template-b"); p == nil {
		t.Fatalf("expected renamed preset to exist")
	}
	if _, sync := store.FindSettingsSync(st, store.ToolCodex, profile.Name); sync == nil || sync.Preset != "template-b" {
		t.Fatalf("expected sync binding to track renamed preset")
	}
}

func TestDeleteSettingsPresetRemovesSyncBindings(t *testing.T) {
	m := newTestManager(t)
	profile, _, err := m.EnsureProfile(store.ToolCodex, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profile.Dir, "config.toml"), []byte("model = \"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SnapshotSettings(store.ToolCodex, profile.Name, "template-a"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetSettingsSync(store.ToolCodex, profile.Name, "template-a", true); err != nil {
		t.Fatal(err)
	}

	if err := m.DeleteSettingsPreset(store.ToolCodex, "template-a"); err != nil {
		t.Fatal(err)
	}

	st, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, p := store.FindSettingsPreset(st, store.ToolCodex, "template-a"); p != nil {
		t.Fatalf("expected preset to be deleted")
	}
	if _, sync := store.FindSettingsSync(st, store.ToolCodex, profile.Name); sync != nil {
		t.Fatalf("expected sync binding to be removed with preset")
	}
}
