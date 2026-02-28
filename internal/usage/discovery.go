package usage

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/derekurban/profilex-cli/internal/store"
)

type profileResolver struct {
	state            *store.State
	syntheticByRoot  map[string]string
	syntheticCounter map[Tool]int
}

type profileMatch struct {
	ProfileID         string
	ProfileName       string
	IsProfilexManaged bool
}

type usageFile struct {
	ParsePath      string
	CanonicalPath  string
	DiscoveredPath string
	AliasPaths     []string
	AliasRoots     []string
}

func newProfileResolver(st *store.State) *profileResolver {
	return &profileResolver{
		state:            st,
		syntheticByRoot:  map[string]string{},
		syntheticCounter: map[Tool]int{ToolClaude: 0, ToolCodex: 0, ToolUnknown: 0},
	}
}

func (r *profileResolver) resolve(tool Tool, root string) profileMatch {
	normRoot := normalizePath(strings.ToLower(root))

	if r.state != nil {
		for _, p := range r.state.Profiles {
			if Tool(p.Tool) != tool {
				continue
			}
			d := normalizePath(strings.ToLower(p.Dir))
			if d == "" {
				continue
			}
			if strings.Contains(normRoot, d) || strings.Contains(d, normRoot) {
				return profileMatch{ProfileID: string(p.Tool) + "/" + p.Name, ProfileName: p.Name, IsProfilexManaged: true}
			}
			marker := "/profiles/" + strings.ToLower(string(p.Tool)) + "/" + strings.ToLower(p.Name)
			if strings.Contains(normRoot, marker) {
				return profileMatch{ProfileID: string(p.Tool) + "/" + p.Name, ProfileName: p.Name, IsProfilexManaged: true}
			}
		}
	}

	key := string(tool) + ":" + normRoot
	if existing, ok := r.syntheticByRoot[key]; ok {
		return profileMatch{ProfileID: string(tool) + "/" + existing, ProfileName: existing}
	}
	r.syntheticCounter[tool]++
	name := "default-" + strconvItoa(r.syntheticCounter[tool])
	r.syntheticByRoot[key] = name
	return profileMatch{ProfileID: string(tool) + "/" + name, ProfileName: name}
}

func inferToolFromPath(path string) Tool {
	p := strings.ToLower(normalizePath(path))
	if strings.Contains(p, "/projects/") || strings.Contains(p, "claude") {
		return ToolClaude
	}
	if strings.Contains(p, "/sessions/") || strings.Contains(p, "codex") {
		return ToolCodex
	}
	return ToolUnknown
}

func extractRootFromFile(filePath string, tool Tool) string {
	p := normalizePath(filePath)
	switch tool {
	case ToolClaude:
		if idx := strings.Index(strings.ToLower(p), "/projects/"); idx >= 0 {
			return p[:idx]
		}
	case ToolCodex:
		if idx := strings.Index(strings.ToLower(p), "/sessions/"); idx >= 0 {
			return p[:idx]
		}
	}
	if idx := strings.LastIndex(p, "/"); idx > 0 {
		return p[:idx]
	}
	return p
}

func discoverRoots(rootDir string, st *store.State) []string {
	home, _ := os.UserHomeDir()
	set := map[string]bool{}
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		set[normalizePath(expandHome(p))] = true
	}

	add(filepath.Join(home, ".config", "claude", "projects"))
	add(filepath.Join(home, ".claude", "projects"))
	for _, p := range splitPathList(os.Getenv("CLAUDE_CONFIG_DIR")) {
		add(ensureLeaf(p, "projects"))
	}

	add(filepath.Join(home, ".codex", "sessions"))
	for _, p := range splitPathList(os.Getenv("CODEX_HOME")) {
		add(ensureLeaf(p, "sessions"))
	}

	for _, p := range splitPathList(os.Getenv("PROFILEX_USAGE_EXTRA_ROOTS")) {
		add(p)
	}

	if st != nil {
		for _, p := range st.Profiles {
			leaf := "projects"
			if p.Tool == store.ToolCodex {
				leaf = "sessions"
			}
			add(ensureLeaf(p.Dir, leaf))
		}
	}

	out := make([]string, 0, len(set))
	for p := range set {
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	_ = rootDir
	return out
}

func collectJSONLFiles(roots []string, deep bool, maxFiles int) ([]usageFile, error) {
	if maxFiles <= 0 {
		maxFiles = 5000
	}
	type aggregate struct {
		canonical string
		aliases   map[string]bool
		roots     map[string]bool
		firstPath string
	}
	byCanonical := map[string]*aggregate{}

	addPath := func(path, root string) {
		normPath := normalizePath(path)
		normRoot := normalizePath(root)
		canon := canonicalizePath(path)
		key := pathKey(canon)

		existing, ok := byCanonical[key]
		if !ok {
			if len(byCanonical) >= maxFiles {
				return
			}
			existing = &aggregate{
				canonical: canon,
				aliases:   map[string]bool{},
				roots:     map[string]bool{},
				firstPath: normPath,
			}
			byCanonical[key] = existing
		}
		existing.aliases[normPath] = true
		if normRoot != "" {
			existing.roots[normRoot] = true
		}
	}

	for _, root := range roots {
		root = normalizePath(root)
		filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if len(byCanonical) >= maxFiles {
				return fs.SkipAll
			}
			if d.IsDir() {
				name := strings.ToLower(d.Name())
				skip := map[string]bool{".git": true, "node_modules": true, "dist": true, "build": true, "tmp": true, "temp": true}
				if skip[name] {
					return fs.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(strings.ToLower(d.Name()), ".jsonl") {
				addPath(path, root)
			}
			return nil
		})
	}

	if deep {
		home, _ := os.UserHomeDir()
		home = normalizePath(home)
		filepath.WalkDir(home, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if len(byCanonical) >= maxFiles {
				return fs.SkipAll
			}
			if d.IsDir() {
				name := strings.ToLower(d.Name())
				skip := map[string]bool{".git": true, "node_modules": true, "dist": true, "build": true, "tmp": true, "temp": true, "library": true, "appdata": true}
				if skip[name] {
					return fs.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(strings.ToLower(d.Name()), ".jsonl") {
				p := strings.ToLower(normalizePath(path))
				if strings.Contains(p, "/projects/") || strings.Contains(p, "/sessions/") || strings.Contains(p, "/claude/") || strings.Contains(p, "/codex/") {
					addPath(path, "")
				}
			}
			return nil
		})
	}

	out := make([]usageFile, 0, len(byCanonical))
	for _, agg := range byCanonical {
		aliasPaths := make([]string, 0, len(agg.aliases))
		for p := range agg.aliases {
			aliasPaths = append(aliasPaths, p)
		}
		sort.Strings(aliasPaths)

		aliasRoots := make([]string, 0, len(agg.roots))
		for r := range agg.roots {
			aliasRoots = append(aliasRoots, r)
		}
		sort.Strings(aliasRoots)

		parsePath := agg.canonical
		if strings.TrimSpace(parsePath) == "" {
			parsePath = agg.firstPath
		}

		out = append(out, usageFile{
			ParsePath:      parsePath,
			CanonicalPath:  agg.canonical,
			DiscoveredPath: agg.firstPath,
			AliasPaths:     aliasPaths,
			AliasRoots:     aliasRoots,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].CanonicalPath
		if left == "" {
			left = out[i].DiscoveredPath
		}
		right := out[j].CanonicalPath
		if right == "" {
			right = out[j].DiscoveredPath
		}
		return pathKey(left) < pathKey(right)
	})
	return out, nil
}

func normalizePath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	if strings.HasSuffix(p, "/") && len(p) > 1 {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}

func expandHome(p string) string {
	if p == "" {
		return p
	}
	if p == "~" {
		h, _ := os.UserHomeDir()
		return h
	}
	if strings.HasPrefix(p, "~/") {
		h, _ := os.UserHomeDir()
		return filepath.Join(h, p[2:])
	}
	return p
}

func splitPathList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func ensureLeaf(base, leaf string) string {
	base = normalizePath(expandHome(base))
	if strings.HasSuffix(strings.ToLower(base), "/"+strings.ToLower(leaf)) {
		return base
	}
	return normalizePath(filepath.Join(base, leaf))
}

func strconvItoa(i int) string {
	return fmt.Sprintf("%d", i)
}

func canonicalizePath(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return normalizePath(resolved)
	}
	return normalizePath(abs)
}

func pathKey(path string) string {
	p := normalizePath(path)
	if runtime.GOOS == "windows" {
		return strings.ToLower(p)
	}
	return p
}
