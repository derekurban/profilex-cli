package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/derekurban/profilex-cli/internal/shim"
	"github.com/derekurban/profilex-cli/internal/store"
	"github.com/derekurban/profilex-cli/internal/usage"
)

type sidebarKind int

const (
	sidebarAdd sidebarKind = iota
	sidebarExport
	sidebarTemplates
	sidebarProfile
	sidebarHeading
)

type sidebarItem struct {
	Kind       sidebarKind
	Label      string
	Selectable bool
	Tool       store.Tool
	Profile    string
}

type tuiDataMsg struct {
	State         *store.State
	Presets       []store.SettingsPreset
	SyncByProfile map[string]string
	SessionShared map[string]bool
	SkillsShared  map[string]bool
}

type tuiOpMsg struct {
	Err     error
	Info    string
	Refresh bool
}

type exportResultMsg struct {
	Err    error
	Result exportResult
}

type exportTickMsg struct{}

type exportResult struct {
	Out      string
	Profiles int
	Events   int
	Roots    int
	Files    int
	Duration time.Duration
}

type modeKind int

const (
	modeNormal modeKind = iota
	modeTemplateApply
	modeTemplateRename
	modeTemplateDelete
	modeProfileRename
	modeProfileDelete
)

type model struct {
	rootDir string
	width   int

	state         *store.State
	presets       []store.SettingsPreset
	syncByProfile map[string]string
	sessionShared map[string]bool
	skillsShared  map[string]bool

	sidebar []sidebarItem
	cursor  int

	addToolIdx   int
	addNameInput textinput.Model
	addShare     bool
	addSync      bool
	addSkills    bool

	exportPathInput textinput.Model
	exportRunning   bool
	exportStarted   time.Time
	exportElapsed   time.Duration
	lastExport      *exportResult

	templateCursor int
	templateSource int
	templateName   textinput.Model

	mode       modeKind
	prompt     textinput.Model
	applyIndex int

	info string
	err  string
}

func cmdTUI(rootDir string, args []string) error {
	if hasHelp(args) {
		fmt.Printf("Usage: profilex tui\n")
		return nil
	}
	if len(args) > 0 {
		return fmt.Errorf("unknown argument(s): %s", strings.Join(args, " "))
	}
	p := tea.NewProgram(newModel(rootDir), tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func newModel(rootDir string) model {
	add := textinput.New()
	add.Placeholder = "profile-name"
	add.CharLimit = 64
	add.Width = 28
	add.Focus()

	out := textinput.New()
	out.SetValue("profilex-usage-" + time.Now().Format("20060102-150405") + ".json")
	out.Width = 64

	tpl := textinput.New()
	tpl.Placeholder = "template-name"
	tpl.CharLimit = 64
	tpl.Width = 24

	p := textinput.New()
	p.CharLimit = 64
	p.Width = 28

	return model{
		rootDir:         rootDir,
		width:           120,
		syncByProfile:   map[string]string{},
		sessionShared:   map[string]bool{},
		skillsShared:    map[string]bool{},
		addShare:        true,
		addSync:         true,
		addSkills:       true,
		addNameInput:    add,
		exportPathInput: out,
		templateName:    tpl,
		prompt:          p,
		templateSource:  0,
		templateCursor:  0,
		addToolIdx:      0,
		mode:            modeNormal,
	}
}

func (m model) Init() tea.Cmd { return refreshCmd(m.rootDir) }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		if msg.Width > 0 {
			m.width = msg.Width
		}
		return m, nil
	case tuiDataMsg:
		m.state, m.presets, m.syncByProfile, m.sessionShared, m.skillsShared = msg.State, msg.Presets, msg.SyncByProfile, msg.SessionShared, msg.SkillsShared
		m.sidebar = buildSidebar(msg.State)
		m.cursor = selectableCursor(m.sidebar, m.cursor)
		if m.templateCursor >= len(m.presets) {
			m.templateCursor = max(0, len(m.presets)-1)
		}
		srcOpts := m.sourceProfilesForCreate()
		if m.templateSource >= len(srcOpts) {
			m.templateSource = max(0, len(srcOpts)-1)
		}
		applyOpts := m.applyTargets()
		if m.applyIndex >= len(applyOpts) {
			m.applyIndex = max(0, len(applyOpts)-1)
		}
		return m, nil
	case tuiOpMsg:
		if msg.Err != nil {
			m.err, m.info = msg.Err.Error(), ""
		} else {
			m.err, m.info = "", msg.Info
		}
		m.mode = modeNormal
		if msg.Refresh {
			return m, refreshCmd(m.rootDir)
		}
		return m, nil
	case exportResultMsg:
		m.exportRunning = false
		if msg.Err != nil {
			m.err, m.info = msg.Err.Error(), ""
			return m, nil
		}
		m.lastExport = &msg.Result
		m.err, m.info = "", "Usage export complete"
		return m, nil
	case exportTickMsg:
		if m.exportRunning {
			m.exportElapsed = time.Since(m.exportStarted)
			return m, exportTick()
		}
		return m, nil
	}

	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	if key.String() == "q" || key.String() == "ctrl+c" {
		return m, tea.Quit
	}

	if m.mode != modeNormal {
		return m.updateMode(key)
	}

	switch key.String() {
	case "up", "k":
		m.cursor = moveCursor(m.sidebar, m.cursor, -1)
		return m, nil
	case "down", "j":
		m.cursor = moveCursor(m.sidebar, m.cursor, 1)
		return m, nil
	}

	switch m.selected().Kind {
	case sidebarAdd:
		return m.updateAdd(key)
	case sidebarExport:
		return m.updateExport(key)
	case sidebarTemplates:
		return m.updateTemplates(key)
	case sidebarProfile:
		return m.updateProfile(key)
	default:
		return m, nil
	}
}

func (m model) updateAdd(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "[", "left":
		m.addToolIdx = (m.addToolIdx + len(store.SupportedTools) - 1) % len(store.SupportedTools)
		return m, nil
	case "]", "right":
		m.addToolIdx = (m.addToolIdx + 1) % len(store.SupportedTools)
		return m, nil
	case "s":
		m.addShare = !m.addShare
		return m, nil
	case "c":
		m.addSync = !m.addSync
		return m, nil
	case "k":
		m.addSkills = !m.addSkills
		return m, nil
	case "enter":
		name := strings.TrimSpace(m.addNameInput.Value())
		if name == "" {
			m.err = "profile name is required"
			return m, nil
		}
		return m, addProfileCmd(m.rootDir, store.SupportedTools[m.addToolIdx], name, m.addShare, m.addSync, m.addSkills)
	}
	var cmd tea.Cmd
	m.addNameInput, cmd = m.addNameInput.Update(key)
	return m, cmd
}

func (m model) updateExport(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.exportRunning {
		return m, nil
	}
	if key.String() == "enter" {
		out := strings.TrimSpace(m.exportPathInput.Value())
		if out == "" {
			out = "profilex-usage-" + time.Now().Format("20060102-150405") + ".json"
			m.exportPathInput.SetValue(out)
		}
		m.exportRunning, m.exportStarted, m.exportElapsed = true, time.Now(), 0
		return m, tea.Batch(exportCmd(m.rootDir, out), exportTick())
	}
	var cmd tea.Cmd
	m.exportPathInput, cmd = m.exportPathInput.Update(key)
	return m, cmd
}

func (m model) updateTemplates(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "[":
		if len(m.presets) > 0 {
			m.templateCursor = (m.templateCursor + len(m.presets) - 1) % len(m.presets)
		}
		return m, nil
	case "]":
		if len(m.presets) > 0 {
			m.templateCursor = (m.templateCursor + 1) % len(m.presets)
		}
		return m, nil
	case ",":
		opts := m.sourceProfilesForCreate()
		if len(opts) > 0 {
			m.templateSource = (m.templateSource + len(opts) - 1) % len(opts)
		}
		return m, nil
	case ".":
		opts := m.sourceProfilesForCreate()
		if len(opts) > 0 {
			m.templateSource = (m.templateSource + 1) % len(opts)
		}
		return m, nil
	case "enter":
		opts := m.sourceProfilesForCreate()
		if len(opts) == 0 {
			m.err = "no source profiles available"
			return m, nil
		}
		name := strings.TrimSpace(m.templateName.Value())
		if name == "" {
			m.err = "template name is required"
			return m, nil
		}
		return m, createTemplateCmd(m.rootDir, m.templateTool(), opts[m.templateSource], name)
	case "a":
		if len(m.presets) == 0 {
			return m, nil
		}
		m.applyIndex = 0
		m.mode = modeTemplateApply
		return m, nil
	case "r":
		if len(m.presets) == 0 {
			return m, nil
		}
		m.prompt.SetValue(m.presets[m.templateCursor].Name)
		m.prompt.Focus()
		m.mode = modeTemplateRename
		return m, nil
	case "d", "x":
		if len(m.presets) == 0 {
			return m, nil
		}
		m.mode = modeTemplateDelete
		return m, nil
	}
	var cmd tea.Cmd
	m.templateName, cmd = m.templateName.Update(key)
	return m, cmd
}

func (m model) updateProfile(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	item := m.selected()
	switch key.String() {
	case "s":
		return m, toggleSessionCmd(m.rootDir, item.Tool, item.Profile, m.sessionShared[pk(item.Tool, item.Profile)])
	case "c":
		_, synced := m.syncByProfile[pk(item.Tool, item.Profile)]
		return m, toggleConfigSyncCmd(m.rootDir, item.Tool, item.Profile, synced)
	case "k":
		return m, toggleSkillsCmd(m.rootDir, item.Tool, item.Profile, m.skillsShared[pk(item.Tool, item.Profile)])
	case "r":
		m.prompt.SetValue(item.Profile)
		m.prompt.Focus()
		m.mode = modeProfileRename
		return m, nil
	case "d", "x":
		m.mode = modeProfileDelete
		return m, nil
	}
	return m, nil
}

func (m model) updateMode(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	if key.String() == "esc" {
		m.mode = modeNormal
		return m, nil
	}
	switch m.mode {
	case modeTemplateApply:
		opts := m.applyTargets()
		switch key.String() {
		case "up", "k":
			if len(opts) > 0 {
				m.applyIndex = (m.applyIndex + len(opts) - 1) % len(opts)
			}
		case "down", "j":
			if len(opts) > 0 {
				m.applyIndex = (m.applyIndex + 1) % len(opts)
			}
		case "enter":
			if len(opts) > 0 {
				t := m.presets[m.templateCursor]
				return m, applyTemplateCmd(m.rootDir, t.Tool, t.Name, opts[m.applyIndex])
			}
		}
		return m, nil
	case modeTemplateRename:
		if key.String() == "enter" {
			newName := strings.TrimSpace(m.prompt.Value())
			if newName == "" {
				m.err = "new template name is required"
				return m, nil
			}
			t := m.presets[m.templateCursor]
			return m, renameTemplateCmd(m.rootDir, t.Tool, t.Name, newName)
		}
		var cmd tea.Cmd
		m.prompt, cmd = m.prompt.Update(key)
		return m, cmd
	case modeTemplateDelete:
		if strings.ToLower(key.String()) == "y" || key.String() == "enter" {
			t := m.presets[m.templateCursor]
			return m, deleteTemplateCmd(m.rootDir, t.Tool, t.Name)
		}
		if strings.ToLower(key.String()) == "n" {
			m.mode = modeNormal
		}
		return m, nil
	case modeProfileRename:
		if key.String() == "enter" {
			item := m.selected()
			newName := strings.TrimSpace(m.prompt.Value())
			if newName == "" {
				m.err = "new profile name is required"
				return m, nil
			}
			return m, renameProfileCmd(m.rootDir, item.Tool, item.Profile, newName)
		}
		var cmd tea.Cmd
		m.prompt, cmd = m.prompt.Update(key)
		return m, cmd
	case modeProfileDelete:
		if strings.ToLower(key.String()) == "y" || key.String() == "enter" {
			item := m.selected()
			return m, deleteProfileCmd(m.rootDir, item.Tool, item.Profile)
		}
		if strings.ToLower(key.String()) == "n" {
			m.mode = modeNormal
		}
	}
	return m, nil
}

func (m model) View() string {
	h := lipgloss.NewStyle().Foreground(lipgloss.Color("#F8FAFC")).Background(lipgloss.Color("#0F766E")).Padding(0, 1).Bold(true).Render("ProfileX TUI | q quit")
	side := m.renderSidebar()
	main := m.renderMain()
	status := ""
	if m.err != "" {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("#B91C1C")).Bold(true).Render("Error: " + m.err)
	} else if m.info != "" {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("#065F46")).Bold(true).Render(m.info)
	}
	body := lipgloss.JoinHorizontal(lipgloss.Top, side, main)
	out := h + "\n" + body
	if status != "" {
		out += "\n" + status
	}
	return out
}

func (m model) renderSidebar() string {
	s := lipgloss.NewStyle().BorderStyle(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#64748B")).Padding(1, 1).Width(34)
	lines := []string{"Navigation"}
	for i, it := range m.sidebar {
		if !it.Selectable {
			lines = append(lines, strings.ToUpper(it.Label))
			continue
		}
		p := "  " + it.Label
		if i == m.cursor {
			p = lipgloss.NewStyle().Foreground(lipgloss.Color("#0F172A")).Background(lipgloss.Color("#A7F3D0")).Render("> " + p)
		}
		lines = append(lines, p)
	}
	return s.Render(strings.Join(lines, "\n"))
}

func (m model) renderMain() string {
	s := lipgloss.NewStyle().BorderStyle(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("#64748B")).Padding(1, 2).Width(max(60, m.width-38))
	var title string
	var lines []string
	switch m.selected().Kind {
	case sidebarAdd:
		title = "Add Profile"
		tool := store.SupportedTools[m.addToolIdx]
		lines = []string{
			fmt.Sprintf("Tool: %s  ([/])", tool),
			"Name: " + m.addNameInput.View(),
			fmt.Sprintf("Share sessions: %t (s)", m.addShare),
			fmt.Sprintf("Sync config: %t (c)", m.addSync),
			fmt.Sprintf("Share skills: %t (k)", m.addSkills),
			"",
			"Press Enter to create profile.",
		}
	case sidebarExport:
		title = "Export Usage"
		lines = []string{"Output file: " + m.exportPathInput.View(), "Press Enter to export."}
		if m.state != nil {
			lines = append(lines, fmt.Sprintf("Profiles found: %d", len(m.state.Profiles)))
		}
		if m.exportRunning {
			lines = append(lines, fmt.Sprintf("Status: running  Elapsed: %s", m.exportElapsed.Round(100*time.Millisecond)))
		}
		if m.lastExport != nil {
			r := m.lastExport
			lines = append(lines, "", "Last export:", fmt.Sprintf("File: %s", r.Out), fmt.Sprintf("Duration: %s", r.Duration.Round(100*time.Millisecond)), fmt.Sprintf("Profiles: %d Events: %d Roots: %d Files: %d", r.Profiles, r.Events, r.Roots, r.Files))
		}
	case sidebarTemplates:
		title = "Templates"
		lines = []string{"[/] choose template | a apply | r rename | d delete", "Use ',' and '.' to choose create-source profile.", "Type template name and press Enter to create.", ""}
		if len(m.presets) == 0 {
			lines = append(lines, "(no templates)")
		} else {
			for i, t := range m.presets {
				p := "  "
				if i == m.templateCursor {
					p = "> "
				}
				lines = append(lines, p+fmt.Sprintf("%s/%s", t.Tool, t.Name))
			}
		}
		opts := m.sourceProfilesForCreate()
		src := "default"
		if len(opts) > 0 {
			src = opts[clampIndex(m.templateSource, len(opts))]
		}
		lines = append(lines, "", fmt.Sprintf("Create from: %s/%s", m.templateTool(), src), "Template name: "+m.templateName.View())
	case sidebarProfile:
		title = "Profile"
		it := m.selected()
		key := pk(it.Tool, it.Profile)
		lines = []string{
			fmt.Sprintf("%s/%s", it.Tool, it.Profile),
			fmt.Sprintf("Session sharing: %t (s toggle)", m.sessionShared[key]),
		}
		lines = append(lines, fmt.Sprintf("Skills sharing: %t (k toggle)", m.skillsShared[key]))
		if p := m.syncByProfile[key]; p != "" {
			lines = append(lines, fmt.Sprintf("Config sync: on (%s) (c toggle)", p))
		} else {
			lines = append(lines, "Config sync: off (c toggle)")
		}
		lines = append(lines, "Controls: s sessions, k skills, c config-sync", "Actions: r rename, d delete")
	default:
		title = "ProfileX"
		lines = []string{"Select a sidebar item."}
	}
	if m.mode != modeNormal {
		lines = append(lines, "", "Mode: "+m.mode.String(), m.modeHint())
	}
	return s.Render(title + "\n\n" + strings.Join(lines, "\n"))
}

func (m modeKind) String() string {
	switch m {
	case modeTemplateApply:
		return "template-apply"
	case modeTemplateRename:
		return "template-rename"
	case modeTemplateDelete:
		return "template-delete"
	case modeProfileRename:
		return "profile-rename"
	case modeProfileDelete:
		return "profile-delete"
	default:
		return "normal"
	}
}

func (m model) modeHint() string {
	switch m.mode {
	case modeTemplateApply:
		opts := m.applyTargets()
		cur := "default"
		if len(opts) > 0 {
			cur = opts[clampIndex(m.applyIndex, len(opts))]
		}
		return fmt.Sprintf("Target: %s (up/down, enter apply, esc cancel)", cur)
	case modeTemplateRename, modeProfileRename:
		return "New name: " + m.prompt.View() + " (enter confirm, esc cancel)"
	case modeTemplateDelete, modeProfileDelete:
		return "Confirm delete? y/n"
	default:
		return ""
	}
}

func (m model) selected() sidebarItem {
	if m.cursor >= 0 && m.cursor < len(m.sidebar) {
		return m.sidebar[m.cursor]
	}
	return sidebarItem{Kind: sidebarHeading}
}

func (m model) templateTool() store.Tool {
	if len(m.presets) > 0 {
		return m.presets[m.templateCursor].Tool
	}
	return store.SupportedTools[m.addToolIdx]
}

func (m model) sourceProfilesForCreate() []string {
	opts := []string{"default"}
	tool := m.templateTool()
	if m.state == nil {
		return opts
	}
	for _, p := range m.state.Profiles {
		if p.Tool == tool {
			opts = append(opts, p.Name)
		}
	}
	return opts
}

func (m model) applyTargets() []string {
	if len(m.presets) == 0 {
		return nil
	}
	tool := m.presets[m.templateCursor].Tool
	out := []string{"default"}
	if m.state == nil {
		return out
	}
	for _, p := range m.state.Profiles {
		if p.Tool == tool {
			out = append(out, p.Name)
		}
	}
	return out
}

func buildSidebar(st *store.State) []sidebarItem {
	items := []sidebarItem{
		{Kind: sidebarAdd, Label: "Add Profile", Selectable: true},
		{Kind: sidebarExport, Label: "Export Usage", Selectable: true},
		{Kind: sidebarTemplates, Label: "Templates", Selectable: true},
		{Kind: sidebarHeading, Label: "Profiles", Selectable: false},
	}
	if st == nil || len(st.Profiles) == 0 {
		items = append(items, sidebarItem{Kind: sidebarHeading, Label: "(none)", Selectable: false})
		return items
	}
	for _, t := range store.SupportedTools {
		items = append(items, sidebarItem{Kind: sidebarHeading, Label: string(t), Selectable: false})
		for _, p := range st.Profiles {
			if p.Tool == t {
				items = append(items, sidebarItem{Kind: sidebarProfile, Label: "  " + p.Name, Selectable: true, Tool: p.Tool, Profile: p.Name})
			}
		}
	}
	return items
}

func selectableCursor(items []sidebarItem, cur int) int {
	if cur >= 0 && cur < len(items) && items[cur].Selectable {
		return cur
	}
	for i := range items {
		if items[i].Selectable {
			return i
		}
	}
	return 0
}

func moveCursor(items []sidebarItem, cur, step int) int {
	if len(items) == 0 {
		return 0
	}
	i := cur
	for n := 0; n < len(items); n++ {
		i += step
		if i < 0 {
			i = len(items) - 1
		}
		if i >= len(items) {
			i = 0
		}
		if items[i].Selectable {
			return i
		}
	}
	return cur
}

func pk(tool store.Tool, profile string) string { return string(tool) + "/" + profile }

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func clampIndex(idx, n int) int {
	if n <= 0 {
		return 0
	}
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

func refreshCmd(rootDir string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		st, err := mgr.Load()
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		presets, syncs, err := mgr.ListSettings(nil)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		syncBy := map[string]string{}
		for _, s := range syncs {
			syncBy[pk(s.Tool, s.Profile)] = s.Preset
		}
		shared := map[string]bool{}
		skills := map[string]bool{}
		for _, p := range st.Profiles {
			prof, e := mgr.GetProfile(st, p.Tool, p.Name)
			if e != nil {
				continue
			}
			on, e := mgr.SharedSessionsEnabled(prof)
			if e == nil {
				shared[pk(prof.Tool, prof.Name)] = on
			}
			skillsOn, e := mgr.SharedSkillsEnabled(prof)
			if e == nil {
				skills[pk(prof.Tool, prof.Name)] = skillsOn
			}
		}
		return tuiDataMsg{State: st, Presets: presets, SyncByProfile: syncBy, SessionShared: shared, SkillsShared: skills}
	}
}

func addProfileCmd(rootDir string, tool store.Tool, name string, share, sync, shareSkills bool) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		p, created, err := mgr.EnsureProfile(tool, name)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if !created {
			return tuiOpMsg{Info: "Profile already exists", Refresh: true}
		}
		if share {
			if _, err := mgr.EnableSharedSessions(p); err != nil {
				return tuiOpMsg{Err: err}
			}
		}
		if shareSkills {
			if _, err := mgr.EnableSharedSkills(p); err != nil {
				return tuiOpMsg{Err: err}
			}
		}
		if sync {
			tpl := fmt.Sprintf("default-%s", tool)
			if _, err := mgr.SnapshotSettings(tool, "default", tpl); err != nil {
				return tuiOpMsg{Err: err}
			}
			if err := mgr.SetSettingsSync(tool, name, tpl, true); err != nil {
				return tuiOpMsg{Err: err}
			}
		}
		_, _ = installShimForProfile(p)
		return tuiOpMsg{Info: fmt.Sprintf("Created %s/%s", tool, name), Refresh: true}
	}
}

func toggleSessionCmd(rootDir string, tool store.Tool, profile string, currentlyShared bool) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		st, err := mgr.Load()
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		p, err := mgr.GetProfile(st, tool, profile)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if currentlyShared {
			err = mgr.DisableSharedSessions(p)
		} else {
			_, err = mgr.EnableSharedSessions(p)
		}
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Session sharing updated", Refresh: true}
	}
}

func toggleSkillsCmd(rootDir string, tool store.Tool, profile string, currentlyShared bool) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		st, err := mgr.Load()
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		p, err := mgr.GetProfile(st, tool, profile)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if currentlyShared {
			err = mgr.DisableSharedSkills(p)
		} else {
			_, err = mgr.EnableSharedSkills(p)
		}
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Skills sharing updated", Refresh: true}
	}
}

func toggleConfigSyncCmd(rootDir string, tool store.Tool, profile string, currentlySynced bool) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if currentlySynced {
			err = mgr.SetSettingsSync(tool, profile, "", false)
		} else {
			tpl := fmt.Sprintf("default-%s", tool)
			if _, err := mgr.SnapshotSettings(tool, "default", tpl); err != nil {
				return tuiOpMsg{Err: err}
			}
			err = mgr.SetSettingsSync(tool, profile, tpl, true)
		}
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Config sync updated", Refresh: true}
	}
}

func renameProfileCmd(rootDir string, tool store.Tool, oldName, newName string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if dir, _ := shim.DefaultShimDir(); dir != "" {
			_ = shim.Remove(dir, store.Profile{Tool: tool, Name: oldName})
		}
		if err := mgr.RenameProfile(tool, oldName, newName); err != nil {
			return tuiOpMsg{Err: err}
		}
		if st, err := mgr.Load(); err == nil {
			if _, p := store.FindProfile(st, tool, newName); p != nil {
				_, _ = installShimForProfile(*p)
			}
		}
		return tuiOpMsg{Info: "Profile renamed", Refresh: true}
	}
}

func deleteProfileCmd(rootDir string, tool store.Tool, name string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if dir, _ := shim.DefaultShimDir(); dir != "" {
			_ = shim.Remove(dir, store.Profile{Tool: tool, Name: name})
		}
		if err := mgr.RemoveProfile(tool, name, false); err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Profile deleted", Refresh: true}
	}
}

func createTemplateCmd(rootDir string, tool store.Tool, source, name string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if _, err := mgr.SnapshotSettings(tool, source, name); err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Template created", Refresh: true}
	}
}

func applyTemplateCmd(rootDir string, tool store.Tool, template, target string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if err := mgr.ApplySettingsPreset(tool, template, target); err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Template applied", Refresh: true}
	}
}

func renameTemplateCmd(rootDir string, tool store.Tool, oldName, newName string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if err := mgr.RenameSettingsPreset(tool, oldName, newName); err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Template renamed", Refresh: true}
	}
}

func deleteTemplateCmd(rootDir string, tool store.Tool, name string) tea.Cmd {
	return func() tea.Msg {
		mgr, err := newManager(rootDir)
		if err != nil {
			return tuiOpMsg{Err: err}
		}
		if err := mgr.DeleteSettingsPreset(tool, name); err != nil {
			return tuiOpMsg{Err: err}
		}
		return tuiOpMsg{Info: "Template deleted", Refresh: true}
	}
}

func exportCmd(rootDir, outPath string) tea.Cmd {
	return func() tea.Msg {
		start := time.Now()
		mgr, err := newManager(rootDir)
		if err != nil {
			return exportResultMsg{Err: err}
		}
		st, err := mgr.Load()
		if err != nil {
			return exportResultMsg{Err: err}
		}
		resolvedRoot, err := resolveRootDir(rootDir)
		if err != nil {
			return exportResultMsg{Err: err}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		bundle, err := usage.GenerateBundle(ctx, st, filepath.Join(resolvedRoot, "state.json"), usage.GenerateOptions{
			RootDir:  resolvedRoot,
			Deep:     true,
			MaxFiles: 5000,
			Timezone: time.Now().Location().String(),
			CostMode: usage.CostModeAuto,
		})
		if err != nil {
			return exportResultMsg{Err: err}
		}
		if err := usage.WriteBundle(outPath, bundle); err != nil {
			return exportResultMsg{Err: err}
		}
		return exportResultMsg{Result: exportResult{
			Out:      outPath,
			Profiles: len(st.Profiles),
			Events:   len(bundle.Events),
			Roots:    len(bundle.Source.UsageRoots),
			Files:    len(bundle.Source.UsageFiles),
			Duration: time.Since(start),
		}}
	}
}

func exportTick() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(time.Time) tea.Msg { return exportTickMsg{} })
}
