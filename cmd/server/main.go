package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	log "github.com/sirupsen/logrus"
)

const useHighPerformanceRenderer = false
const logViewHeight = 5

const (
	SPINCOUNT = 5
	TERMCOUNT = 5
)

var (
	modelStyle = lipgloss.NewStyle().
		Align(lipgloss.Center, lipgloss.Center).
		BorderStyle(lipgloss.NormalBorder())
)

type mainModel struct {
	wg        *sync.WaitGroup
	cmds      []*exec.Cmd
	terminals []*terminalWriter

	logger *terminalWriter

	ready bool
}

func newModel() *mainModel {
	terminals := []*terminalWriter{}
	for range TERMCOUNT {
		terminals = append(terminals, NewTerm())
	}

	m := &mainModel{
		wg:        &sync.WaitGroup{},
		terminals: terminals,
		ready:     false,
		logger:    NewTerm(),
		cmds:      []*exec.Cmd{},
	}

	return m
}

type terminalWriter struct {
	data *strings.Builder
	sub  chan any
	vp   viewport.Model
}

func NewTerm() *terminalWriter {
	return &terminalWriter{
		data: &strings.Builder{},
		sub:  make(chan any, 10),
		vp:   viewport.Model{},
	}
}

func (tw *terminalWriter) String() string {
	return tw.data.String()
}

func (tw *terminalWriter) Write(p []byte) (n int, err error) {
	n, err = tw.data.Write(p)
	tw.sub <- struct{}{}
	return
}

func (tw *terminalWriter) WriteString(s string) (n int, err error) {
	n, err = tw.data.WriteString(s)
	tw.sub <- struct{}{}
	return
}

func (tw *terminalWriter) WaitContent() tea.Msg {
	<-tw.sub
	return tw
}

func (m *mainModel) Init() tea.Cmd {
	cmds := []tea.Cmd{
		m.logger.WaitContent,
	}

	for i := range TERMCOUNT {
		proc, cmd := Run(m.terminals[i], i, m.wg)
		m.cmds = append(m.cmds, proc)
		cmds = append(cmds, cmd)
	}
	log.Debugf("init len is %d", len(m.cmds))

	for i := range m.terminals {
		cmds = append(cmds, m.terminals[i].WaitContent)
	}

	return tea.Batch(cmds...)
}

func (m *mainModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	log.Debugf("got msg: %T", msg)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.logger.WriteString("trying to stop all servers...\n")
			for i := range m.cmds {
				err := syscall.Kill(-m.cmds[i].Process.Pid, syscall.SIGKILL)
				if err != nil {
					msg := "canceling error: " + err.Error() + "\n"
					log.Error(msg)
				}
			}
			m.logger.WriteString("DONE trying to stop all servers...\n")

			return m, tea.Quit

		case "s":

			m.logger.WriteString("trying to stop all servers...\n")
			for i := range m.cmds {
				log.Info("doing this ", i)

				err := syscall.Kill(-m.cmds[i].Process.Pid, syscall.SIGKILL)
				if err != nil {
					msg := "canceling error: " + err.Error() + "\n"
					m.logger.WriteString(msg)
					log.Error(msg)
				}
			}
			m.logger.WriteString("DONE trying to stop all servers...\n")

			return m, nil
		}
	case tea.WindowSizeMsg:
		vpW := msg.Width/TERMCOUNT - 2

		logVp := &m.logger.vp

		if !m.ready {
			*logVp = viewport.New(vpW*TERMCOUNT+8, logViewHeight)
			logVp.HighPerformanceRendering = useHighPerformanceRenderer
			logVp.SetContent("starting...")

			lvHeight := msg.Height - logViewHeight - 4

			for i := range m.terminals {
				vp := &m.terminals[i].vp

				*vp = viewport.New(vpW, lvHeight)
				vp.HighPerformanceRendering = useHighPerformanceRenderer
				vp.SetContent("waiting for data...")

			}

			m.ready = true

		} else {

			logVp.Width = vpW*TERMCOUNT + 8
			logVp.Height = logViewHeight

			lvHeight := msg.Height - logViewHeight - 4

			for i := range m.terminals {
				vp := &m.terminals[i].vp
				vp.Width = vpW
				vp.Height = lvHeight
			}

		}

		if useHighPerformanceRenderer {
			for i := range m.terminals {
				cmds = append(cmds, viewport.Sync(m.terminals[i].vp))
			}
			cmds = append(cmds, viewport.Sync(*logVp))
		}

	case *terminalWriter:

		vp := &msg.vp
		vp.SetContent(msg.data.String())
		vp.GotoBottom()

		for i := range m.terminals {
			t := m.terminals[i]
			t.vp.SetContent(t.data.String())
			t.vp.GotoBottom()
		}

		cmds = append(cmds, msg.WaitContent)

		if useHighPerformanceRenderer {
			cmds = append(cmds, viewport.Sync(*vp))
		}
	}

	for i := range m.terminals {
		m.terminals[i].vp, cmd = m.terminals[i].vp.Update(msg)
		cmds = append(cmds, cmd)
	}

	m.logger.vp, cmd = m.logger.vp.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m *mainModel) View() string {
	var s string

	strs := []string{}
	for _, view := range m.terminals {
		strs = append(strs, modelStyle.Render(view.vp.View()))
	}

	serverView := lipgloss.JoinHorizontal(
		lipgloss.Top,
		strs...,
	)

	s += lipgloss.JoinVertical(
		lipgloss.Center,
		modelStyle.Render(m.logger.vp.View()),
		serverView,
	)

	return s
}

func Run(output *terminalWriter, idx int, wg *sync.WaitGroup) (*exec.Cmd, tea.Cmd) {

	log.Info("running ", idx)

	idxStr := fmt.Sprintf("%d", idx)
	port := fmt.Sprintf(":808%d", idx)

	args := []string{
		"run", "main.go", "db.go", "schema.go",
		"--self-id", idxStr,
		"-a", port,
		"--dbprefix", fmt.Sprintf("store/store_%d.db", idx),
	}

	cmd := exec.Command("go", args...)

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	cmd.Stdout = output
	cmd.Stderr = output

	wg.Add(1)

	run := func() tea.Msg {
		defer wg.Done()
		defer output.WriteString("\n\nPROCESS KILLED\n\n")
		err := cmd.Run()

		if err != nil {
			log.Error(err.Error())
			output.WriteString("\n\nerror: " + err.Error())
			return 0
		}

		return 0
	}

	return cmd, run
}

func RunNoTUI() {
	command := "go"

	wg := sync.WaitGroup{}
	log.Info("starting")

	for i := range 5 {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("running ", i)

			idx := fmt.Sprintf("%d", i)
			port := fmt.Sprintf(":808%d", i)
			args := []string{"run", "main.go", "db.go", "schema.go", "--self-id", idx, "-a", port}

			cmd := exec.Command(command, args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Run()

			if err != nil {
				log.Error(err.Error())
				return
			}

		}()
	}

	wg.Wait()
}

func main() {

	// RunNoTUI()

	logfile := "tui.log"

	f, err := os.OpenFile(logfile, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)

	model := newModel()

	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}

	model.wg.Wait()
}
