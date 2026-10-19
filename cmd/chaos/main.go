// Command chaos is a failure-injection test for a local DistribuQ
// cluster. It starts three scheduler replicas and a few workers, then
// repeats a round of:
//
//  1. submit a batch of tasks (retrying while no leader exists),
//  2. kill and restart a random worker while they run,
//  3. wait for every task to execute,
//  4. kill the current Raft leader and restart it from its saved state.
//
// It exits non-zero if any task never ran. Tasks run more than once are
// reported but are not failures: reassigning a task after a worker dies
// is at-least-once by design.
//
// Tasks submitted before a leader dies and still unfinished are NOT part
// of this test: the scheduler does not replicate task state to followers
// yet, so those are lost when their leader dies. Each round therefore
// waits for its batch to finish before the leader is killed.
//
// Usage:
//
//	mkdir -p bin
//	for c in scheduler worker submit; do go build -o bin/$c ./cmd/$c; done
//	go run ./cmd/chaos -bin ./bin
package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// proc is one managed child process (a scheduler or a worker).
type proc struct {
	name string
	bin  string
	args []string
	cmd  *exec.Cmd

	mu       sync.Mutex
	isLeader bool // schedulers only, maintained from the process log
	partial  []byte
}

// Write receives the process's log output and tracks leadership from
// the scheduler's own "elected LEADER" / "stepped down" lines.
func (p *proc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partial = append(p.partial, b...)
	for {
		i := bytes.IndexByte(p.partial, '\n')
		if i < 0 {
			break
		}
		line := string(p.partial[:i])
		p.partial = p.partial[i+1:]
		switch {
		case strings.Contains(line, "elected LEADER"):
			p.isLeader = true
		case strings.Contains(line, "stepped down"):
			p.isLeader = false
		}
	}
	return len(b), nil
}

func (p *proc) leader() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isLeader && p.alive()
}

func (p *proc) alive() bool { return p.cmd != nil && p.cmd.Process != nil && p.cmd.ProcessState == nil }

func (p *proc) start() error {
	p.mu.Lock()
	p.isLeader = false
	p.mu.Unlock()
	p.cmd = exec.Command(p.bin, p.args...)
	p.cmd.Stdout = p
	p.cmd.Stderr = p
	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", p.name, err)
	}
	return nil
}

func (p *proc) kill() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
	p.mu.Lock()
	p.isLeader = false
	p.mu.Unlock()
}

func main() {
	binDir := flag.String("bin", "./bin", "directory with the scheduler, worker and submit binaries")
	rounds := flag.Int("rounds", 3, "number of kill-the-leader rounds")
	perRound := flag.Int("tasks", 10, "tasks submitted per round")
	nWorkers := flag.Int("workers", 3, "number of workers")
	timeout := flag.Duration("timeout", 60*time.Second, "max wait for one round's tasks")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed (printed so a failure can be replayed)")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))
	fmt.Println("seed:", *seed)

	work, err := os.MkdirTemp("", "distribuq-chaos-")
	if err != nil {
		die(err)
	}
	defer os.RemoveAll(work)

	const (
		schedBase  = 18200
		workerBase = 19200
	)
	ids := []string{"s1", "s2", "s3"}
	var addrs []string
	for i := range ids {
		addrs = append(addrs, fmt.Sprintf("localhost:%d", schedBase+i))
	}
	allAddrs := strings.Join(addrs, ",")

	var scheds, workers []*proc
	defer func() {
		for _, p := range append(append([]*proc{}, scheds...), workers...) {
			p.kill()
		}
	}()

	for i, id := range ids {
		var peers []string
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid+"="+addrs[j])
			}
		}
		p := &proc{name: id, bin: filepath.Join(*binDir, "scheduler"), args: []string{
			"-id", id, "-addr", fmt.Sprintf(":%d", schedBase+i),
			"-peers", strings.Join(peers, ","),
			"-data-dir", filepath.Join(work, "state"),
		}}
		if err := p.start(); err != nil {
			die(err)
		}
		scheds = append(scheds, p)
	}
	if !waitFor(10*time.Second, func() bool { return currentLeader(scheds) != nil }) {
		die(fmt.Errorf("no leader elected at startup"))
	}

	for i := 0; i < *nWorkers; i++ {
		p := &proc{name: fmt.Sprintf("w%d", i+1), bin: filepath.Join(*binDir, "worker"), args: []string{
			"-id", fmt.Sprintf("w%d", i+1), "-addr", fmt.Sprintf(":%d", workerBase+i),
			"-scheduler", allAddrs,
		}}
		if err := p.start(); err != nil {
			die(err)
		}
		workers = append(workers, p)
	}
	time.Sleep(2 * time.Second) // let workers register

	var logs []string
	for r := 1; r <= *rounds; r++ {
		leader := currentLeader(scheds)
		if leader == nil {
			if !waitFor(15*time.Second, func() bool { return currentLeader(scheds) != nil }) {
				die(fmt.Errorf("round %d: no leader", r))
			}
			leader = currentLeader(scheds)
		}
		fmt.Printf("\n== round %d: leader is %s\n", r, leader.name)

		var batch []string
		for i := 0; i < *perRound; i++ {
			id := fmt.Sprintf("r%d-t%d", r, i)
			logPath := filepath.Join(work, id+".log")
			batch = append(batch, logPath)
			payload := fmt.Sprintf("echo run >> '%s' && sleep 1", logPath)
			if err := submitWithRetry(filepath.Join(*binDir, "submit"), id, payload, allAddrs, 20*time.Second); err != nil {
				die(fmt.Errorf("submit %s: %w", id, err))
			}
		}
		logs = append(logs, batch...)
		fmt.Printf("submitted %d tasks\n", len(batch))

		// Kill a random worker while tasks are running, then bring it back.
		time.Sleep(time.Duration(300+rng.Intn(700)) * time.Millisecond)
		w := workers[rng.Intn(len(workers))]
		fmt.Printf("killing worker %s\n", w.name)
		w.kill()
		time.Sleep(time.Second)
		if err := w.start(); err != nil {
			die(err)
		}

		if !waitFor(*timeout, func() bool { return countRan(batch) == len(batch) }) {
			fmt.Printf("round %d: only %d/%d tasks ran before the timeout\n", r, countRan(batch), len(batch))
			break
		}
		fmt.Printf("round %d: all %d tasks ran\n", r, len(batch))

		// Now kill the leader and restart it from its persisted state.
		leader = currentLeader(scheds)
		if leader == nil {
			continue
		}
		fmt.Printf("killing leader %s\n", leader.name)
		leader.kill()
		time.Sleep(500 * time.Millisecond)
		if err := leader.start(); err != nil {
			die(err)
		}
		if !waitFor(15*time.Second, func() bool { return currentLeader(scheds) != nil }) {
			die(fmt.Errorf("round %d: no new leader after killing %s", r, leader.name))
		}
		fmt.Printf("new leader: %s\n", currentLeader(scheds).name)
		time.Sleep(3 * time.Second) // workers heartbeat to the new leader and re-register
	}

	lost, dup := 0, 0
	for _, l := range logs {
		switch n := runs(l); {
		case n == 0:
			lost++
			fmt.Printf("LOST: %s never ran\n", filepath.Base(l))
		case n > 1:
			dup++
		}
	}
	fmt.Printf("\nresult: %d tasks, %d lost, %d ran more than once (expected under at-least-once)\n",
		len(logs), lost, dup)
	if lost > 0 {
		os.Exit(1)
	}
}

func currentLeader(scheds []*proc) *proc {
	for _, p := range scheds {
		if p.leader() {
			return p
		}
	}
	return nil
}

func submitWithRetry(bin, id, payload, addrs string, within time.Duration) error {
	deadline := time.Now().Add(within)
	var out []byte
	var err error
	for time.Now().Before(deadline) {
		out, err = exec.Command(bin, "-id", id, "-payload", payload, "-schedulers", addrs).CombinedOutput()
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
}

func runs(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(data), "\n")
}

func countRan(paths []string) int {
	n := 0
	for _, p := range paths {
		if runs(p) >= 1 {
			n++
		}
	}
	return n
}

func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return cond()
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "chaos:", err)
	os.Exit(1)
}
