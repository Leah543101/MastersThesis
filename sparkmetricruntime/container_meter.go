package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const ticksPerSecond = 100.0 // typical Linux HZ=100; adjust via env HZ if you need

// ----------------------------
// Small types
// ----------------------------
type cpuTimes struct {
	utime uint64
	stime uint64
}

type cgroupLimits struct {
	Version       int     // 1 or 2
	CPUQuotaCores float64 // 0 => unlimited
	MemLimitBytes uint64  // 0 => unlimited
}

type trackedProc struct {
	pid    int
	specs  userSpecs
	prevCT cpuTimes
	prevTS time.Time
}

type userSpecs struct {
	AppName        string
	DriverMemory   string
	ExecutorMemory string
	DriverCores    string
	ExecutorCores  string
	MemoryOverhead string
	Label          string // master/worker/executor/spark
}

// ----------------------------
// Read helpers for files and cmdline
// ----------------------------
func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func isDigits(name string) bool {
	for _, r := range name {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(name) > 0
}

func readCmdline(pid int) string {
	return string(mustRead(fmt.Sprintf("/proc/%d/cmdline", pid)))
}

func mustRead(path string) []byte {
	b, _ := os.ReadFile(path)
	return b
}

// ----------------------------
// Detect our (this process) cgroup path
// ----------------------------
type selfCgroup struct {
	version   int
	unified   string // v2 path (id 0)
	cpuPathV1 string // v1 cpu controller path
	memPathV1 string // v1 memory controller path
}

func readSelfCgroup() selfCgroup {
	sc := selfCgroup{}
	// detect cgroup v2 presence
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		sc.version = 2
	} else {
		sc.version = 1
	}

	b := mustRead("/proc/self/cgroup")
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if sc.version == 1 {
		for _, ln := range lines {
			parts := strings.SplitN(ln, ":", 3)
			if len(parts) != 3 {
				continue
			}
			ctrl, path := parts[1], parts[2]
			if ctrl == "cpu" || strings.Contains(","+ctrl+",", ",cpu,") {
				sc.cpuPathV1 = path
			}
			if ctrl == "memory" || strings.Contains(","+ctrl+",", ",memory,") {
				sc.memPathV1 = path
			}
		}
	} else {
		// unified path appears on hierarchy "0"
		for _, ln := range lines {
			parts := strings.SplitN(ln, ":", 3)
			if len(parts) == 3 && parts[0] == "0" {
				sc.unified = parts[2]
				break
			}
		}
	}
	return sc
}

// ----------------------------
// Determine if a PID belongs to "our" container/pod cgroup
// (first-pass filter to target the Spark JVM in the same container)
// ----------------------------
func sameCgroup(pid int, self selfCgroup) bool {
	b := mustRead(fmt.Sprintf("/proc/%d/cgroup", pid))
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if self.version == 1 {
		var cpuPath, memPath string
		for _, ln := range lines {
			parts := strings.SplitN(ln, ":", 3)
			if len(parts) != 3 {
				continue
			}
			ctrl, path := parts[1], parts[2]
			if ctrl == "cpu" || strings.Contains(","+ctrl+",", ",cpu,") {
				cpuPath = path
			}
			if ctrl == "memory" || strings.Contains(","+ctrl+",", ",memory,") {
				memPath = path
			}
		}
		// A simple equality check is usually sufficient. If your runtime nests groups,
		// you can relax to prefix match.
		return cpuPath == self.cpuPathV1 || memPath == self.memPathV1
	} else {
		var unified string
		for _, ln := range lines {
			parts := strings.SplitN(ln, ":", 3)
			if len(parts) == 3 && parts[0] == "0" {
				unified = parts[2]
				break
			}
		}
		return unified == self.unified
	}
}

// ----------------------------
// Identify Spark role and app name from cmdline
// ----------------------------
func parseUserSpecsFromCmdline(cmdArgs []string) userSpecs {
	u := userSpecs{
		AppName:        "unknown",
		DriverMemory:   "unknown",
		ExecutorMemory: "unknown",
		DriverCores:    "unknown",
		ExecutorCores:  "unknown",
		MemoryOverhead: "unknown",
		Label:          "spark",
	}

	// role
	joined := strings.Join(cmdArgs, " ")
	switch {
	case strings.Contains(joined, "org.apache.spark.deploy.master.Master"):
		u.Label = "master"
	case strings.Contains(joined, "org.apache.spark.deploy.worker.Worker"):
		u.Label = "worker"
	case strings.Contains(joined, "org.apache.spark.executor.CoarseGrainedExecutorBackend"):
		u.Label = "executor"
	}

	// explicit app name via --name <value>
	for i := 0; i < len(cmdArgs); i++ {
		if cmdArgs[i] == "--name" && i+1 < len(cmdArgs) {
			u.AppName = cmdArgs[i+1]
			break
		}
	}
	// fallback app name from script/jar
	if u.AppName == "unknown" {
		for _, a := range cmdArgs {
			if strings.HasSuffix(a, ".py") || strings.HasSuffix(a, ".jar") || strings.HasSuffix(a, ".scala") {
				parts := strings.Split(a, "/")
				u.AppName = parts[len(parts)-1]
				break
			}
		}
	}

	// memory/cores from spark.conf args or Xmx
	for _, a := range cmdArgs {
		if strings.HasPrefix(a, "-Xmx") {
			u.ExecutorMemory = a[4:]
		}
		if strings.Contains(a, "spark.executor.memory=") {
			u.ExecutorMemory = strings.SplitN(a, "=", 2)[1]
		}
		if strings.Contains(a, "spark.driver.memory=") {
			u.DriverMemory = strings.SplitN(a, "=", 2)[1]
		}
		if strings.Contains(a, "spark.executor.cores=") {
			u.ExecutorCores = strings.SplitN(a, "=", 2)[1]
		}
		if strings.Contains(a, "spark.driver.cores=") {
			u.DriverCores = strings.SplitN(a, "=", 2)[1]
		}
		if strings.Contains(a, "spark.executor.memoryOverhead=") {
			u.MemoryOverhead = strings.SplitN(a, "=", 2)[1]
		}
	}

	return u
}

// ----------------------------
// Auto-detect Spark PID & label (no flags).
// Pass 1: same cgroup. Pass 2: any PID. Pass 3: PID 1 if Java.
// ----------------------------
func detectSparkPIDAndSpecs() (pid int, specs userSpecs, err error) {
	// optional overrides via env (still "no flags" for users)
	if v := os.Getenv("METER_TARGET_PID"); v != "" {
		if p, e := strconv.Atoi(v); e == nil && p > 0 {
			cmd := readCmdline(p)
			args := strings.Split(cmd, "\x00")
			return p, parseUserSpecsFromCmdline(args), nil
		}
	}
	selfCg := readSelfCgroup()

	type cand struct {
		pid   int
		specs userSpecs
		score int // prefer master > worker > executor > spark
	}
	scoreOf := func(label string) int {
		switch label {
		case "master":
			return 4
		case "worker":
			return 3
		case "executor":
			return 2
		default:
			return 1
		}
	}

	// helper to scan with a cgroup filter
	scan := func(requireSameCgroup bool) (cand, bool) {
		best := cand{}
		found := false
		entries, _ := os.ReadDir("/proc")
		for _, e := range entries {
			name := e.Name()
			if !isDigits(name) {
				continue
			}
			p, _ := strconv.Atoi(name)
			if requireSameCgroup && !sameCgroup(p, selfCg) {
				continue
			}
			cmd := readCmdline(p)
			if cmd == "" {
				continue
			}
			args := strings.Split(cmd, "\x00")
			joined := strings.Join(args, " ")
			if strings.Contains(joined, "org.apache.spark") || strings.Contains(joined, "spark-submit") || strings.Contains(joined, "CoarseGrainedExecutorBackend") {
				s := parseUserSpecsFromCmdline(args)
				sc := scoreOf(s.Label)
				if !found || sc > best.score {
					best = cand{pid: p, specs: s, score: sc}
					found = true
				}
			}
		}
		return best, found
	}

	// Pass 1: restrict to our cgroup (same container)
	if c, ok := scan(true); ok {
		return c.pid, c.specs, nil
	}
	// Pass 2: any PID (works in PID-shared sidecar/host)
	if c, ok := scan(false); ok {
		return c.pid, c.specs, nil
	}
	// Pass 3: fallback to PID 1 if it's a java/spark
	cmd1 := readCmdline(1)
	if cmd1 != "" {
		args := strings.Split(cmd1, "\x00")
		joined := strings.Join(args, " ")
		if strings.Contains(joined, "java") || strings.Contains(joined, "org.apache.spark") {
			return 1, parseUserSpecsFromCmdline(args), nil
		}
	}
	return 0, userSpecs{}, errors.New("Spark JVM not found")
}

// ----------------------------
// CPU and memory readers
// ----------------------------
func readCPU(pid int) (cpuTimes, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return cpuTimes{}, err
	}
	f := strings.Fields(string(b))
	if len(f) < 15 {
		return cpuTimes{}, errors.New("short /proc/<pid>/stat")
	}
	ut, _ := strconv.ParseUint(f[13], 10, 64)
	st, _ := strconv.ParseUint(f[14], 10, 64)
	return cpuTimes{utime: ut, stime: st}, nil
}

func cpuPct(prev, now cpuTimes, elapsedSec float64) float64 {
	if elapsedSec <= 0 {
		return 0
	}
	delta := float64((now.utime - prev.utime) + (now.stime - prev.stime))
	return (delta / ticksPerSecond / elapsedSec) * 100.0
}

func memAndThreads(pid int) (rssBytes, vmSizeBytes uint64, threads int) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			parts := strings.Fields(line)
			kb, _ := strconv.ParseUint(parts[1], 10, 64)
			rssBytes = kb * 1024
		}
		if strings.HasPrefix(line, "VmSize:") {
			parts := strings.Fields(line)
			kb, _ := strconv.ParseUint(parts[1], 10, 64)
			vmSizeBytes = kb * 1024
		}
		if strings.HasPrefix(line, "Threads:") {
			parts := strings.Fields(line)
			threads, _ = strconv.Atoi(parts[1])
		}
	}
	return
}

// ----------------------------
// cgroup limits for a PID (v1 & v2)
// ----------------------------
func readCGroupLimitsForPID(pid int) cgroupLimits {
	var lim cgroupLimits
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		lim.Version = 2
	} else {
		lim.Version = 1
	}
	b := mustRead(fmt.Sprintf("/proc/%d/cgroup", pid))
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if lim.Version == 1 {
		var cpuPath, memPath string
		for _, ln := range lines {
			parts := strings.SplitN(ln, ":", 3)
			if len(parts) != 3 {
				continue
			}
			ctrl, path := parts[1], parts[2]
			if ctrl == "cpu" || strings.Contains(","+ctrl+",", ",cpu,") {
				cpuPath = path
			}
			if ctrl == "memory" || strings.Contains(","+ctrl+",", ",memory,") {
				memPath = path
			}
		}
		if cpuPath != "" {
			qb := readFileTrim(filepath.Join("/sys/fs/cgroup/cpu", cpuPath, "cpu.cfs_quota_us"))
			pb := readFileTrim(filepath.Join("/sys/fs/cgroup/cpu", cpuPath, "cpu.cfs_period_us"))
			if qb != "" && pb != "" && qb != "-1" {
				q, _ := strconv.ParseFloat(qb, 64)
				p, _ := strconv.ParseFloat(pb, 64)
				if q > 0 && p > 0 {
					lim.CPUQuotaCores = q / p
				}
			}
		}
		if memPath != "" {
			mb := readFileTrim(filepath.Join("/sys/fs/cgroup/memory", memPath, "memory.limit_in_bytes"))
			if mb != "" {
				if v, err := strconv.ParseUint(mb, 10, 64); err == nil {
					lim.MemLimitBytes = v
				}
			}
		}
	} else {
		var path string
		for _, ln := range lines {
			parts := strings.SplitN(ln, ":", 3)
			if len(parts) == 3 && parts[0] == "0" {
				path = parts[2]
				break
			}
		}
		if path != "" {
			if s := readFileTrim(filepath.Join("/sys/fs/cgroup", path, "cpu.max")); s != "" {
				f := strings.Fields(s)
				if len(f) == 2 && f[0] != "max" {
					quota, _ := strconv.ParseFloat(f[0], 64)
					period, _ := strconv.ParseFloat(f[1], 64)
					if quota > 0 && period > 0 {
						lim.CPUQuotaCores = quota / period
					}
				}
			}
			if s := readFileTrim(filepath.Join("/sys/fs/cgroup", path, "memory.max")); s != "" && s != "max" {
				if v, err := strconv.ParseUint(s, 10, 64); err == nil {
					lim.MemLimitBytes = v
				}
			}
		}
	}
	return lim
}

func humanBytes(b uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// detectAllSparkPIDs returns all Spark-like PIDs in our container (or host),
// with their parsed userSpecs. It reuses your cgroup detection and parsing logic.
func detectAllSparkPIDs() ([]int, []userSpecs, error) {
	selfCg := readSelfCgroup()

	type cand struct {
		pid   int
		specs userSpecs
	}

	var cands []cand

	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		name := e.Name()
		if !isDigits(name) {
			continue
		}
		p, _ := strconv.Atoi(name)

		// only pids in same cgroup (i.e., same container/pod)
		if !sameCgroup(p, selfCg) {
			continue
		}

		cmd := readCmdline(p)
		if cmd == "" {
			continue
		}
		args := strings.Split(cmd, "\x00")
		joined := strings.Join(args, " ")
		if strings.Contains(joined, "org.apache.spark") ||
			strings.Contains(joined, "spark-submit") ||
			strings.Contains(joined, "CoarseGrainedExecutorBackend") {
			s := parseUserSpecsFromCmdline(args)
			cands = append(cands, cand{pid: p, specs: s})
		}
	}

	if len(cands) == 0 {
		return nil, nil, errors.New("no Spark JVMs found in this cgroup")
	}

	// split into parallel slices for convenience
	pids := make([]int, len(cands))
	specs := make([]userSpecs, len(cands))
	for i, c := range cands {
		pids[i] = c.pid
		specs[i] = c.specs
	}
	return pids, specs, nil
}

// ----------------------------
// main (no flags needed)
// ----------------------------
func main() {
	fmt.Println("Meter: auto-detecting Spark JVMs…")

	for {
		pids, specsList, err := detectAllSparkPIDs()
		if err != nil {
			fmt.Println("ERROR:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Initialize tracked processes
		var procs []trackedProc
		for i, pid := range pids {
			specs := specsList[i]
			fmt.Printf("Monitoring PID=%d (role=%s, app=%s)\n", pid, specs.Label, specs.AppName)

			// Print user specs we could infer (same as before, once per process)
			fmt.Println("User Specs (from cmdline if present):")
			fmt.Printf("  Driver Memory:   %s\n", specs.DriverMemory)
			fmt.Printf("  Executor Memory: %s\n", specs.ExecutorMemory)
			fmt.Printf("  Driver Cores:    %s\n", specs.DriverCores)
			fmt.Printf("  Executor Cores:  %s\n", specs.ExecutorCores)
			fmt.Printf("  Memory Overhead: %s\n", specs.MemoryOverhead)

			// cgroup limits (same container for all, but reading per pid is fine)
			limits := readCGroupLimitsForPID(pid)
			memLimit := "unlimited"
			if limits.MemLimitBytes > 0 {
				memLimit = humanBytes(limits.MemLimitBytes)
			}
			cpuLimit := "unlimited"
			if limits.CPUQuotaCores > 0 {
				cpuLimit = fmt.Sprintf("%.2f cores", limits.CPUQuotaCores)
			}
			if limits.Version > 0 {
				fmt.Printf("cgroup v%d | CPU limit: %s | Mem limit: %s\n",
					limits.Version, cpuLimit, memLimit)
			}
			fmt.Println()

			ct, err := readCPU(pid)
			if err != nil {
				fmt.Println("readCPU error (init):", err)
				continue
			}
			procs = append(procs, trackedProc{
				pid:    pid,
				specs:  specs,
				prevCT: ct,
				prevTS: time.Now(),
			})
		}

		if len(procs) == 0 {
			fmt.Println("No valid Spark JVMs to monitor; retrying…")
			time.Sleep(5 * time.Second)
			continue
		}

		fmt.Println("Live Usage:")
		for {
			time.Sleep(1 * time.Second)

			// refresh list periodically if some PIDs disappear
			if len(procs) == 0 {
				fmt.Println("All tracked PIDs gone; re-detecting…")
				break
			}

			var alive []trackedProc
			for _, tp := range procs {
				nowCT, err := readCPU(tp.pid)
				if err != nil {
					fmt.Printf("[%s] readCPU error for PID=%d: %v (will drop)\n",
						tp.specs.Label, tp.pid, err)
					continue
				}
				nowTS := time.Now()
				cpu := cpuPct(tp.prevCT, nowCT, nowTS.Sub(tp.prevTS).Seconds())
				rss, vmsize, threads := memAndThreads(tp.pid)

				fmt.Printf("[%s] PID=%d | CPU: %.2f%% | RSS: %s | VmSize: %s | Threads: %d\n",
					tp.specs.Label, tp.pid, cpu, humanBytes(rss), humanBytes(vmsize), threads)

				tp.prevCT = nowCT
				tp.prevTS = nowTS
				alive = append(alive, tp)
			}
			procs = alive
		}
	}
}
