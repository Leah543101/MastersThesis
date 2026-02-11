package main

import (
	"bufio"
	"fmt"
	"os"

	// "os/user"
	"strconv"
	"strings"
	"time"
)

// --------------------
// Find Spark PID
// --------------------
func findLocalSparkPid() (int, error) {
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue
		}

		cmd := string(cmdline)
		if strings.Contains(cmd, "org.apache.spark.deploy.SparkSubmit") ||
			strings.Contains(cmd, "org.apache.spark.executor.CoarseGrainedExecutorBackend") ||
			strings.Contains(cmd, "spark-submit") {
			return pid, nil
		}
	}

	return 0, fmt.Errorf("no Spark process found")
}

// --------------------
// Read CPU (percent)
// --------------------
type cpuTimes struct {
	utime uint64
	stime uint64
}

// userSpecs holds the user-specified Spark resource settings (e.g. from spark-submit args)
type userSpecs struct {
	DriverMemory   string
	ExecutorMemory string
	DriverCores    string
	ExecutorCores  string
	MemoryOverhead string
	//total_cgroup_memory string
	// thread_count string
}

func readCPU(pid int) (cpuTimes, error) {
	statBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return cpuTimes{}, err
	}

	fields := strings.Fields(string(statBytes))
	utime, _ := strconv.ParseUint(fields[13], 10, 64)
	stime, _ := strconv.ParseUint(fields[14], 10, 64)

	return cpuTimes{utime, stime}, nil
}

func getCPUPercent(pid int, prev cpuTimes, prevTS time.Time) (float64, cpuTimes) {
	now, err := readCPU(pid)
	if err != nil {
		return 0.0, prev
	}

	deltaU := float64(now.utime - prev.utime)
	deltaS := float64(now.stime - prev.stime)
	elapsed := time.Since(prevTS).Seconds()

	// Linux: 100 ticks per second
	hz := 100.0

	cpuPct := ((deltaU + deltaS) / hz / elapsed) * 100.0
	return cpuPct, now
}

// --------------------
// Read memory (RSS bytes)
// --------------------
func getRSS(pid int) (uint64, uint64, int) {
	rss := uint64(0)
	vmsize := uint64(0)
	threads := 0

	f, _ := os.Open(fmt.Sprintf("/proc/%d/status", pid))

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			parts := strings.Fields(line)
			kb, _ := strconv.ParseUint(parts[1], 10, 64)
			rss = kb * 1024 // convert KB → bytes
		}

		if strings.HasPrefix(line, "VmSize:") {
			parts := strings.Fields(line)
			kb, _ := strconv.ParseUint(parts[1], 10, 64)
			vmsize = kb * 1024
		}
		if strings.HasPrefix(line, "Threads:") {
			parts := strings.Fields(line)
			threads, _ = strconv.Atoi(parts[1])
		}

	}

	return rss, vmsize, threads
}

func GetSparkUserResourceSpecs(pid int) userSpecs {
	specs := userSpecs{

		DriverMemory:   "unknown",
		ExecutorMemory: "unknown",
		DriverCores:    "unknown",
		ExecutorCores:  "unknown",
		MemoryOverhead: "unknown",
		//total_cgroup_memory: "unknown",
		//thread_count: "unknown",
	}
	cmdBytes, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))

	args := strings.Split(string(cmdBytes), "\x00")
	for _, arg := range args {

		if strings.HasPrefix(arg, "-Xmx") {
			specs.ExecutorMemory = arg[4:]
		}
		if strings.Contains(arg, "spark.executor.memory=") {
			specs.ExecutorMemory = strings.Split(arg, "=")[1]
		}
		if strings.Contains(arg, "spark.driver.memory=") {
			specs.DriverMemory = strings.Split(arg, "=")[1]
		}
		if strings.Contains(arg, "spark.executor.cores=") {
			specs.ExecutorCores = strings.Split(arg, "=")[1]
		}
		if strings.Contains(arg, "spark.driver.cores=") {
			specs.DriverCores = strings.Split(arg, "=")[1]
		}
		if strings.Contains(arg, "spark.executor.memoryOverhead=") {
			specs.MemoryOverhead = strings.Split(arg, "=")[1]
		}

	}

	return specs

	// Read cgroup memory limit
	//if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
	//    s := strings.TrimSpace(string(data))
	//    if s != "" && s != "max" {
	//        //result["total_cgroup_memory"] = s + " bytes"
	//        specs["total_cgroup_memory"] = s

	//    }
	//}

	//statusBytes,_ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	//for _, line := range strings.Split(string(statusBytes), "\n") {
	//    if strings.HasPrefix(line, "Threads:") {
	//        parts := strings.Fields(line)
	//        if len(parts) >= 2 {
	//            result["thread_count"] = parts[1]
	//        }
	//    }
	//}

	//return specs
}

// --------------------
// Main loop
// --------------------
func main() {
	fmt.Println("Spark meter started...")

	// Wait until Spark PID appears
	var pid int
	var err error
	for pid == 0 {
		pid, err = findLocalSparkPid()
		if err == nil {
			fmt.Println("Found Spark PID:", pid)
			break
		}
		time.Sleep(1 * time.Second)
	}
	getUserSpecs := GetSparkUserResourceSpecs(pid)
	fmt.Printf("User-specified Spark resources:")
	fmt.Printf(" Driver Memory: %s\n ", getUserSpecs.DriverMemory)
	fmt.Printf(" Executor Memory: %s\n ", getUserSpecs.ExecutorMemory)
	fmt.Printf(" Driver Cores: %s\n ", getUserSpecs.DriverCores)
	fmt.Printf(" Executor Cores: %s\n ", getUserSpecs.ExecutorCores)
	fmt.Printf(" Memory Overhead: %s\n ", getUserSpecs.MemoryOverhead)
	//fmt.Printf(" Total Cgroup Memory: %s\n " userSpecs.total_cgroup_memory)
	//fmt.Printf(" Thread Count: %s\n " userSpecs.thread_count)

	prevCPU, _ := readCPU(pid)
	prevTS := time.Now()

	for {
		cpuPct, nowCPU := getCPUPercent(pid, prevCPU, prevTS)
		rss, vsize, threads := getRSS(pid)

		prevCPU = nowCPU
		prevTS = time.Now()

		time.Sleep(5 * time.Second)

		fmt.Printf("CPU: %.2f%% | RSS: %.2f MB | VmSize: %.2f MB | Threads: %d\n",
			cpuPct,
			float64(rss)/(1024*1024),
			float64(vsize)/(1024*1024),
			threads,
		)
		//prevCPU = nowCPU
		//prevTS = time.Now()

		//time.Sleep(5 * time.Second)

	}
}
