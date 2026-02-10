package main

import (
    "bufio"
    "fmt"
    "os"
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
func getRSS(pid int) uint64 {
    f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
    if err != nil {
        return 0
    }
    defer f.Close()

    scanner := bufio.NewScanner(f)
    for scanner.Scan() {
        line := scanner.Text()
        if strings.HasPrefix(line, "VmRSS:") {
            parts := strings.Fields(line)
            kb, _ := strconv.ParseUint(parts[1], 10, 64)
            return kb * 1024 // convert KB → bytes
        }
    }

    return 0
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

    prevCPU, _ := readCPU(pid)
    prevTS := time.Now()

    for {
        cpuPct, nowCPU := getCPUPercent(pid, prevCPU, prevTS)
        rss := getRSS(pid)

        fmt.Printf("CPU: %.2f%% | RSS: %.2f MB\n",
            cpuPct,
            float64(rss)/(1024*1024),
        )

        prevCPU = nowCPU
        prevTS = time.Now()

        time.Sleep(5 * time.Second)
    }
}