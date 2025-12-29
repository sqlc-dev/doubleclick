package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// Required ClickHouse version for generating explain files
	// Update this when regenerating all test expectations
	requiredVersion = "25.8.13.73"
	requiredLTS     = "v25.8.13.73-lts"

	// Download URL for the static binary package
	downloadURL = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.8.13.73-lts/clickhouse-common-static-25.8.13.73-amd64.tgz"
)

var (
	clickhouseDir = ".clickhouse"
	clickhouseBin = "./clickhouse"
	pidFile       = ".clickhouse/clickhouse.pid"
	configFile    = ".clickhouse/config.xml"
)

func main() {
	testName := flag.String("test", "", "Single test directory name to process (if empty, process all)")
	dryRun := flag.Bool("dry-run", false, "Print statements without executing")
	serverOnly := flag.Bool("server", false, "Only ensure server is running, don't regenerate")
	stopServer := flag.Bool("stop", false, "Stop the ClickHouse server")
	parallel := flag.Int("j", runtime.NumCPU(), "Number of parallel workers (default: number of CPUs)")
	flag.Parse()

	// Handle stop command
	if *stopServer {
		if err := stopClickHouse(); err != nil {
			fmt.Fprintf(os.Stderr, "Error stopping ClickHouse: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("ClickHouse server stopped")
		return
	}

	// Ensure ClickHouse is running with correct version
	if err := ensureClickHouse(); err != nil {
		fmt.Fprintf(os.Stderr, "Error setting up ClickHouse: %v\n", err)
		os.Exit(1)
	}

	if *serverOnly {
		fmt.Println("ClickHouse server is running with correct version")
		return
	}

	if *dryRun {
		fmt.Println("Dry run mode - would process tests using clickhouse client")
	}

	testdataDir := "parser/testdata"

	if *testName != "" {
		// Process single test
		if err := processTest(filepath.Join(testdataDir, *testName), *dryRun); err != nil {
			fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", *testName, err)
			os.Exit(1)
		}
		return
	}

	// Process all tests
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading testdata: %v\n", err)
		os.Exit(1)
	}

	// Collect test directories
	var testDirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		testDirs = append(testDirs, filepath.Join(testdataDir, entry.Name()))
	}

	// Process tests in parallel
	type result struct {
		name    string
		err     error
		skipped bool
	}

	numWorkers := *parallel
	if numWorkers < 1 {
		numWorkers = 1
	}
	fmt.Printf("Processing %d tests with %d workers...\n", len(testDirs), numWorkers)

	jobs := make(chan string, len(testDirs))
	results := make(chan result, len(testDirs))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for testDir := range jobs {
				err := processTest(testDir, *dryRun)
				skipped := err != nil && strings.Contains(err.Error(), "no statements found")
				if skipped {
					err = nil
				}
				results <- result{name: filepath.Base(testDir), err: err, skipped: skipped}
			}
		}()
	}

	// Send jobs
	for _, testDir := range testDirs {
		jobs <- testDir
	}
	close(jobs)

	// Wait for workers and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var errors []string
	var processed, skipped int
	for r := range results {
		if r.skipped {
			skipped++
		} else if r.err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", r.name, r.err))
		} else {
			processed++
		}
	}

	fmt.Printf("\nProcessed: %d, Skipped: %d, Errors: %d\n", processed, skipped, len(errors))
	if len(errors) > 0 {
		fmt.Fprintf(os.Stderr, "\nErrors:\n")
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "  %s\n", e)
		}
		os.Exit(1)
	}
}

// ensureClickHouse ensures ClickHouse is downloaded and running with the correct version
func ensureClickHouse() error {
	// Download if binary doesn't exist or has wrong version
	if err := ensureBinary(); err != nil {
		return fmt.Errorf("ensuring binary: %w", err)
	}

	// Check if already running with correct version
	if version, err := getRunningVersion(); err == nil {
		if version == requiredVersion {
			fmt.Printf("ClickHouse %s is already running\n", version)
			return nil
		}
		fmt.Printf("ClickHouse %s is running but need %s, restarting...\n", version, requiredVersion)
		if err := stopClickHouse(); err != nil {
			return fmt.Errorf("stopping existing ClickHouse: %w", err)
		}
	}

	// Start the server
	if err := startClickHouse(); err != nil {
		return fmt.Errorf("starting ClickHouse: %w", err)
	}

	return nil
}

// getRunningVersion checks if ClickHouse server is running and returns its version
func getRunningVersion() (string, error) {
	// Check PID file
	pidData, err := os.ReadFile(pidFile)
	if err != nil {
		return "", fmt.Errorf("no PID file: %w", err)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		return "", fmt.Errorf("invalid PID: %w", err)
	}

	// Check if process is running
	process, err := os.FindProcess(pid)
	if err != nil {
		return "", fmt.Errorf("process not found: %w", err)
	}
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return "", fmt.Errorf("process not running: %w", err)
	}

	// Query the server for version
	cmd := exec.Command(clickhouseBin, "client", "--query", "SELECT version()")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("querying version: %w", err)
	}

	return strings.TrimSpace(stdout.String()), nil
}

// ensureBinary downloads ClickHouse if needed
func ensureBinary() error {
	// Check if binary exists and has correct version
	if _, err := os.Stat(clickhouseBin); err == nil {
		version, err := getBinaryVersion()
		if err == nil && version == requiredVersion {
			fmt.Printf("ClickHouse binary %s already present\n", version)
			return nil
		}
		if err == nil {
			fmt.Printf("ClickHouse binary is %s but need %s, re-downloading...\n", version, requiredVersion)
		}
	}

	fmt.Printf("Downloading ClickHouse %s...\n", requiredLTS)

	// Create HTTP client with proxy support
	client := &http.Client{
		Timeout: 10 * time.Minute,
	}

	// Check for HTTPS_PROXY environment variable
	if proxyURL := os.Getenv("HTTPS_PROXY"); proxyURL != "" {
		proxy, err := url.Parse(proxyURL)
		if err == nil {
			client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxy),
			}
			fmt.Printf("Using HTTPS_PROXY: %s\n", proxyURL)
		}
	}

	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("downloading: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	// Download to temporary file
	tmpTgz := clickhouseBin + ".tgz"
	out, err := os.Create(tmpTgz)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}

	written, err := io.Copy(out, resp.Body)
	out.Close()
	if err != nil {
		os.Remove(tmpTgz)
		return fmt.Errorf("writing archive: %w", err)
	}

	fmt.Printf("Downloaded %d bytes\n", written)

	// Extract clickhouse binary from the tgz
	if err := extractClickhouseBinary(tmpTgz); err != nil {
		os.Remove(tmpTgz)
		return fmt.Errorf("extracting binary: %w", err)
	}
	os.Remove(tmpTgz)

	// Verify version
	version, err := getBinaryVersion()
	if err != nil {
		return fmt.Errorf("verifying binary: %w", err)
	}

	if version != requiredVersion {
		return fmt.Errorf("downloaded binary is version %s but expected %s", version, requiredVersion)
	}

	fmt.Printf("ClickHouse %s installed successfully\n", version)
	return nil
}

// extractClickhouseBinary extracts the clickhouse binary from a tgz archive
func extractClickhouseBinary(tgzPath string) error {
	f, err := os.Open(tgzPath)
	if err != nil {
		return err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	// Look for the clickhouse binary in the archive
	// It's at usr/bin/clickhouse
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}

		// Check if this is the clickhouse binary (at usr/bin/clickhouse)
		if header.Typeflag == tar.TypeReg && strings.HasSuffix(header.Name, "/usr/bin/clickhouse") {
			fmt.Printf("Extracting %s...\n", header.Name)

			tmpBin := clickhouseBin + ".tmp"
			outFile, err := os.OpenFile(tmpBin, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				return fmt.Errorf("creating output file: %w", err)
			}

			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				os.Remove(tmpBin)
				return fmt.Errorf("extracting file: %w", err)
			}
			outFile.Close()

			if err := os.Rename(tmpBin, clickhouseBin); err != nil {
				os.Remove(tmpBin)
				return fmt.Errorf("renaming binary: %w", err)
			}

			return nil
		}
	}

	return fmt.Errorf("clickhouse binary not found in archive")
}

// getBinaryVersion returns the version of the clickhouse binary
func getBinaryVersion() (string, error) {
	cmd := exec.Command(clickhouseBin, "local", "--query", "SELECT version()")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return strings.TrimSpace(stdout.String()), nil
}

// startClickHouse starts the ClickHouse server
func startClickHouse() error {
	// Create directories
	dirs := []string{
		filepath.Join(clickhouseDir, "data"),
		filepath.Join(clickhouseDir, "logs"),
		filepath.Join(clickhouseDir, "tmp"),
		filepath.Join(clickhouseDir, "user_files"),
		filepath.Join(clickhouseDir, "format_schemas"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating directory %s: %w", dir, err)
		}
	}

	// Create config file
	if err := writeConfig(); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	fmt.Println("Starting ClickHouse server...")

	// Start the server in background using nohup-style approach
	cmd := exec.Command(clickhouseBin, "server", "--config-file="+configFile)

	// Redirect output to log files
	logFile, err := os.OpenFile(filepath.Join(clickhouseDir, "logs", "clickhouse-server.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	errLogFile, err := os.OpenFile(filepath.Join(clickhouseDir, "logs", "clickhouse-server.err.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return fmt.Errorf("opening error log file: %w", err)
	}

	cmd.Stdout = logFile
	cmd.Stderr = errLogFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		errLogFile.Close()
		return fmt.Errorf("starting server: %w", err)
	}

	// Write PID file
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0644); err != nil {
		return fmt.Errorf("writing PID file: %w", err)
	}

	// Wait for server to be ready
	fmt.Println("Waiting for server to be ready...")
	for i := 0; i < 60; i++ {
		time.Sleep(1 * time.Second)
		// Try to connect to the server
		testCmd := exec.Command(clickhouseBin, "client", "--query", "SELECT version()")
		var stdout bytes.Buffer
		testCmd.Stdout = &stdout
		if err := testCmd.Run(); err == nil {
			version := strings.TrimSpace(stdout.String())
			if version != requiredVersion {
				return fmt.Errorf("server started with version %s but expected %s", version, requiredVersion)
			}
			fmt.Printf("ClickHouse server %s is ready\n", version)
			return nil
		}
	}

	// Check logs for errors
	if logData, err := os.ReadFile(filepath.Join(clickhouseDir, "logs", "clickhouse-server.err.log")); err == nil && len(logData) > 0 {
		fmt.Fprintf(os.Stderr, "Server error log:\n%s\n", string(logData))
	}

	return fmt.Errorf("timeout waiting for server to start")
}

// stopClickHouse stops the ClickHouse server
func stopClickHouse() error {
	pidData, err := os.ReadFile(pidFile)
	if err != nil {
		// Check if any clickhouse process is running
		exec.Command("pkill", "-f", "clickhouse server").Run()
		return nil
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		os.Remove(pidFile)
		return nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		os.Remove(pidFile)
		return nil
	}

	// Send SIGTERM
	if err := process.Signal(syscall.SIGTERM); err != nil {
		os.Remove(pidFile)
		return nil
	}

	// Wait for process to exit
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if err := process.Signal(syscall.Signal(0)); err != nil {
			os.Remove(pidFile)
			return nil
		}
	}

	// Force kill
	process.Signal(syscall.SIGKILL)
	os.Remove(pidFile)
	return nil
}

// writeConfig creates the ClickHouse server config file
func writeConfig() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	config := fmt.Sprintf(`<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>information</level>
        <log>%s/.clickhouse/logs/clickhouse-server.log</log>
        <errorlog>%s/.clickhouse/logs/clickhouse-server.err.log</errorlog>
        <size>100M</size>
        <count>3</count>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>

    <listen_host>127.0.0.1</listen_host>

    <path>%s/.clickhouse/data/</path>
    <tmp_path>%s/.clickhouse/tmp/</tmp_path>
    <user_files_path>%s/.clickhouse/user_files/</user_files_path>
    <format_schema_path>%s/.clickhouse/format_schemas/</format_schema_path>

    <mark_cache_size>5368709120</mark_cache_size>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>

    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
`, cwd, cwd, cwd, cwd, cwd, cwd)

	return os.WriteFile(configFile, []byte(config), 0644)
}

func processTest(testDir string, dryRun bool) error {
	queryPath := filepath.Join(testDir, "query.sql")
	queryBytes, err := os.ReadFile(queryPath)
	if err != nil {
		return fmt.Errorf("reading query.sql: %w", err)
	}

	statements := splitStatements(string(queryBytes))
	if len(statements) == 0 {
		return fmt.Errorf("no statements found")
	}

	testName := filepath.Base(testDir)

	if dryRun {
		fmt.Printf("Processing %s (%d statements)\n", testName, len(statements))
		for i, stmt := range statements {
			fmt.Printf("  [%d] %s\n", i+1, truncate(stmt, 80))
		}
		return nil
	}

	// Generate version header comment
	versionHeader := fmt.Sprintf("-- Generated by ClickHouse %s\n", requiredVersion)

	var stmtErrors []string
	for i, stmt := range statements {
		stmtNum := i + 1 // 1-indexed

		explain, err := explainAST(stmt)
		if err != nil {
			stmtErrors = append(stmtErrors, fmt.Sprintf("stmt %d: %v", stmtNum, err))
			// Skip statements that fail - they might be intentionally invalid
			continue
		}

		// Output filename: explain.txt for first, explain_N.txt for N >= 2
		var outputPath string
		if stmtNum == 1 {
			outputPath = filepath.Join(testDir, "explain.txt")
		} else {
			outputPath = filepath.Join(testDir, fmt.Sprintf("explain_%d.txt", stmtNum))
		}

		// Write with version header
		content := versionHeader + explain + "\n"
		if err := os.WriteFile(outputPath, []byte(content), 0644); err != nil {
			return fmt.Errorf("writing %s: %w", outputPath, err)
		}
	}

	// Print summary
	if len(stmtErrors) > 0 {
		fmt.Printf("%s: %d stmts, %d errors\n", testName, len(statements), len(stmtErrors))
	} else {
		fmt.Printf("%s: %d stmts OK\n", testName, len(statements))
	}

	return nil
}

// splitStatements splits SQL content into individual statements.
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and full-line comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Remove inline comments
		if idx := findCommentStart(trimmed); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[:idx])
			if trimmed == "" {
				continue
			}
		}

		if current.Len() > 0 {
			current.WriteString(" ")
		}
		current.WriteString(trimmed)

		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			stmt = strings.TrimSuffix(stmt, ";")
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		stmt = strings.TrimSuffix(stmt, ";")
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

func findCommentStart(line string) int {
	inString := false
	var stringChar byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inString {
			if c == '\\' && i+1 < len(line) {
				i++
				continue
			}
			if c == stringChar {
				inString = false
			}
		} else {
			if c == '\'' || c == '"' || c == '`' {
				inString = true
				stringChar = c
			} else if c == '-' && i+1 < len(line) && line[i+1] == '-' {
				if i+2 >= len(line) || line[i+2] == ' ' || line[i+2] == '\t' {
					return i
				}
			}
		}
	}
	return -1
}

// explainAST runs EXPLAIN AST on the statement using clickhouse client
func explainAST(stmt string) (string, error) {
	query := fmt.Sprintf("EXPLAIN AST %s", stmt)
	cmd := exec.Command(clickhouseBin, "client", "--query", query)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg == "" {
			errMsg = err.Error()
		}
		return "", fmt.Errorf("%s", errMsg)
	}

	return strings.TrimSpace(stdout.String()), nil
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
