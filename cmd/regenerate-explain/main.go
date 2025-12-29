package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// Required ClickHouse version for generating explain files
	// Update this when regenerating all test expectations
	requiredVersion = "25.8.1.3073"
	requiredLTS     = "25.8.13.73-lts"

	// Download URL for the static binary
	downloadURL = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.8.1.3073-lts/clickhouse-linux-amd64"
)

var (
	clickhouseDir  = ".clickhouse"
	clickhouseBin  = "clickhouse"
	pidFile        = filepath.Join(clickhouseDir, "clickhouse.pid")
	configFile     = filepath.Join(clickhouseDir, "config.xml")
	serverLogFile  = filepath.Join(clickhouseDir, "logs", "server.log")
	serverErrFile  = filepath.Join(clickhouseDir, "logs", "server.err.log")
)

func main() {
	testName := flag.String("test", "", "Single test directory name to process (if empty, process all)")
	dryRun := flag.Bool("dry-run", false, "Print statements without executing")
	serverOnly := flag.Bool("server", false, "Only ensure server is running, don't regenerate")
	stopServer := flag.Bool("stop", false, "Stop the ClickHouse server")
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
		fmt.Println("Dry run mode - would process tests using clickhouse local")
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

	var errors []string
	var processed, skipped int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		testDir := filepath.Join(testdataDir, entry.Name())
		if err := processTest(testDir, *dryRun); err != nil {
			if strings.Contains(err.Error(), "no statements found") {
				skipped++
				continue
			}
			errors = append(errors, fmt.Sprintf("%s: %v", entry.Name(), err))
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

	// Download if binary doesn't exist or has wrong version
	if err := ensureBinary(); err != nil {
		return fmt.Errorf("ensuring binary: %w", err)
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

	// Write to temporary file first
	tmpFile := clickhouseBin + ".tmp"
	out, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}

	written, err := io.Copy(out, resp.Body)
	out.Close()
	if err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("writing binary: %w", err)
	}

	fmt.Printf("Downloaded %d bytes\n", written)

	// Make executable and move to final location
	if err := os.Chmod(tmpFile, 0755); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("chmod: %w", err)
	}

	if err := os.Rename(tmpFile, clickhouseBin); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rename: %w", err)
	}

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

	// Start the server as a daemon
	cmd := exec.Command(clickhouseBin, "server",
		"--config-file="+configFile,
		"--daemon",
		"--pid-file="+pidFile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	// Wait for server to be ready
	fmt.Println("Waiting for server to be ready...")
	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		if version, err := getRunningVersion(); err == nil {
			if version != requiredVersion {
				return fmt.Errorf("server started with version %s but expected %s", version, requiredVersion)
			}
			fmt.Printf("ClickHouse server %s is ready\n", version)
			return nil
		}
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
        <log>%s/%s</log>
        <errorlog>%s/%s</errorlog>
        <size>100M</size>
        <count>3</count>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <mysql_port>9004</mysql_port>

    <listen_host>127.0.0.1</listen_host>

    <path>%s/%s/data/</path>
    <tmp_path>%s/%s/tmp/</tmp_path>
    <user_files_path>%s/%s/user_files/</user_files_path>
    <format_schema_path>%s/%s/format_schemas/</format_schema_path>

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
`,
		cwd, serverLogFile,
		cwd, serverErrFile,
		cwd, clickhouseDir,
		cwd, clickhouseDir,
		cwd, clickhouseDir,
		cwd, clickhouseDir,
	)

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

	fmt.Printf("Processing %s (%d statements)\n", filepath.Base(testDir), len(statements))

	// Generate version header comment
	versionHeader := fmt.Sprintf("-- Generated by ClickHouse %s\n", requiredVersion)

	for i, stmt := range statements {
		stmtNum := i + 1 // 1-indexed
		if dryRun {
			fmt.Printf("  [%d] %s\n", stmtNum, truncate(stmt, 80))
			continue
		}

		explain, err := explainAST(stmt)
		if err != nil {
			fmt.Printf("  [%d] ERROR: %v\n", stmtNum, err)
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
		fmt.Printf("  [%d] -> %s\n", stmtNum, filepath.Base(outputPath))
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

// explainAST runs EXPLAIN AST on the statement using clickhouse local
func explainAST(stmt string) (string, error) {
	query := fmt.Sprintf("EXPLAIN AST %s", stmt)
	cmd := exec.Command(clickhouseBin, "local", "--query", query)

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
