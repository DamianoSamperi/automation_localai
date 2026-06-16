// Package experiment handles parallel load test orchestration:
//   - generates one locustfile per cluster
//   - runs all locust processes concurrently
//   - scrapes Prometheus for infra metrics
//   - generates an interactive HTML Plotly report
package experiment

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── Public Types ──────────────────────────────────────────────────────────────

// ClusterTarget describes one master to test during an experiment.
type ClusterTarget struct {
	ClusterID   string `json:"clusterId"`
	GatewayHost string `json:"gatewayHost"` // IP del master node, auto-resolved
	GatewayPort int    `json:"gatewayPort"`
	Model       string `json:"model"`
	ProfileCSV  string `json:"profileCsv"` // content of the load profile CSV
}

// Experiment is the runtime state of one experiment run.
type Experiment struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	StartedAt  time.Time       `json:"startedAt"`
	FinishedAt *time.Time      `json:"finishedAt,omitempty"`
	Status     string          `json:"status"` // running | done | error
	Clusters   []ClusterTarget `json:"clusters"`
	ReportFile string          `json:"reportFile,omitempty"`
	Logs       []string        `json:"logs,omitempty"`
	// internal — not serialized
	cancelCh chan struct{} `json:"-"`
	jobNames []string      `json:"-"` // populated as Jobs are created
}

// Manager owns all experiments.
type Manager struct {
	mu          sync.RWMutex
	baseDir     string
	prometheusURL string
	experiments map[string]*Experiment
	subs        map[string][]chan string
}

func NewManager(baseDir, prometheusURL string) *Manager {
	os.MkdirAll(filepath.Join(baseDir, "locustfiles"), 0755)
	os.MkdirAll(filepath.Join(baseDir, "results"), 0755)
	m := &Manager{
		baseDir:       baseDir,
		prometheusURL: prometheusURL,
		experiments:   make(map[string]*Experiment),
		subs:          make(map[string][]chan string),
	}
	m.loadExperiments()
	return m
}

// loadExperiments scans the results/ directory and reloads previously saved
// experiments from their metadata.json files.
func (m *Manager) loadExperiments() {
	resultsDir := filepath.Join(m.baseDir, "results")
	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		metaPath := filepath.Join(resultsDir, e.Name(), "metadata.json")
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var exp Experiment
		if err := json.Unmarshal(data, &exp); err != nil {
			log.Printf("[loadExperiments] failed to parse %s: %v", metaPath, err)
			continue
		}
		// Mark as done if it was left running (process crashed mid-experiment)
		if exp.Status == "running" {
			exp.Status = "interrupted"
		}
		m.experiments[exp.ID] = &exp
	}
	log.Printf("[loadExperiments] loaded %d experiments from disk", len(m.experiments))
}

// saveExperiment persists experiment metadata to disk so it survives restarts.
func (m *Manager) saveExperiment(exp *Experiment) {
	dir := filepath.Join(m.baseDir, "results", exp.ID)
	os.MkdirAll(dir, 0755)
	data, err := json.MarshalIndent(exp, "", "  ")
	if err != nil {
		log.Printf("[saveExperiment] marshal error: %v", err)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0644); err != nil {
		log.Printf("[saveExperiment] write error: %v", err)
	}
}
// Cancel terminates a running experiment by signalling its goroutine and
// deleting all locust Kubernetes Jobs associated with it.
func (m *Manager) Cancel(id string) error {
	m.mu.Lock()
	exp := m.experiments[id]
	m.mu.Unlock()
	if exp == nil {
		return fmt.Errorf("experiment %s not found", id)
	}
	if exp.Status != "running" {
		return fmt.Errorf("experiment %s is not running (status=%s)", id, exp.Status)
	}

	// Signal cancellation
	if exp.cancelCh != nil {
		select {
		case <-exp.cancelCh:
			// already closed
		default:
			close(exp.cancelCh)
		}
	}

	// Delete all jobs tagged with this experiment
	exec.Command("kubectl", "delete", "job",
		"-n", "local-ai",
		"-l", fmt.Sprintf("experiment=%s", id),
		"--ignore-not-found=true",
	).Run()

	m.mu.Lock()
	exp.Status = "cancelled"
	now := time.Now()
	exp.FinishedAt = &now
	m.mu.Unlock()
	m.saveExperiment(exp)

	return nil
}
// ── Public API ────────────────────────────────────────────────────────────────

func (m *Manager) Start(name, globalProfile string, clusters []ClusterTarget) (string, error) {
	id := fmt.Sprintf("exp-%d", time.Now().UnixMilli())
	exp := &Experiment{
		ID:        id,
		Name:      name,
		StartedAt: time.Now(),
		Status:    "running",
		Clusters:  clusters,
	}
	m.mu.Lock()
	m.experiments[id] = exp
	m.mu.Unlock()

	m.saveExperiment(exp)
	go m.run(exp, globalProfile)
	return id, nil
}

func (m *Manager) List() []*Experiment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]*Experiment, 0, len(m.experiments))
	for _, e := range m.experiments {
		list = append(list, e)
	}
	// Sort newest first
	sort.Slice(list, func(i, j int) bool {
		return list[i].StartedAt.After(list[j].StartedAt)
	})
	return list
}

func (m *Manager) Get(id string) *Experiment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.experiments[id]
}

func (m *Manager) Subscribe(id string) chan string {
	m.mu.RLock()
	if m.experiments[id] == nil {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	ch := make(chan string, 200)
	m.mu.Lock()
	m.subs[id] = append(m.subs[id], ch)
	m.mu.Unlock()
	return ch
}

func (m *Manager) ReportPath(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	exp := m.experiments[id]
	if exp == nil {
		return ""
	}
	return exp.ReportFile
}

// ── Internal run loop ─────────────────────────────────────────────────────────

func (m *Manager) run(exp *Experiment, globalProfile string) {
	addLog := func(msg string) {
		m.mu.Lock()
		exp.Logs = append(exp.Logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg))
		subs := m.subs[exp.ID]
		m.mu.Unlock()
		for _, ch := range subs {
			select {
			case ch <- msg:
			default:
			}
		}
		log.Printf("[exp:%s] %s", exp.ID, msg)
	}

	expDir := filepath.Join(m.baseDir, "results", exp.ID)
	os.MkdirAll(expDir, 0755)

	// 1. Generate one locustfile per cluster
	addLog("Generating locustfiles...")
	for i, ct := range exp.Clusters {
		profile := ct.ProfileCSV
		if profile == "" {
			profile = globalProfile
		}
		if profile == "" {
			addLog(fmt.Sprintf("⚠ [%s] profile CSV is EMPTY — locust will exit immediately!", ct.ClusterID))
		} else {
			lines := strings.Count(profile, "\n")
			addLog(fmt.Sprintf("[%s] profile: %d lines", ct.ClusterID, lines))
		}
		lf := generateLocustfile(ct, profile)
		lfPath := filepath.Join(m.baseDir, "locustfiles", fmt.Sprintf("locust_%s.py", ct.ClusterID))
		if err := os.WriteFile(lfPath, []byte(lf), 0644); err != nil {
			addLog(fmt.Sprintf("ERROR writing locustfile for %s: %v", ct.ClusterID, err))
			continue
		}
		exp.Clusters[i] = ct
		addLog(fmt.Sprintf("Locustfile ready for %s → %s:%d model=%s",
			ct.ClusterID, ct.GatewayHost, ct.GatewayPort, ct.Model))
	}

	// 2. Run all locust processes in parallel as Kubernetes Jobs
	addLog("Starting parallel load tests (inside cluster)...")
	startTime := time.Now().UTC()

	type result struct {
		clusterID string
		csvPrefix string
		err       error
	}
	resultCh := make(chan result, len(exp.Clusters))
	var wg sync.WaitGroup

	for _, ct := range exp.Clusters {
		ct := ct
		wg.Add(1)
		go func() {
			defer wg.Done()
			csvPrefix := filepath.Join(expDir, "locust_"+ct.ClusterID)
			lfPath := filepath.Join(m.baseDir, "locustfiles", fmt.Sprintf("locust_%s.py", ct.ClusterID))

			// Host: use ClusterIP service name — works reliably inside the cluster.
			// Traffic goes through Istio sidecar (Envoy) injected in the locust pod,
			// which still generates istio_requests_total metrics.
			// NodePort/MetalLB IPs may not be reachable from within the cluster on k3s.
			hostURL := fmt.Sprintf("http://local-ai-%s.local-ai.svc.cluster.local:8080", ct.ClusterID)
			addLog(fmt.Sprintf("[%s] locust → %s (ClusterIP, Istio sidecar active)", ct.ClusterID, hostURL))

			// Pass locustfile as base64 env var — avoids all CRLF/indentation issues
			lfContent, err := os.ReadFile(lfPath)
			if err != nil {
				addLog(fmt.Sprintf("[%s] ERROR reading locustfile: %v", ct.ClusterID, err))
				resultCh <- result{ct.ClusterID, csvPrefix, err}
				return
			}

			// Encode as base64 — completely avoids CRLF/indentation issues with ConfigMap/YAML mounts
			lfB64 := base64.StdEncoding.EncodeToString(lfContent)
			addLog(fmt.Sprintf("[%s] Locustfile encoded (%d bytes → %d b64 chars)", ct.ClusterID, len(lfContent), len(lfB64)))

			// Job name unique per experiment+cluster
			jobName := fmt.Sprintf("locust-%s-%d", ct.ClusterID, startTime.Unix())

			// Note: --run-time is ignored when LoadTestShape is present, so we
			// rely on the shape's tick() returning None to end the test.

			jobYAML := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: local-ai
  labels:
    cluster-id: "%s"
    role: locust
    experiment: "%s"
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: locust
          image: locustio/locust:2.32.3
          command:
            - /bin/sh
            - -c
            - |
              echo "$LOCUST_FILE_B64" | base64 -d > /tmp/locustfile.py
              echo "=== locustfile preview ===" && head -5 /tmp/locustfile.py
              exec locust -f /tmp/locustfile.py --host "%s" --headless --only-summary --exit-code-on-error 0 --loglevel INFO
          env:
            - name: LOCUST_FILE_B64
              value: "%s"
            - name: MODEL_NAME
              value: "%s"
            - name: CLUSTER_ID
              value: "%s"`,
				jobName, ct.ClusterID, exp.ID,
				hostURL,
				lfB64,
				ct.Model, ct.ClusterID)

			jobFile := filepath.Join(os.TempDir(), jobName+".yaml")
			os.WriteFile(jobFile, []byte(jobYAML), 0644)

			jobOut, jobErr := exec.Command("kubectl", "apply", "-f", jobFile).CombinedOutput()
			if jobErr != nil {
				addLog(fmt.Sprintf("[%s] ERROR creating job: %s", ct.ClusterID, string(jobOut)))
				resultCh <- result{ct.ClusterID, csvPrefix, jobErr}
				return
			}
			addLog(fmt.Sprintf("[%s] Job %s created", ct.ClusterID, jobName))

			// Stream logs from the job pod in real time
			// Wait for pod to be running first
			podName := ""
			for i := 0; i < 30; i++ {
				time.Sleep(3 * time.Second)
				out, _ := exec.Command("kubectl", "get", "pods",
					"-n", "local-ai",
					"-l", fmt.Sprintf("job-name=%s", jobName),
					"-o", "jsonpath={.items[0].metadata.name}",
				).Output()
				podName = strings.TrimSpace(string(out))
				if podName != "" {
					addLog(fmt.Sprintf("[%s] Pod ready: %s", ct.ClusterID, podName))
					break
				}
			}

			if podName != "" {
				// Stream logs from the locust pod in real time.
				// Forward REQ OK/FAIL, TEST START/STOP, errors and shape transitions
				// to the experiment log so the user can follow progress.
				logCmd := exec.Command("kubectl", "logs", "-n", "local-ai",
					"-f", "--pod-running-timeout=60s", podName)
				logOut, _ := logCmd.StdoutPipe()
				logCmd.Stderr = logCmd.Stdout
				logCmd.Start()

				okCount := 0
				failCount := 0
				buf := make([]byte, 0, 4096)
				tmp := make([]byte, 512)
				for {
					n, err := logOut.Read(tmp)
					if n > 0 {
						buf = append(buf, tmp[:n]...)
						for {
							idx := strings.Index(string(buf), "\n")
							if idx < 0 {
								break
							}
							line := strings.TrimSpace(string(buf[:idx]))
							buf = buf[idx+1:]
							if line == "" {
								continue
							}

							// Per-request completion lines
							if strings.Contains(line, "REQ OK") {
								okCount++
								// Log every request — comment out the if to throttle
								addLog(fmt.Sprintf("[%s] %s", ct.ClusterID, line))
								continue
							}
							if strings.Contains(line, "REQ FAIL") {
								failCount++
								addLog(fmt.Sprintf("[%s] %s", ct.ClusterID, line))
								continue
							}
							// Lifecycle events
							if strings.Contains(line, "TEST START") ||
								strings.Contains(line, "TEST STOP") ||
								strings.Contains(line, "Profile loaded") ||
								strings.Contains(line, "Shape test updating") ||
								strings.Contains(line, "All users spawned") ||
								strings.Contains(line, "sending request") ||
								strings.Contains(line, "ERROR") ||
								strings.Contains(line, "WARNING") {
								addLog(fmt.Sprintf("[%s] %s", ct.ClusterID, line))
								continue
							}
							// Aggregated summary (printed at end)
							if strings.Contains(line, "Aggregated") {
								addLog(fmt.Sprintf("[%s] %s", ct.ClusterID, line))
							}
						}
					}
					if err != nil {
						break
					}
				}
				logCmd.Wait()
				addLog(fmt.Sprintf("[%s] Log stream ended (OK=%d FAIL=%d)", ct.ClusterID, okCount, failCount))
			}

			// Wait for job completion
			exec.Command("kubectl", "wait",
				fmt.Sprintf("job/%s", jobName),
				"-n", "local-ai",
				"--for=condition=complete",
				"--timeout=3600s",
			).Run()

			// Also accept failed condition (locust exits 1 on errors but test ran)
			exec.Command("kubectl", "wait",
				fmt.Sprintf("job/%s", jobName),
				"-n", "local-ai",
				"--for=condition=failed",
				"--timeout=5s",
			).Run()

			// Fetch full logs for CSV parsing
			if podName != "" {
				logsOut, _ := exec.Command("kubectl", "logs", "-n", "local-ai", podName).Output()
				csvFile := csvPrefix + "_stats.csv"
				writeLocustSummaryCSV(csvFile, string(logsOut), ct.ClusterID)
			}

			// Cleanup: delete job only (no configmap created with b64 approach)
			exec.Command("kubectl", "delete", "job", jobName, "-n", "local-ai", "--ignore-not-found=true").Run()

			addLog(fmt.Sprintf("[%s] ✅ locust job done", ct.ClusterID))
			resultCh <- result{ct.ClusterID, csvPrefix, nil}
		}()
	}

	wg.Wait()
	close(resultCh)
	endTime := time.Now().UTC()

	// Check if cancelled — skip report generation
	select {
	case <-exp.cancelCh:
		addLog("Experiment cancelled by user — skipping report generation")
		now := time.Now()
		m.mu.Lock()
		exp.FinishedAt = &now
		exp.Status = "cancelled"
		subs := m.subs[exp.ID]
		m.mu.Unlock()
		m.saveExperiment(exp)
		for _, ch := range subs {
			close(ch)
		}
		return
	default:
	}

	// Collect locust CSV paths
	csvPaths := map[string]string{}
	for r := range resultCh {
		csvPaths[r.clusterID] = r.csvPrefix
	}

	// 3. Wait for Prometheus flush
	addLog("Waiting 30s for Prometheus data flush...")
	time.Sleep(30 * time.Second)

	// 4. Scrape Prometheus
	addLog("Fetching metrics from Prometheus...")
	allMetrics := map[string]interface{}{}
	for _, ct := range exp.Clusters {
		addLog(fmt.Sprintf("Scraping metrics for %s...", ct.ClusterID))
		metrics := scrapePrometheus(m.prometheusURL, ct.ClusterID, startTime, endTime)
		allMetrics[ct.ClusterID] = metrics
	}

	// Save raw JSON
	rawFile := filepath.Join(expDir, "metrics_raw.json")
	if data, err := json.MarshalIndent(allMetrics, "", "  "); err == nil {
		os.WriteFile(rawFile, data, 0644)
	}

	// 5. Generate HTML Plotly report
	addLog("Generating HTML report...")
	reportPath := filepath.Join(expDir, "report.html")
	if err := generateReport(reportPath, exp, csvPaths, allMetrics, startTime, endTime); err != nil {
		addLog("ERROR generating report: " + err.Error())
	} else {
		addLog("Report ready: " + reportPath)
	}
	addLog("Experiment complete.")

	// Close subscriber channels
	now := time.Now()
	m.mu.Lock()
	exp.FinishedAt = &now
	exp.Status = "done"
	exp.ReportFile = reportPath
	subs := m.subs[exp.ID]
	m.mu.Unlock()

	m.saveExperiment(exp)

	for _, ch := range subs {
		close(ch)
	}
}

// ── Locustfile Generator ──────────────────────────────────────────────────────

func generateLocustfile(ct ClusterTarget, profileCSV string) string {
	// Safely embed CSV in Python triple-quoted string
	safeCSV := strings.ReplaceAll(profileCSV, `\`, `\\`)
	safeCSV = strings.ReplaceAll(safeCSV, `"""`, `\"\"\"`)
	// Trim trailing whitespace from each CSV line (space after comma causes int() parse error)
	csvLines := strings.Split(safeCSV, "\n")
	for i, line := range csvLines {
		fields := strings.Split(line, ",")
		for j, f := range fields {
			fields[j] = strings.TrimSpace(f)
		}
		csvLines[i] = strings.Join(fields, ",")
	}
	safeCSV = strings.Join(csvLines, "\n")

	lines := []string{
		fmt.Sprintf("# Auto-generated locustfile for cluster: %s", ct.ClusterID),
		fmt.Sprintf("# Model: %s", ct.Model),
		"import csv, io, os, time",
		"from locust import HttpUser, task, between, LoadTestShape, events",
		"",
		fmt.Sprintf(`MODEL = os.getenv("MODEL_NAME", "%s")`, ct.Model),
		fmt.Sprintf(`CLUSTER_ID = os.getenv("CLUSTER_ID", "%s")`, ct.ClusterID),
		"",
		`PROFILE_CSV = """` + safeCSV + `"""`,
		"",
		"profile = []",
		"try:",
		"    reader = csv.DictReader(io.StringIO(PROFILE_CSV))",
		"    for row in reader:",
		`        profile.append({"second": int(row["second"]), "users": int(row["users"])})`,
		`    print(f"[{CLUSTER_ID}] Profile loaded: {len(profile)} entries, last={profile[-1] if profile else 'none'}", flush=True)`,
		"except Exception as e:",
		`    print(f"Profile parse error: {e}", flush=True)`,
		"",
		"# Event hooks to log every request as it completes",
		"@events.request.add_listener",
		"def on_request(request_type, name, response_time, response_length, exception, **kwargs):",
		`    status = "FAIL" if exception else "OK"`,
		`    print(f"[{CLUSTER_ID}] REQ {status} {request_type} {name} rt={response_time:.0f}ms len={response_length}", flush=True)`,
		"",
		"@events.test_start.add_listener",
		"def on_start(**kwargs):",
		`    print(f"[{CLUSTER_ID}] TEST START at {time.time()}", flush=True)`,
		"",
		"@events.test_stop.add_listener",
		"def on_stop(**kwargs):",
		`    print(f"[{CLUSTER_ID}] TEST STOP at {time.time()}", flush=True)`,
		"",
		"",
		"class LLMUser(HttpUser):",
		"    wait_time = between(1, 15)",
		`    long_context = "Questo è un test di contesto. " * 10 + "/no_think"`,
		"",
		"    @task",
		"    def ask_llm(self):",
		`        print(f"[{CLUSTER_ID}] sending request...", flush=True)`,
		"        payload = {",
		`            "model": MODEL,`,
		`            "messages": [{"role": "user", "content": self.long_context}],`,
		`            "temperature": 0.7,`,
		`            "max_tokens": 600`,
		"        }",
		"        with self.client.post(",
		`            "/v1/chat/completions",`,
		"            json=payload,",
		"            timeout=3600,",
		"            catch_response=True,",
		`            name=f"/v1/chat/completions [{CLUSTER_ID}]"`,
		"        ) as response:",
		"            if response.status_code == 200:",
		"                response.success()",
		"            else:",
		`                response.failure(f"HTTP {response.status_code}: {response.text[:200]}")`,
		"",
		"",
		"class CSVStrategy(LoadTestShape):",
		"    def tick(self):",
		"        if not profile:",
		`            print("WARNING: profile is empty, stopping", flush=True)`,
		"            return None",
		"        run_time = round(self.get_run_time())",
		"        current_users = 0",
		"        for entry in profile:",
		"            if entry[\"second\"] <= run_time:",
		"                current_users = entry[\"users\"]",
		"            else:",
		"                break",
		"        # Terminate as soon as we hit profile end (last entry users=0)",
		"        if run_time >= profile[-1][\"second\"]:",
		"            return None",
		"        return (current_users, 1)",
	}
	return strings.Join(lines, "\n") + "\n"
}

// ── Prometheus Scraper ────────────────────────────────────────────────────────

// metricQueries returns the PromQL queries for one cluster.
// Labels use the pattern <clusterID>_master and <clusterID>_worker_* for Istio telemetry.
// Covers: load-aware metrics (CPU/RAM/pressure) + network-aware metrics (RTT, bandwidth, saturation).
func metricQueries(clusterID string) map[string]string {
	return map[string]string{

		// =====================================================================
		// CONTAINER RESOURCES — Master
		// =====================================================================
		"ram_master": fmt.Sprintf(
			`sum(container_memory_usage_bytes{namespace="local-ai", pod=~"local-ai-%s-.*"})`, clusterID),
		"cpu_master": fmt.Sprintf(
			`sum(rate(container_cpu_usage_seconds_total{namespace="local-ai", pod=~"local-ai-%s-.*"}[30s]))`, clusterID),
		"network_tx_master": fmt.Sprintf(
			`sum(rate(container_network_transmit_bytes_total{namespace="local-ai", pod=~"local-ai-%s-.*"}[30s]))`, clusterID),
		"network_rx_master": fmt.Sprintf(
			`sum(rate(container_network_receive_bytes_total{namespace="local-ai", pod=~"local-ai-%s-.*"}[30s]))`, clusterID),

		// =====================================================================
		// CONTAINER RESOURCES — Workers
		// =====================================================================
		"ram_workers": fmt.Sprintf(
			`sum(container_memory_usage_bytes{namespace="local-ai", pod=~"localai-worker-%s-.*"})`, clusterID),
		"cpu_workers": fmt.Sprintf(
			`sum(rate(container_cpu_usage_seconds_total{namespace="local-ai", pod=~"localai-worker-%s-.*"}[30s]))`, clusterID),
		"network_tx_workers": fmt.Sprintf(
			`sum(rate(container_network_transmit_bytes_total{namespace="local-ai", pod=~"localai-worker-%s-.*"}[30s]))`, clusterID),
		"network_rx_workers": fmt.Sprintf(
			`sum(rate(container_network_receive_bytes_total{namespace="local-ai", pod=~"localai-worker-%s-.*"}[30s]))`, clusterID),

		// =====================================================================
		// ISTIO — Latenza Gateway→Master
		// Master usa hostNetwork: Istio non inietta sidecar, quindi
		// destination_app="unknown". Usiamo destination_service_name che
		// corrisponde al Service name "local-ai-<clusterID>".
		// =====================================================================
		"istio_req_total": fmt.Sprintf(
			`sum(rate(istio_requests_total{destination_service_name="local-ai-%s",reporter="source"}[30s]))`, clusterID),
		"istio_latency_p50": fmt.Sprintf(
			`histogram_quantile(0.50, sum(rate(istio_request_duration_milliseconds_bucket{destination_service_name="local-ai-%s",reporter="source"}[30s])) by (le))`, clusterID),
		"istio_latency_p95": fmt.Sprintf(
			`histogram_quantile(0.95, sum(rate(istio_request_duration_milliseconds_bucket{destination_service_name="local-ai-%s",reporter="source"}[30s])) by (le))`, clusterID),
		"istio_latency_p99": fmt.Sprintf(
			`histogram_quantile(0.99, sum(rate(istio_request_duration_milliseconds_bucket{destination_service_name="local-ai-%s",reporter="source"}[30s])) by (le))`, clusterID),

		// =====================================================================
		// ISTIO — Traffico TCP P2P
		// Workers hanno sidecar Envoy, Master no (hostNetwork).
		// Misuriamo dal lato source (worker) verso il master.
		// =====================================================================
		"istio_tcp_master_to_workers": fmt.Sprintf(
			`sum(rate(istio_tcp_sent_bytes_total{destination_service_name=~"localai-worker-%s.*",reporter="source"}[30s]))`, clusterID),
		"istio_tcp_workers_to_master": fmt.Sprintf(
			`sum(rate(istio_tcp_sent_bytes_total{destination_service_name="local-ai-%s",reporter="source"}[30s]))`, clusterID),
		"istio_tcp_recv_master": fmt.Sprintf(
			`sum(rate(istio_tcp_received_bytes_total{destination_service_name="local-ai-%s",reporter="source"}[30s]))`, clusterID),
		"istio_tcp_recv_workers": fmt.Sprintf(
			`sum(rate(istio_tcp_received_bytes_total{destination_service_name=~"localai-worker-%s.*",reporter="source"}[30s]))`, clusterID),

		// =====================================================================
		// NODE-LEVEL NETWORK (physical NIC saturation) — node_exporter
		// instance labels are hostnames: ds-cluster-01-worker-01, etc.
		// =====================================================================
		"node_net_tx_master": `sum(rate(node_network_transmit_bytes_total{instance="jetsonorigin", device!="lo"}[30s]))`,
		"node_net_rx_master": `sum(rate(node_network_receive_bytes_total{instance="jetsonorigin", device!="lo"}[30s]))`,
		"node_net_tx_drop_master": `sum(rate(node_network_transmit_drop_total{instance="jetsonorigin", device!="lo"}[30s]))`,
		"node_net_tx_workers": `sum(rate(node_network_transmit_bytes_total{instance=~"ds-cluster-01-worker-.*", device!="lo"}[30s]))`,
		"node_net_rx_workers": `sum(rate(node_network_receive_bytes_total{instance=~"ds-cluster-01-worker-.*", device!="lo"}[30s]))`,
		"node_net_drop_workers": `sum(rate(node_network_transmit_drop_total{instance=~"ds-cluster-01-worker-.*", device!="lo"}[30s]))`,

		// =====================================================================
		// LOAD-AWARE — CPU e memoria per nodo
		// =====================================================================
		"node_mem_available_master":  `node_memory_MemAvailable_bytes{instance="jetsonorigin"}`,
		"node_mem_available_worker1": `node_memory_MemAvailable_bytes{instance="ds-cluster-01-worker-01"}`,
		"node_mem_available_worker2": `node_memory_MemAvailable_bytes{instance="ds-cluster-01-worker-02"}`,
		"node_cpu_steal_master":  `avg(rate(node_cpu_seconds_total{mode="steal", instance="jetsonorigin"}[30s])) * 100`,
		"node_cpu_steal_worker1": `avg(rate(node_cpu_seconds_total{mode="steal", instance="ds-cluster-01-worker-01"}[30s])) * 100`,
		"node_cpu_steal_worker2": `avg(rate(node_cpu_seconds_total{mode="steal", instance="ds-cluster-01-worker-02"}[30s])) * 100`,
		"node_cpu_usage_master":  `(1 - avg(rate(node_cpu_seconds_total{mode="idle", instance="jetsonorigin"}[30s]))) * 100`,
		"node_cpu_usage_worker1": `(1 - avg(rate(node_cpu_seconds_total{mode="idle", instance="ds-cluster-01-worker-01"}[30s]))) * 100`,
		"node_cpu_usage_worker2": `(1 - avg(rate(node_cpu_seconds_total{mode="idle", instance="ds-cluster-01-worker-02"}[30s]))) * 100`,

		// =====================================================================
		// LOCALAI INFERENCE METRICS (se LocalAI espone /metrics)
		// Queste metriche sono disponibili se --prometheus-port è abilitato
		// in LocalAI (o se Prometheus scrapa il pod direttamente)
		// =====================================================================
		// Token generati al secondo (throughput inferenza)
		"localai_tokens_per_sec": fmt.Sprintf(
			`sum(rate(localai_tokens_generated_total{pod=~"local-ai-%s-.*"}[30s]))`, clusterID),
		// Richieste in coda (profondità della queue di inferenza)
		"localai_queue_depth": fmt.Sprintf(
			`localai_inference_queue_depth{pod=~"local-ai-%s-.*"}`, clusterID),
		// Tempo medio di inferenza (ms) per token
		"localai_inference_duration_p50": fmt.Sprintf(
			`histogram_quantile(0.50, rate(localai_inference_duration_milliseconds_bucket{pod=~"local-ai-%s-.*"}[30s]))`, clusterID),
		"localai_inference_duration_p99": fmt.Sprintf(
			`histogram_quantile(0.99, rate(localai_inference_duration_milliseconds_bucket{pod=~"local-ai-%s-.*"}[30s]))`, clusterID),

		// =====================================================================
		// CONTAINER DROPS — namespace-wide
		// =====================================================================
		"network_drops": `sum(rate(container_network_transmit_packets_dropped_total{namespace="local-ai"}[30s]))`,

		// =====================================================================
		// JETSON GPU (global)
		// =====================================================================
		"gpu_usage_jetson": `gpu_usage_percentage{instance="192.168.1.99:9401"}`,
		"ram_free_jetson":  `ram_usage{instance="192.168.1.99:9401", statistic="free"}`,
	}
}

// scrapePrometheus fetches time-series data from Prometheus for all metrics of a cluster.
func scrapePrometheus(baseURL, clusterID string, start, end time.Time) map[string]interface{} {
	result := map[string]interface{}{}
	startTS := start.Unix()
	endTS := end.Unix()

	client := &http.Client{Timeout: 30 * time.Second}

	for name, query := range metricQueries(clusterID) {
		reqURL := fmt.Sprintf("%s/api/v1/query_range", baseURL)
		req, err := http.NewRequest("GET", reqURL, nil)
		if err != nil {
			log.Printf("Prometheus %s request error: %v", name, err)
			continue
		}
		q := req.URL.Query()
		q.Set("query", query)
		q.Set("start", fmt.Sprintf("%d", startTS))
		q.Set("end", fmt.Sprintf("%d", endTS))
		q.Set("step", "10s")
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Prometheus %s error: %v", name, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var parsed struct {
			Status string `json:"status"`
			Data   struct {
				Result []promResult `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			log.Printf("Prometheus %s parse error: %v", name, err)
			continue
		}
		if parsed.Status == "success" && len(parsed.Data.Result) > 0 {
			result[name] = parsed.Data.Result
		}
	}
	return result
}

// ── Locust helpers ────────────────────────────────────────────────────────────

// calcRunTimeFromProfile parses a CSV profile (second,users header) and returns
// the maximum 'second' value. Used to set --run-time on locust so it terminates
// and prints the summary table before container exit.
func calcRunTimeFromProfile(csvContent string) int {
	maxSec := 0
	for i, line := range strings.Split(csvContent, "\n") {
		if i == 0 {
			continue // skip header
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 1 {
			continue
		}
		var sec int
		fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &sec)
		if sec > maxSec {
			maxSec = sec
		}
	}
	if maxSec == 0 {
		maxSec = 60
	}
	return maxSec
}

// indentLines adds a prefix to every non-empty line of s.
func indentLines(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n")
}

// writeLocustSummaryCSV parses locust --only-summary stdout and writes a
// _stats.csv compatible with parseLocustCSV.
func writeLocustSummaryCSV(path, logs, clusterID string) {
	header := "Type,Name,Request Count,Failure Count,Median Response Time,Average Response Time," +
		"Min Response Time,Max Response Time,Average Content Size,Requests/s,Failures/s," +
		"50%,66%,75%,80%,90%,95%,98%,99%,99.9%,99.99%,100%\n"

	rows := header
	parsed := false
	lines := strings.Split(logs, "\n")

	log.Printf("[locust-parse] total log lines: %d", len(lines))

	// ── Try 1: parse the Aggregated summary table ────────────────────────────
	var aggFields []string
	inPercentileSection := false
	for i, line := range lines {
		if strings.Contains(line, "Response time percentiles") {
			inPercentileSection = true
			log.Printf("[locust-parse] entering percentile section at line %d", i)
		}
		if inPercentileSection {
			continue
		}
		if strings.Contains(line, "Aggregated") {
			clean := strings.ReplaceAll(line, "|", " ")
			fields := strings.Fields(clean)
			if len(fields) >= 8 {
				aggFields = fields
				break
			}
		}
	}

	if len(aggFields) >= 8 {
		reqs := aggFields[1]
		fails := strings.Split(aggFields[2], "(")[0]
		avg := aggFields[3]
		minRT := aggFields[4]
		maxRT := aggFields[5]
		med := aggFields[6]
		rps := aggFields[7]
		failrps := "0"
		if len(aggFields) > 8 {
			failrps = aggFields[8]
		}

		p50, p66, p75, p80, p90, p95, p98, p99, p999, p9999, p100 :=
			med, med, med, med, med, med, med, med, med, med, maxRT

		inPerc := false
		for _, pline := range lines {
			if strings.Contains(pline, "Response time percentiles") {
				inPerc = true
				continue
			}
			if inPerc && strings.Contains(pline, "Aggregated") {
				pclean := strings.ReplaceAll(pline, "|", " ")
				pf := strings.Fields(pclean)
				if len(pf) >= 12 {
					p50 = pf[1]
					p66 = pf[2]
					p75 = pf[3]
					p80 = pf[4]
					p90 = pf[5]
					p95 = pf[6]
					p98 = pf[7]
					p99 = pf[8]
					p999 = pf[9]
					p9999 = pf[10]
					p100 = pf[11]
				}
				break
			}
		}

		csvRow := fmt.Sprintf("POST,Aggregated,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			reqs, fails, med, avg, minRT, maxRT,
			rps, failrps,
			p50, p66, p75, p80, p90, p95, p98, p99, p999, p9999, p100)
		log.Printf("[locust-parse] parsed Aggregated table → %s reqs, %s rps", reqs, rps)
		rows += csvRow
		parsed = true
	}

	// ── Try 2: fallback — parse REQ OK/FAIL event lines from listeners ────────
	// These are printed by the locustfile's @events.request.add_listener
	// even if the test gets killed before the summary table is written.
	if !parsed {
		log.Printf("[locust-parse] no Aggregated table, falling back to REQ event parsing")

		var rts []float64       // response times (ms)
		totalReqs := 0
		failedReqs := 0
		var testStart, testEnd float64

		for _, line := range lines {
			// TEST START at <timestamp>
			if idx := strings.Index(line, "TEST START at "); idx >= 0 {
				fmt.Sscanf(line[idx+len("TEST START at "):], "%f", &testStart)
				continue
			}
			if idx := strings.Index(line, "TEST STOP at "); idx >= 0 {
				fmt.Sscanf(line[idx+len("TEST STOP at "):], "%f", &testEnd)
				continue
			}
			// [<cluster>] REQ OK POST /v1/... rt=271632ms len=280
			// [<cluster>] REQ FAIL POST /v1/... rt=25044ms len=111
			if !strings.Contains(line, "REQ ") || !strings.Contains(line, "rt=") {
				continue
			}
			totalReqs++
			if strings.Contains(line, "REQ FAIL") {
				failedReqs++
			}
			// Extract rt=NUMBERms
			rtIdx := strings.Index(line, "rt=")
			if rtIdx < 0 {
				continue
			}
			rtStr := line[rtIdx+3:]
			endIdx := strings.Index(rtStr, "ms")
			if endIdx < 0 {
				continue
			}
			var rt float64
			fmt.Sscanf(rtStr[:endIdx], "%f", &rt)
			if rt > 0 {
				rts = append(rts, rt)
			}
		}

		if totalReqs > 0 {
			// Compute stats from the collected RTs
			sort.Float64s(rts)
			sum := 0.0
			for _, v := range rts {
				sum += v
			}
			avg := 0.0
			minRT, maxRT := 0.0, 0.0
			if len(rts) > 0 {
				avg = sum / float64(len(rts))
				minRT = rts[0]
				maxRT = rts[len(rts)-1]
			}
			pct := func(p float64) float64 {
				if len(rts) == 0 {
					return 0
				}
				idx := int(float64(len(rts)-1) * p)
				return rts[idx]
			}
			p50 := pct(0.50)
			p66 := pct(0.66)
			p75 := pct(0.75)
			p80 := pct(0.80)
			p90 := pct(0.90)
			p95 := pct(0.95)
			p98 := pct(0.98)
			p99 := pct(0.99)
			p999 := pct(0.999)
			p9999 := pct(0.9999)

			// Compute rps from test duration
			duration := testEnd - testStart
			if duration <= 0 {
				// fallback: use first/last log timestamps would require parsing dates;
				// rough estimate from the number of completed requests
				duration = float64(totalReqs) * (avg / 1000.0)
			}
			rps := 0.0
			if duration > 0 {
				rps = float64(totalReqs) / duration
			}
			failrps := 0.0
			if duration > 0 {
				failrps = float64(failedReqs) / duration
			}

			csvRow := fmt.Sprintf("POST,Aggregated,%d,%d,%.0f,%.0f,%.0f,%.0f,0,%.4f,%.4f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f\n",
				totalReqs, failedReqs,
				p50, avg, minRT, maxRT,
				rps, failrps,
				p50, p66, p75, p80, p90, p95, p98, p99, p999, p9999, maxRT)
			log.Printf("[locust-parse] parsed %d REQ events → avg=%.0fms p95=%.0fms rps=%.2f fail=%d", totalReqs, avg, p95, rps, failedReqs)
			rows += csvRow
			parsed = true
		}
	}

	if !parsed {
		log.Printf("[locust-parse] WARNING: no data found in logs (neither Aggregated table nor REQ events)")
		rows += "POST,Aggregated,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0\n"
	}
	os.WriteFile(path, []byte(rows), 0644)
	log.Printf("[locust-parse] wrote CSV to %s (parsed=%v)", path, parsed)
}

// splitLocustFields splits a locust table row on 2+ spaces, returning non-empty tokens.
func splitLocustFields(s string) []string {
	s = strings.TrimSpace(s)
	var result []string
	var cur strings.Builder
	spaces := 0
	for _, c := range s {
		if c == ' ' || c == '\t' {
			spaces++
		} else {
			if spaces >= 2 && cur.Len() > 0 {
				tok := strings.TrimSpace(cur.String())
				if tok != "" {
					result = append(result, tok)
				}
				cur.Reset()
			} else if spaces == 1 && cur.Len() > 0 {
				cur.WriteRune(' ')
			}
			cur.WriteRune(c)
			spaces = 0
		}
	}
	if cur.Len() > 0 {
		tok := strings.TrimSpace(cur.String())
		if tok != "" {
			result = append(result, tok)
		}
	}
	return result
}