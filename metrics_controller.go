package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"time"
)

// ── MetricsController ─────────────────────────────────────────────────────────
// Runs every 30s, scrapes Prometheus, writes annotations on nodes/deployments.
// Annotations are read by the NetworkAware scheduler plugin (sophos).
//
// Node annotations:
//   cpu-usage:                    float 0.0-1.0
//   memory-usage:                 float 0.0-1.0
//   network-latency.<peerNode>:   RTT seconds (from smokeping)
//
// Deployment annotations:
//   cpu-usage:                    float (cores)
//   memory-usage:                 float (bytes)
//   traffic.<peerApp>:            bytes/s TX toward peer

// excludedFromScheduling are nodes where we never deploy LocalAI workers.
var excludedFromScheduling = map[string]bool{
	"ds-cluster-01-cp-01":      true,
	"ds-cluster-01-management": true,
}

type MetricsController struct {
	prometheusURL string
	nodes         func() []NodeConfig
	clusters      func() []ClusterConfig
	interval      time.Duration
	httpClient    *http.Client
}

func NewMetricsController(
	prometheusURL string,
	nodesFn func() []NodeConfig,
	clustersFn func() []ClusterConfig,
) *MetricsController {
	return &MetricsController{
		prometheusURL: prometheusURL,
		nodes:         nodesFn,
		clusters:      clustersFn,
		interval:      30 * time.Second,
		httpClient:    &http.Client{Timeout: 15 * time.Second},
	}
}

func (mc *MetricsController) Start() {
	log.Println("[MetricsController] started — annotating nodes every 30s")
	go func() {
		for {
			mc.run()
			time.Sleep(mc.interval)
		}
	}()
}

func (mc *MetricsController) run() {
	allNodes := mc.nodes()
	if len(allNodes) == 0 {
		return
	}

	// Only deployable nodes (exclude cp and management)
	var deployable []NodeConfig
	for _, n := range allNodes {
		if !excludedFromScheduling[n.Name] {
			deployable = append(deployable, n)
		}
	}

	for _, node := range deployable {
		mc.annotateNode(node, deployable)
	}

	for _, cluster := range mc.clusters() {
		if cluster.Status != "running" {
			continue
		}
		mc.annotateDeployments(cluster)
	}
}

// ── Node annotations ──────────────────────────────────────────────────────────
// Units expected by sophos helpers:
//   cpu-usage:    millicores (float) — plugin divides by node.Allocatable.MilliCPU
//   memory-usage: bytes (float)      — plugin divides by node.Allocatable.Memory
//   network-latency.<peer>: seconds  — NetworkAware multiplies by traffic

func (mc *MetricsController) annotateNode(node NodeConfig, peers []NodeConfig) {
	ann := map[string]string{}

	// cpu-usage in millicores: (1 - idle_ratio) * total_millicores
	// We write used millicores so plugin can divide by Allocatable.MilliCPU
	cpuUsed := mc.scalar(fmt.Sprintf(
		`sum(rate(node_cpu_seconds_total{mode!="idle",instance="%s"}[60s])) * 1000`,
		node.Name))
	if cpuUsed >= 0 {
		ann["cpu-usage"] = fmt.Sprintf("%.2f", cpuUsed)
	}

	// memory-usage in bytes: MemTotal - MemAvailable
	avail := mc.scalar(fmt.Sprintf(`node_memory_MemAvailable_bytes{instance="%s"}`, node.Name))
	total := mc.scalar(fmt.Sprintf(`node_memory_MemTotal_bytes{instance="%s"}`, node.Name))
	if avail >= 0 && total > 0 {
		ann["memory-usage"] = fmt.Sprintf("%.0f", total-avail)
	}

	// network-latency.<peerNode>: RTT in seconds from smokeping
	for _, peer := range peers {
		if peer.Name == node.Name {
			continue
		}
		rtt := mc.smokepingRTT(peer.Host)
		if rtt >= 0 {
			ann["network-latency."+peer.Name] = fmt.Sprintf("%.6f", rtt)
		}
	}

	if len(ann) == 0 {
		return
	}
	mc.annotateNode_(node.Name, ann)
}

func (mc *MetricsController) smokepingRTT(targetIP string) float64 {
	// avg RTT across all smokeping pods measuring that target
	q := fmt.Sprintf(
		`rate(smokeping_response_duration_seconds_sum{ip="%s"}[60s]) `+
			`/ rate(smokeping_response_duration_seconds_count{ip="%s"}[60s])`,
		targetIP, targetIP)
	results := mc.vector(q)
	if len(results) == 0 {
		return -1
	}
	// Use minimum RTT (most optimistic path)
	min := -1.0
	for _, r := range results {
		if r.val > 0 && (min < 0 || r.val < min) {
			min = r.val
		}
	}
	return min
}

// ── Deployment annotations ────────────────────────────────────────────────────
// Units expected by sophos helpers:
//   cpu-usage:    millicores (float) — GetAppCpuUsage divides by node.Allocatable.MilliCPU
//   memory-usage: bytes (float)      — GetAppMemoryUsage divides by node.Allocatable.Memory
//   traffic.<app>: bytes/s (float)   — GetAppTraffic reads raw bytes/s

func (mc *MetricsController) annotateDeployments(cluster ClusterConfig) {
	masterApp := cluster.ID + "_master"

	// ── Master ──
	masterAnn := map[string]string{}

	// cpu-usage in millicores: rate() gives cores, multiply by 1000
	cpuM := mc.scalar(fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace="local-ai",pod=~"local-ai-%s-.*"}[60s])) * 1000`,
		cluster.ID))
	if cpuM >= 0 {
		masterAnn["cpu-usage"] = fmt.Sprintf("%.2f", cpuM)
	}

	// memory-usage in bytes (working set)
	ramM := mc.scalar(fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace="local-ai",pod=~"local-ai-%s-.*"})`,
		cluster.ID))
	if ramM >= 0 {
		masterAnn["memory-usage"] = fmt.Sprintf("%.0f", ramM)
	}

	// traffic.<workerApp>: bytes/s TX from worker node
	for _, w := range cluster.Workers {
		if w.NodeName == "" {
			continue
		}
		workerApp := fmt.Sprintf("%s_worker_%s", cluster.ID, w.NodeName)
		tx := mc.scalar(fmt.Sprintf(
			`sum(rate(node_network_transmit_bytes_total{instance="%s",device!="lo"}[60s]))`,
			w.NodeName))
		if tx >= 0 {
			masterAnn["traffic."+workerApp] = fmt.Sprintf("%.0f", tx)
		}
	}

	if len(masterAnn) > 0 {
		mc.annotateDeployment_("local-ai", "local-ai-"+cluster.ID, masterAnn)
	}

	// ── Workers ──
	for _, w := range cluster.Workers {
		if w.NodeName == "" {
			continue
		}
		wAnn := map[string]string{}
		deployName := fmt.Sprintf("localai-worker-%s-%s", cluster.ID, w.NodeName)

		// cpu-usage in millicores
		cpuW := mc.scalar(fmt.Sprintf(
			`sum(rate(container_cpu_usage_seconds_total{namespace="local-ai",pod=~"%s-.*"}[60s])) * 1000`,
			deployName))
		if cpuW >= 0 {
			wAnn["cpu-usage"] = fmt.Sprintf("%.2f", cpuW)
		}

		// memory-usage in bytes
		ramW := mc.scalar(fmt.Sprintf(
			`sum(container_memory_working_set_bytes{namespace="local-ai",pod=~"%s-.*"})`,
			deployName))
		if ramW >= 0 {
			wAnn["memory-usage"] = fmt.Sprintf("%.0f", ramW)
		}

		// traffic toward master in bytes/s
		tx := mc.scalar(fmt.Sprintf(
			`sum(rate(node_network_transmit_bytes_total{instance="%s",device!="lo"}[60s]))`,
			w.NodeName))
		if tx >= 0 {
			wAnn["traffic."+masterApp] = fmt.Sprintf("%.0f", tx)
		}

		if len(wAnn) > 0 {
			mc.annotateDeployment_("local-ai", deployName, wAnn)
		}
	}
}

// ── Prometheus helpers ────────────────────────────────────────────────────────

type promResult struct {
	metric map[string]string
	val    float64
}

func (mc *MetricsController) scalar(query string) float64 {
	r := mc.vector(query)
	if len(r) == 0 {
		return -1
	}
	return r[0].val
}

func (mc *MetricsController) vector(query string) []promResult {
	req, err := http.NewRequest("GET", mc.prometheusURL+"/api/v1/query", nil)
	if err != nil {
		return nil
	}
	q := req.URL.Query()
	q.Set("query", query)
	req.URL.RawQuery = q.Encode()

	resp, err := mc.httpClient.Do(req)
	if err != nil {
		log.Printf("[MetricsController] query error: %v", err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var out struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &out) != nil || out.Status != "success" {
		return nil
	}

	var results []promResult
	for _, r := range out.Data.Result {
		if len(r.Value) < 2 {
			continue
		}
		s, _ := r.Value[1].(string)
		if s == "NaN" || s == "+Inf" || s == "-Inf" {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		results = append(results, promResult{metric: r.Metric, val: v})
	}
	return results
}

// ── kubectl helpers ───────────────────────────────────────────────────────────

func (mc *MetricsController) annotateNode_(name string, ann map[string]string) {
	args := []string{"annotate", "node", name, "--overwrite"}
	for k, v := range ann {
		args = append(args, k+"="+v)
	}
	if out, err := exec.Command("kubectl", args...).CombinedOutput(); err != nil {
		log.Printf("[MetricsController] annotate node %s: %s", name, out)
	} else {
		log.Printf("[MetricsController] node/%s annotated (%d keys)", name, len(ann))
	}
}

func (mc *MetricsController) annotateDeployment_(ns, name string, ann map[string]string) {
	args := []string{"annotate", "deployment", name, "-n", ns, "--overwrite"}
	for k, v := range ann {
		args = append(args, k+"="+v)
	}
	if out, err := exec.Command("kubectl", args...).CombinedOutput(); err != nil {
		log.Printf("[MetricsController] annotate deploy %s: %s", name, out)
	} else {
		log.Printf("[MetricsController] deploy/%s annotated (%d keys)", name, len(ann))
	}
}