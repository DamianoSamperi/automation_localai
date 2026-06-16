package experiment

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// generateReport creates a self-contained HTML file with Plotly charts.
// It includes:
//   - Per-cluster: latency, throughput, RAM, CPU, network, Istio metrics
//   - Cross-cluster comparison charts
func generateReport(
	outPath string,
	exp *Experiment,
	csvPaths map[string]string,
	allMetrics map[string]interface{},
	start, end time.Time,
) error {

	// 1. Parse Locust CSV stats per cluster
	locustData := map[string]locustStats{}
	for clusterID, prefix := range csvPaths {
		stats := parseLocustCSV(prefix)
		locustData[clusterID] = stats
	}

	// 2. Build Plotly chart JSON data
	chartsJSON, err := buildChartsJSON(exp.Clusters, locustData, allMetrics)
	if err != nil {
		return err
	}

	// 3. Render HTML
	html := renderHTML(exp, chartsJSON, start, end)
	return os.WriteFile(outPath, []byte(html), 0644)
}

// ── Prometheus types ──────────────────────────────────────────────────────────

type promResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

// ── Locust CSV parsing ────────────────────────────────────────────────────────

type locustStats struct {
	TotalRequests   int
	FailedRequests  int
	AvgResponseTime float64
	P50             float64
	P95             float64
	P99             float64
	RPS             float64
	// Time series from stats_history CSV: [{t, users, rps, p50, p95}]
	History []locustHistoryPoint
}

type locustHistoryPoint struct {
	T     float64
	Users int
	RPS   float64
	P50   float64
	P95   float64
}

func parseLocustCSV(prefix string) locustStats {
	stats := locustStats{}

	statsFile := prefix + "_stats.csv"
	if f, err := os.Open(statsFile); err == nil {
		defer f.Close()
		r := csv.NewReader(f)
		rows, readErr := r.ReadAll()
		if readErr != nil {
			log.Printf("[parseLocustCSV] CSV read error %s: %v", statsFile, readErr)
			goto history
		}
		log.Printf("[parseLocustCSV] %s: %d rows", statsFile, len(rows))
		if len(rows) < 2 {
			log.Printf("[parseLocustCSV] not enough rows in %s", statsFile)
			goto history
		}
		log.Printf("[parseLocustCSV] header: %v", rows[0])
		for ri, row := range rows[1:] {
			log.Printf("[parseLocustCSV] data row[%d]: %v", ri, row)
			if len(row) < 14 {
				continue
			}
			if row[0] == "POST" && row[1] == "Aggregated" {
				stats.TotalRequests, _ = strconv.Atoi(row[2])
				stats.FailedRequests, _ = strconv.Atoi(row[3])
				stats.AvgResponseTime, _ = strconv.ParseFloat(row[5], 64)
				stats.P50, _ = strconv.ParseFloat(row[11], 64)
				stats.P95, _ = strconv.ParseFloat(row[16], 64)
				stats.P99, _ = strconv.ParseFloat(row[18], 64)
				stats.RPS, _ = strconv.ParseFloat(row[9], 64)
				log.Printf("[parseLocustCSV] parsed stats: total=%d avgRT=%.1f p50=%.1f p95=%.1f rps=%.3f",
					stats.TotalRequests, stats.AvgResponseTime, stats.P50, stats.P95, stats.RPS)
				break
			}
		}
	} else {
		log.Printf("[parseLocustCSV] cannot open %s: %v", statsFile, err)
	}

history:
	// --- _stats_history.csv (time series) ---
	histFile := prefix + "_stats_history.csv"
	if f, err := os.Open(histFile); err == nil {
		defer f.Close()
		r := csv.NewReader(f)
		rows, _ := r.ReadAll()
		if len(rows) < 2 {
			return stats
		}
		for _, row := range rows[1:] {
			if len(row) < 9 {
				continue
			}
			t, _ := strconv.ParseFloat(row[0], 64)
			users, _ := strconv.Atoi(row[3])
			rps, _ := strconv.ParseFloat(row[6], 64)
			p50, _ := strconv.ParseFloat(row[11], 64)
			p95, _ := strconv.ParseFloat(row[13], 64)
			stats.History = append(stats.History, locustHistoryPoint{
				T: t, Users: users, RPS: rps, P50: p50, P95: p95,
			})
		}
	}

	return stats
}

// ── Chart data builder ────────────────────────────────────────────────────────

type chartsDef struct {
	// Per-cluster sections
	PerCluster map[string]clusterCharts `json:"perCluster"`
	// Comparison charts
	Comparison comparisonCharts `json:"comparison"`
}

type clusterCharts struct {
	ClusterID string        `json:"clusterId"`
	Model     string        `json:"model"`
	// Locust
	Latency plotData `json:"latency"`
	RPS     plotData `json:"rps"`
	Users   plotData `json:"users"`
	// Container resources
	RAM   plotData `json:"ram"`
	CPU   plotData `json:"cpu"`
	NetTX plotData `json:"netTx"`
	NetRX plotData `json:"netRx"`
	// Istio — latency Gateway→Master
	IstioReq  plotData `json:"istioReq"`
	IstioP50  plotData `json:"istioP50"`
	IstioP95  plotData `json:"istioP95"`
	IstioP99  plotData `json:"istioP99"`
	// Istio — TCP P2P traffic (directional)
	TCPMasterToWorkers plotData `json:"tcpMasterToWorkers"`
	TCPWorkersToMaster plotData `json:"tcpWorkersToMaster"`
	// Node-level network (physical NIC saturation)
	NodeNetTX   plotData `json:"nodeNetTx"`
	NodeNetRX   plotData `json:"nodeNetRx"`
	NodeNetDrop plotData `json:"nodeNetDrop"`
	// Node-level CPU and memory pressure
	NodeCPU plotData `json:"nodeCpu"`
	NodeMem plotData `json:"nodeMem"`
	// LocalAI inference metrics
	InferenceTokensPerSec plotData `json:"inferenceTokensPerSec"`
	InferenceQueueDepth   plotData `json:"inferenceQueueDepth"`
	InferenceLatP50       plotData `json:"inferenceLatP50"`
	InferenceLatP99       plotData `json:"inferenceLatP99"`

	Summary locustSummary `json:"summary"`
}

type locustSummary struct {
	Total   int     `json:"total"`
	Failed  int     `json:"failed"`
	AvgRT   float64 `json:"avgRt"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
	RPS     float64 `json:"rps"`
}

type plotData struct {
	X      []interface{} `json:"x"`
	Y      []interface{} `json:"y"`
	Name   string        `json:"name"`
	Series []plotSeries  `json:"series,omitempty"` // multiple lines on same chart
}

type plotSeries struct {
	Name string        `json:"name"`
	X    []interface{} `json:"x"`
	Y    []interface{} `json:"y"`
}

type comparisonCharts struct {
	AvgLatency  []barItem `json:"avgLatency"`
	P95Latency  []barItem `json:"p95Latency"`
	P99Latency  []barItem `json:"p99Latency"`
	RPS         []barItem `json:"rps"`
	ErrorRate   []barItem `json:"errorRate"`
	PeakRAM     []barItem `json:"peakRam"`
	PeakCPU     []barItem `json:"peakCpu"`
}

type barItem struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

func buildChartsJSON(
	clusters []ClusterTarget,
	locust map[string]locustStats,
	allMetrics map[string]interface{},
) (string, error) {

	charts := chartsDef{
		PerCluster: map[string]clusterCharts{},
	}

	for _, ct := range clusters {
		ls := locust[ct.ClusterID]
		metrics, _ := allMetrics[ct.ClusterID].(map[string]interface{})

		cc := clusterCharts{
			ClusterID: ct.ClusterID,
			Model:     ct.Model,
			Summary: locustSummary{
				Total:  ls.TotalRequests,
				Failed: ls.FailedRequests,
				AvgRT:  ls.AvgResponseTime,
				P50:    ls.P50,
				P95:    ls.P95,
				P99:    ls.P99,
				RPS:    ls.RPS,
			},
		}

		// Locust time series
		if len(ls.History) > 0 {
			ts := make([]interface{}, len(ls.History))
			p50s := make([]interface{}, len(ls.History))
			p95s := make([]interface{}, len(ls.History))
			rpss := make([]interface{}, len(ls.History))
			users := make([]interface{}, len(ls.History))
			for i, h := range ls.History {
				ts[i] = h.T
				p50s[i] = h.P50
				p95s[i] = h.P95
				rpss[i] = h.RPS
				users[i] = h.Users
			}
			cc.Latency = plotData{
				Name: "Latency",
				Series: []plotSeries{
					{Name: "P50 (ms)", X: ts, Y: p50s},
					{Name: "P95 (ms)", X: ts, Y: p95s},
				},
			}
			cc.RPS = plotData{Name: "Requests/s", X: ts, Y: rpss}
			cc.Users = plotData{Name: "Concurrent Users", X: ts, Y: users}
		}

		// Prometheus time series
		cc.RAM = promSeries(metrics, "ram_master", "ram_workers", "RAM Master (MB)", "RAM Workers (MB)", 1<<20)
		cc.CPU = promSeries(metrics, "cpu_master", "cpu_workers", "CPU Master (cores)", "CPU Workers (cores)", 1)
		cc.NetTX = promSeries(metrics, "network_tx_master", "network_tx_workers", "TX Master (KB/s)", "TX Workers (KB/s)", 1024)
		cc.NetRX = promSeries(metrics, "network_rx_master", "network_rx_workers", "RX Master (KB/s)", "RX Workers (KB/s)", 1024)

		// Istio latency Gateway→Master (3 quantili)
		cc.IstioReq = promSingleSeries(metrics, "istio_req_total", "Istio req/s", 1)
		cc.IstioP50 = promSingleSeries(metrics, "istio_latency_p50", "Istio P50 (ms)", 1)
		cc.IstioP95 = promSingleSeries(metrics, "istio_latency_p95", "Istio P95 (ms)", 1)
		cc.IstioP99 = promSingleSeries(metrics, "istio_latency_p99", "Istio P99 (ms)", 1)

		// TCP P2P — direzione separata Master↔Worker
		cc.TCPMasterToWorkers = promSingleSeries(metrics, "istio_tcp_master_to_workers", "TCP Master→Workers (KB/s)", 1024)
		cc.TCPWorkersToMaster = promSingleSeries(metrics, "istio_tcp_workers_to_master", "TCP Workers→Master (KB/s)", 1024)

		// Node-level NIC (saturazione fisica)
		cc.NodeNetTX = promSeries(metrics, "node_net_tx_master", "node_net_tx_workers", "Node TX Master (MB/s)", "Node TX Workers (MB/s)", 1<<20)
		cc.NodeNetRX = promSeries(metrics, "node_net_rx_master", "node_net_rx_workers", "Node RX Master (MB/s)", "Node RX Workers (MB/s)", 1<<20)
		cc.NodeNetDrop = promSeries(metrics, "node_net_tx_drop_master", "node_net_drop_workers", "Drop Master (pkt/s)", "Drop Workers (pkt/s)", 1)

		// Node CPU e memoria — multi-series per nodo
		cc.NodeCPU = plotData{
			Name: "Node CPU %",
			Series: func() []plotSeries {
				var s []plotSeries
				for _, key := range []string{"node_cpu_usage_master", "node_cpu_usage_worker1", "node_cpu_usage_worker2"} {
					names := map[string]string{
						"node_cpu_usage_master":  "Master CPU %",
						"node_cpu_usage_worker1": "Worker-01 CPU %",
						"node_cpu_usage_worker2": "Worker-02 CPU %",
					}
					x, y := promExtractTimeSeries(metrics, key, 1)
					if x != nil {
						s = append(s, plotSeries{Name: names[key], X: x, Y: y})
					}
				}
				return s
			}(),
		}
		cc.NodeMem = plotData{
			Name: "Node RAM Available (GB)",
			Series: func() []plotSeries {
				var s []plotSeries
				for _, key := range []string{"node_mem_available_master", "node_mem_available_worker1", "node_mem_available_worker2"} {
					names := map[string]string{
						"node_mem_available_master":  "Master RAM avail (GB)",
						"node_mem_available_worker1": "Worker-01 RAM avail (GB)",
						"node_mem_available_worker2": "Worker-02 RAM avail (GB)",
					}
					x, y := promExtractTimeSeries(metrics, key, 1<<30)
					if x != nil {
						s = append(s, plotSeries{Name: names[key], X: x, Y: y})
					}
				}
				return s
			}(),
		}

		// LocalAI inference metrics
		cc.InferenceTokensPerSec = promSingleSeries(metrics, "localai_tokens_per_sec", "Tokens/s", 1)
		cc.InferenceQueueDepth = promSingleSeries(metrics, "localai_queue_depth", "Queue depth", 1)
		cc.InferenceLatP50 = promSingleSeries(metrics, "localai_inference_duration_p50", "Inference P50 (ms)", 1)
		cc.InferenceLatP99 = promSingleSeries(metrics, "localai_inference_duration_p99", "Inference P99 (ms)", 1)

		charts.PerCluster[ct.ClusterID] = cc

		// Comparison data
		errorRate := 0.0
		if ls.TotalRequests > 0 {
			errorRate = float64(ls.FailedRequests) / float64(ls.TotalRequests) * 100
		}
		label := ct.ClusterID + " (" + ct.Model + ")"
		charts.Comparison.AvgLatency = append(charts.Comparison.AvgLatency, barItem{label, ls.AvgResponseTime})
		charts.Comparison.P95Latency = append(charts.Comparison.P95Latency, barItem{label, ls.P95})
		charts.Comparison.P99Latency = append(charts.Comparison.P99Latency, barItem{label, ls.P99})
		charts.Comparison.RPS = append(charts.Comparison.RPS, barItem{label, ls.RPS})
		charts.Comparison.ErrorRate = append(charts.Comparison.ErrorRate, barItem{label, errorRate})
		charts.Comparison.PeakRAM = append(charts.Comparison.PeakRAM, barItem{label, peakValue(metrics, "ram_master", 1<<20)})
		charts.Comparison.PeakCPU = append(charts.Comparison.PeakCPU, barItem{label, peakValue(metrics, "cpu_master", 1)})
	}

	b, err := json.Marshal(charts)
	return string(b), err
}

// ── Prometheus data extractors ────────────────────────────────────────────────

func promExtractTimeSeries(metrics map[string]interface{}, key string, divisor float64) ([]interface{}, []interface{}) {
	if metrics == nil {
		return nil, nil
	}
	raw, ok := metrics[key]
	if !ok {
		return nil, nil
	}
	b, _ := json.Marshal(raw)
	var results []promResult
	if err := json.Unmarshal(b, &results); err != nil || len(results) == 0 {
		return nil, nil
	}
	xs := make([]interface{}, 0, len(results[0].Values))
	ys := make([]interface{}, 0, len(results[0].Values))
	for _, v := range results[0].Values {
		if len(v) < 2 {
			continue
		}
		// Prometheus timestamp is Unix seconds (float). Convert to ms for Plotly date axis.
		var tsMs float64
		switch t := v[0].(type) {
		case float64:
			tsMs = t * 1000
		case string:
			f, _ := strconv.ParseFloat(t, 64)
			tsMs = f * 1000
		}
		valStr, _ := v[1].(string)
		// Skip NaN/Inf values produced by histogram_quantile with sparse data
		if valStr == "NaN" || valStr == "+Inf" || valStr == "-Inf" || valStr == "" {
			continue
		}
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		xs = append(xs, tsMs)
		ys = append(ys, val/divisor)
	}
	if len(xs) == 0 {
		return nil, nil
	}
	// Sort by timestamp to avoid Plotly drawing backward lines on out-of-order points
	type pt struct {
		x float64
		y interface{}
	}
	pts := make([]pt, len(xs))
	for i := range xs {
		pts[i] = pt{x: xs[i].(float64), y: ys[i]}
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].x < pts[j].x })
	for i := range pts {
		xs[i] = pts[i].x
		ys[i] = pts[i].y
	}
	return xs, ys
}

func promSeries(metrics map[string]interface{}, k1, k2, n1, n2 string, divisor float64) plotData {
	x1, y1 := promExtractTimeSeries(metrics, k1, divisor)
	x2, y2 := promExtractTimeSeries(metrics, k2, divisor)
	pd := plotData{Name: n1}
	if x1 != nil {
		pd.Series = append(pd.Series, plotSeries{Name: n1, X: x1, Y: y1})
	}
	if x2 != nil {
		pd.Series = append(pd.Series, plotSeries{Name: n2, X: x2, Y: y2})
	}
	return pd
}

func promSingleSeries(metrics map[string]interface{}, key, name string, divisor float64) plotData {
	x, y := promExtractTimeSeries(metrics, key, divisor)
	return plotData{Name: name, X: x, Y: y}
}

func peakValue(metrics map[string]interface{}, key string, divisor float64) float64 {
	_, ys := promExtractTimeSeries(metrics, key, divisor)
	peak := 0.0
	for _, v := range ys {
		if f, ok := v.(float64); ok && f > peak {
			peak = f
		}
	}
	return peak
}

// ── HTML Renderer ─────────────────────────────────────────────────────────────

func renderHTML(exp *Experiment, chartsJSON string, start, end time.Time) string {
	clusterIDs := make([]string, len(exp.Clusters))
	for i, c := range exp.Clusters {
		clusterIDs[i] = `"` + c.ClusterID + `"`
	}

	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Experiment Report — %s</title>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #6366f1;
    --green: #22c55e; --red: #ef4444; --amber: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 1.5rem 2rem; }
  header h1 { font-size: 1.5rem; color: var(--accent); }
  header p { color: var(--muted); font-size: .875rem; margin-top: .25rem; }
  nav { display: flex; gap: .5rem; padding: 1rem 2rem; background: var(--surface);
        border-bottom: 1px solid var(--border); overflow-x: auto; }
  nav button { background: var(--border); border: none; color: var(--text); padding: .5rem 1rem;
               border-radius: 6px; cursor: pointer; white-space: nowrap; font-size: .875rem; }
  nav button.active { background: var(--accent); }
  main { max-width: 1400px; margin: 0 auto; padding: 2rem; }
  .section { display: none; }
  .section.active { display: block; }
  h2 { font-size: 1.25rem; margin-bottom: 1.5rem; color: var(--text); padding-bottom: .5rem;
       border-bottom: 1px solid var(--border); }
  h3 { font-size: 1rem; color: var(--muted); margin: 1.5rem 0 .75rem; }
  .summary-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px,1fr)); gap: 1rem; margin-bottom: 2rem; }
  .kpi { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
         padding: 1rem; text-align: center; }
  .kpi .val { font-size: 1.75rem; font-weight: 700; color: var(--accent); }
  .kpi .lbl { font-size: .75rem; color: var(--muted); margin-top: .25rem; }
  .kpi.warn .val { color: var(--red); }
  .kpi.ok .val { color: var(--green); }
  .charts-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(600px,1fr)); gap: 1.5rem; }
  .chart-box { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
               padding: 1rem; }
  .chart-box .chart-title { font-size: .875rem; color: var(--muted); margin-bottom: .5rem; }
  .cluster-tabs { display: flex; gap: .5rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
  .cluster-tab { background: var(--border); border: none; color: var(--text); padding: .4rem .9rem;
                 border-radius: 6px; cursor: pointer; font-size: .875rem; }
  .cluster-tab.active { background: var(--accent); }
  .cluster-panel { display: none; }
  .cluster-panel.active { display: block; }
</style>
</head>
<body>
<header>
  <h1>📊 Experiment Report — %s</h1>
  <p>%s → %s &nbsp;|&nbsp; Clusters: %s</p>
</header>
<nav>
  <button class="active" onclick="showSection('comparison')">🔀 Comparison</button>
  <button onclick="showSection('clusters')">📦 Per Cluster</button>
  <button onclick="showSection('istio')">🌐 Istio / TCP P2P</button>
  <button onclick="showSection('infra')">🖥️ Container Resources</button>
  <button onclick="showSection('node')">⚙️ Node Metrics</button>
  <button onclick="showSection('inference')">🧠 Inference</button>
</nav>
<main>

<!-- ═══════════════════ COMPARISON ═══════════════════ -->
<div id="comparison" class="section active">
  <h2>Cross-Cluster Comparison</h2>
  <div class="charts-grid">
    <div class="chart-box"><div class="chart-title">Average Response Time (ms)</div><div id="cmp_avg_rt" style="height:300px"></div></div>
    <div class="chart-box"><div class="chart-title">P95 Latency (ms)</div><div id="cmp_p95" style="height:300px"></div></div>
    <div class="chart-box"><div class="chart-title">P99 Latency (ms)</div><div id="cmp_p99" style="height:300px"></div></div>
    <div class="chart-box"><div class="chart-title">Throughput (req/s)</div><div id="cmp_rps" style="height:300px"></div></div>
    <div class="chart-box"><div class="chart-title">Error Rate (%%)</div><div id="cmp_err" style="height:300px"></div></div>
    <div class="chart-box"><div class="chart-title">Peak RAM Master (MB)</div><div id="cmp_ram" style="height:300px"></div></div>
    <div class="chart-box"><div class="chart-title">Peak CPU Master (cores)</div><div id="cmp_cpu" style="height:300px"></div></div>
  </div>
</div>

<!-- ═══════════════════ PER CLUSTER ═══════════════════ -->
<div id="clusters" class="section">
  <h2>Per-Cluster Detail</h2>
  <div class="cluster-tabs" id="clusterTabs"></div>
  <div id="clusterPanels"></div>
</div>

<!-- ═══════════════════ ISTIO ═══════════════════ -->
<div id="istio" class="section">
  <h2>Istio / Network Traffic</h2>
  <div class="cluster-tabs" id="istioTabs"></div>
  <div id="istioPanels"></div>
</div>

<!-- ═══════════════════ INFRA ═══════════════════ -->
<div id="infra" class="section">
  <h2>Infrastructure — Container Resources</h2>
  <div class="cluster-tabs" id="infraTabs"></div>
  <div id="infraPanels"></div>
</div>

<!-- ═══════════════════ NODE ═══════════════════ -->
<div id="node" class="section">
  <h2>Node-Level Metrics (Load-Aware + Network-Aware)</h2>
  <div class="cluster-tabs" id="nodeTabs"></div>
  <div id="nodePanels"></div>
</div>

<!-- ═══════════════════ INFERENCE ═══════════════════ -->
<div id="inference" class="section">
  <h2>LocalAI Inference Metrics</h2>
  <div class="cluster-tabs" id="inferenceTabs"></div>
  <div id="inferencePanels"></div>
</div>

</main>
<script>
const CHARTS = %s;
const CLUSTER_IDS = [%s];

const LAYOUT_BASE = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#e2e8f0', size: 11 },
  xaxis: { gridcolor: '#2a2d3a', linecolor: '#2a2d3a' },
  yaxis: { gridcolor: '#2a2d3a', linecolor: '#2a2d3a' },
  legend: { bgcolor: 'rgba(0,0,0,0)' },
  margin: { t: 20, b: 40, l: 50, r: 10 },
};
const COLORS = ['#6366f1','#22c55e','#f59e0b','#ef4444','#06b6d4','#a78bfa','#fb923c'];

function plotBar(divId, items, unit) {
  if (!items || !items.length) return;
  const trace = {
    type: 'bar',
    x: items.map(i => i.label),
    y: items.map(i => i.value),
    marker: { color: COLORS },
    text: items.map(i => i.value.toFixed(2) + ' ' + (unit||'')),
    textposition: 'auto',
  };
  Plotly.newPlot(divId, [trace], {...LAYOUT_BASE}, {responsive:true, displayModeBar:false});
}

function plotLine(divId, pd, unit) {
  if (!pd) return;
  const traces = [];
  const toDates = arr => (arr||[]).map(v => new Date(v));
  if (pd.series && pd.series.length) {
    pd.series.forEach((s,i) => {
      if (!s.x || !s.x.length) return;
      traces.push({type:'scatter', mode:'lines', name: s.name, x: toDates(s.x), y: s.y, line:{color: COLORS[i %% COLORS.length]}});
    });
  } else if (pd.x && pd.x.length) {
    traces.push({type:'scatter', mode:'lines', name: pd.name, x: toDates(pd.x), y: pd.y, line:{color: COLORS[0]}});
  }
  if (!traces.length) { document.getElementById(divId).innerHTML = '<p style="color:#94a3b8;padding:1rem">No data</p>'; return; }
  const layout = {...LAYOUT_BASE,
    xaxis: {...LAYOUT_BASE.xaxis, type: 'date', tickformat: '%%H:%%M:%%S'},
    yaxis: {...LAYOUT_BASE.yaxis, title: unit||''}};
  Plotly.newPlot(divId, traces, layout, {responsive:true, displayModeBar:false});
}

function buildClusterPanel(cid, container, mode) {
  const cc = CHARTS.perCluster[cid];
  if (!cc) return;
  const s = cc.summary;
  const errRate = s.total > 0 ? (s.failed/s.total*100).toFixed(1) : '0.0';
  const warnErr = parseFloat(errRate) > 5 ? 'warn' : 'ok';

  let html = '<div class="summary-grid">' +
    kpi(s.total, 'Total Requests') +
    kpi(s.rps.toFixed(2), 'Req/s') +
    kpi(s.avgRt.toFixed(0), 'Avg RT (ms)') +
    kpi(s.p50.toFixed(0), 'P50 (ms)') +
    kpi(s.p95.toFixed(0), 'P95 (ms)') +
    kpi(s.p99.toFixed(0), 'P99 (ms)') +
    kpiClass(errRate + '%%', 'Error Rate', warnErr) +
    '</div>';

  const p = (mode + '_' + cid).replace(/-/g,'_');

  if (mode === 'locust') {
    const hasHistory = (cc.latency && cc.latency.series && cc.latency.series.length) ||
                       (cc.rps && cc.rps.x && cc.rps.x.length);
    if (hasHistory) {
      html += '<div class="charts-grid">' +
        chartBox(p+'_lat', 'Latency over time (Locust)') +
        chartBox(p+'_rps', 'Throughput (req/s)') +
        chartBox(p+'_users', 'Concurrent Users') +
        '</div>';
      container.innerHTML += html;
      plotLine(p+'_lat', cc.latency, 'ms');
      plotLine(p+'_rps', cc.rps, 'req/s');
      plotLine(p+'_users', cc.users, 'users');
    } else {
      // No locust history (--only-summary): show Istio request rate + latency over time instead
      html += '<div class="charts-grid">' +
        chartBox(p+'_ireq', 'Request rate over time (Istio req/s)') +
        chartBox(p+'_ip50', 'Latency P50 over time (Istio, ms)') +
        chartBox(p+'_ip95', 'Latency P95 over time (Istio, ms)') +
        '</div>';
      container.innerHTML += html;
      plotLine(p+'_ireq', cc.istioReq, 'req/s');
      plotLine(p+'_ip50', cc.istioP50, 'ms');
      plotLine(p+'_ip95', cc.istioP95, 'ms');
    }

  } else if (mode === 'istio') {
    html += '<div class="charts-grid">' +
      chartBox(p+'_req', 'Istio Req/s (Gateway→Master)') +
      chartBox(p+'_p50', 'Istio P50 Latency (ms)') +
      chartBox(p+'_p95', 'Istio P95 Latency (ms)') +
      chartBox(p+'_p99', 'Istio P99 Latency (ms)') +
      chartBox(p+'_tcpm2w', 'TCP P2P Master→Workers (KB/s)') +
      chartBox(p+'_tcpw2m', 'TCP P2P Workers→Master (KB/s)') +
      '</div>';
    container.innerHTML += html;
    plotLine(p+'_req', cc.istioReq, 'req/s');
    plotLine(p+'_p50', cc.istioP50, 'ms');
    plotLine(p+'_p95', cc.istioP95, 'ms');
    plotLine(p+'_p99', cc.istioP99, 'ms');
    plotLine(p+'_tcpm2w', cc.tcpMasterToWorkers, 'KB/s');
    plotLine(p+'_tcpw2m', cc.tcpWorkersToMaster, 'KB/s');

  } else if (mode === 'infra') {
    html += '<div class="charts-grid">' +
      chartBox(p+'_ram', 'Container RAM (MB)') +
      chartBox(p+'_cpu', 'Container CPU (cores)') +
      chartBox(p+'_tx', 'Container Network TX (KB/s)') +
      chartBox(p+'_rx', 'Container Network RX (KB/s)') +
      '</div>';
    container.innerHTML += html;
    plotLine(p+'_ram', cc.ram, 'MB');
    plotLine(p+'_cpu', cc.cpu, 'cores');
    plotLine(p+'_tx', cc.netTx, 'KB/s');
    plotLine(p+'_rx', cc.netRx, 'KB/s');

  } else if (mode === 'node') {
    html += '<div class="charts-grid">' +
      chartBox(p+'_cpu', 'Node CPU %% per nodo') +
      chartBox(p+'_mem', 'Node RAM disponibile (GB)') +
      chartBox(p+'_ntx', 'Node NIC TX (MB/s)') +
      chartBox(p+'_nrx', 'Node NIC RX (MB/s)') +
      chartBox(p+'_drop', 'Node Packet Drop (pkt/s)') +
      '</div>';
    container.innerHTML += html;
    plotLine(p+'_cpu', cc.nodeCpu, '%%');
    plotLine(p+'_mem', cc.nodeMem, 'GB');
    plotLine(p+'_ntx', cc.nodeNetTx, 'MB/s');
    plotLine(p+'_nrx', cc.nodeNetRx, 'MB/s');
    plotLine(p+'_drop', cc.nodeNetDrop, 'pkt/s');

  } else if (mode === 'inference') {
    html += '<div class="charts-grid">' +
      chartBox(p+'_tok', 'Tokens/s generati') +
      chartBox(p+'_q', 'Queue depth inferenza') +
      chartBox(p+'_ip50', 'Inference P50 (ms/token)') +
      chartBox(p+'_ip99', 'Inference P99 (ms/token)') +
      '</div>';
    container.innerHTML += html;
    plotLine(p+'_tok', cc.inferenceTokensPerSec, 'tok/s');
    plotLine(p+'_q', cc.inferenceQueueDepth, '');
    plotLine(p+'_ip50', cc.inferenceLatP50, 'ms');
    plotLine(p+'_ip99', cc.inferenceLatP99, 'ms');
  }
}

function kpi(val, lbl) { return '<div class="kpi"><div class="val">'+val+'</div><div class="lbl">'+lbl+'</div></div>'; }
function kpiClass(val, lbl, cls) { return '<div class="kpi '+cls+'"><div class="val">'+val+'</div><div class="lbl">'+lbl+'</div></div>'; }
function chartBox(id, title) { return '<div class="chart-box"><div class="chart-title">'+title+'</div><div id="'+id+'" style="height:280px"></div></div>'; }

function buildTabPanels(tabContainerId, panelContainerId, mode) {
  const tabs = document.getElementById(tabContainerId);
  const panels = document.getElementById(panelContainerId);
  CLUSTER_IDS.forEach((cid, i) => {
    const btn = document.createElement('button');
    btn.className = 'cluster-tab' + (i===0?' active':'');
    btn.textContent = cid + ' (' + (CHARTS.perCluster[cid]||{model:''}).model + ')';
    btn.onclick = () => {
      tabs.querySelectorAll('.cluster-tab').forEach(b=>b.classList.remove('active'));
      panels.querySelectorAll('.cluster-panel').forEach(p=>p.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById(panelContainerId+'_'+cid).classList.add('active');
    };
    tabs.appendChild(btn);

    const panel = document.createElement('div');
    panel.id = panelContainerId + '_' + cid;
    panel.className = 'cluster-panel' + (i===0?' active':'');
    panels.appendChild(panel);
    buildClusterPanel(cid, panel, mode);
  });
}

// Init
plotBar('cmp_avg_rt', CHARTS.comparison.avgLatency, 'ms');
plotBar('cmp_p95', CHARTS.comparison.p95Latency, 'ms');
plotBar('cmp_p99', CHARTS.comparison.p99Latency, 'ms');
plotBar('cmp_rps', CHARTS.comparison.rps, 'req/s');
plotBar('cmp_err', CHARTS.comparison.errorRate, '%%');
plotBar('cmp_ram', CHARTS.comparison.peakRam, 'MB');
plotBar('cmp_cpu', CHARTS.comparison.peakCpu, 'cores');

buildTabPanels('clusterTabs','clusterPanels', 'locust');
buildTabPanels('istioTabs','istioPanels', 'istio');
buildTabPanels('infraTabs','infraPanels', 'infra');
buildTabPanels('nodeTabs','nodePanels', 'node');
buildTabPanels('inferenceTabs','inferencePanels', 'inference');

function showSection(id) {
  document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  const sections = ['comparison','clusters','istio','infra','node','inference'];
  document.querySelectorAll('nav button').forEach((b,i)=>{
    b.classList.toggle('active', sections[i]===id);
  });
}
</script>
</body>
</html>`,
		exp.Name,
		exp.Name,
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
		strings.Join(func() []string {
			ids := make([]string, len(exp.Clusters))
			for i, c := range exp.Clusters {
				ids[i] = c.ClusterID
			}
			return ids
		}(), ", "),
		chartsJSON,
		strings.Join(func() []string {
			ids := make([]string, len(exp.Clusters))
			for i, c := range exp.Clusters {
				ids[i] = `"` + c.ClusterID + `"`
			}
			return ids
		}(), ","),
	)
}