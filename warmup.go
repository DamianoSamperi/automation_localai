package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"localai-orchestrator/experiment"
)

// ── Warm-up Logic ────────────────────────────────────────────────────────────
//
// A warm-up deploys (or reuses) the given clusters, runs a short load test,
// waits for the MetricsController to populate annotations on the deployments,
// reads those annotations and persists them in clusters.json under the
// SavedAnnotations field. On the next deploy, generateMasterYAML and
// generateWorkerYAML inject these annotations into the manifest so the
// LoadAwareResourcesBalancedAllocation scheduler plugin has data immediately.
//
// Strategy: non-blocking — if a cluster is already running, the warm-up does
// NOT redeploy it, and will NOT undeploy it afterwards either. It just runs
// the load test on the existing pods and snapshots the annotations.
//
// For clusters that were NOT running (warm-up deployed them itself), the
// warm-up undeploys them again once the annotations have been snapshotted —
// leaving the cluster definition (with its new SavedAnnotations) intact for
// a future deploy, but freeing up cluster resources in the meantime.

// WarmupRequest is the body sent by the UI to /api/cluster/warmup
type WarmupRequest struct {
	ClusterIDs      []string `json:"clusterIds"`     // empty = warmup all
	DurationSeconds int      `json:"durationSeconds"` // default 600 (10 min)
	Profile         string   `json:"profile"`         // CSV, default a gentle ramp
}

// handleWarmup runs a warmup for the requested clusters and saves annotations.
func (m *Manager) handleWarmup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req WarmupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if req.DurationSeconds == 0 {
		req.DurationSeconds = 600
	}
	if req.Profile == "" {
		// Default: gentle ramp 1→2→3 users over the duration, then 0
		req.Profile = fmt.Sprintf("second,users\n0,1\n%d,2\n%d,3\n%d,0",
			req.DurationSeconds/3,
			(req.DurationSeconds*2)/3,
			req.DurationSeconds)
	}

	// Resolve target clusters
	m.mu.RLock()
	var targets []ClusterConfig
	if len(req.ClusterIDs) == 0 {
		// Warmup-all
		targets = make([]ClusterConfig, len(m.config.Clusters))
		copy(targets, m.config.Clusters)
	} else {
		idSet := map[string]bool{}
		for _, id := range req.ClusterIDs {
			idSet[id] = true
		}
		for _, c := range m.config.Clusters {
			if idSet[c.ID] {
				targets = append(targets, c)
			}
		}
	}
	m.mu.RUnlock()

	if len(targets) == 0 {
		http.Error(w, "no clusters to warm up", 400)
		return
	}

	go m.runWarmup(targets, req.DurationSeconds, req.Profile)

	w.Header().Set("Content-Type", "application/json")
	ids := make([]string, len(targets))
	for i, c := range targets {
		ids[i] = c.ID
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "started",
		"clusters": ids,
		"duration": req.DurationSeconds,
	})
}

// runWarmup performs the warm-up workflow for the given clusters.
func (m *Manager) runWarmup(targets []ClusterConfig, duration int, profile string) {
	log.Printf("[warmup] starting for %d cluster(s), duration=%ds", len(targets), duration)

	// 1) Deploy clusters that are not running yet (non-blocking: don't touch running ones).
	//    Track which ones THIS warmup run deployed, so we know which to undeploy later.
	deployedByWarmup := map[string]bool{}
	for _, c := range targets {
		status := m.clusterStatus(c.ID)
		if status == "running" {
			log.Printf("[warmup] cluster %s already running, skipping deploy", c.ID)
			continue
		}
		log.Printf("[warmup] deploying cluster %s (status=%s)", c.ID, status)
		m.deployCluster(c.ID) // synchronous — waits for master+gateway
		deployedByWarmup[c.ID] = true
	}

	// 2) Wait a bit so workers and master have time to register with each other
	log.Printf("[warmup] waiting 30s for P2P discovery...")
	time.Sleep(30 * time.Second)

	// 3) Launch a load-test experiment in parallel for all targets
	expClusters := make([]experiment.ClusterTarget, 0, len(targets))
	m.mu.RLock()
	for _, c := range targets {
		// Re-read live cluster to get GatewayPort populated by deploy
		for _, live := range m.config.Clusters {
			if live.ID != c.ID {
				continue
			}
			target := experiment.ClusterTarget{
				ClusterID:   live.ID,
				GatewayPort: live.GatewayPort,
				Model:       live.Model,
			}
			for _, n := range m.config.Nodes {
				if n.Name == live.MasterNode {
					target.GatewayHost = n.Host
					break
				}
			}
			expClusters = append(expClusters, target)
		}
	}
	m.mu.RUnlock()

	expName := fmt.Sprintf("warmup-%d", time.Now().Unix())
	expID, err := m.expManager.Start(expName, profile, expClusters)
	if err != nil {
		log.Printf("[warmup] ERROR starting experiment: %v", err)
		return
	}
	log.Printf("[warmup] experiment %s started", expID)

	// 4) Wait for the warm-up duration + small buffer so MetricsController has
	//    written at least 2 cycles of annotations (it runs every 30s).
	wait := time.Duration(duration+90) * time.Second
	log.Printf("[warmup] sleeping %s while metrics accumulate...", wait)
	time.Sleep(wait)

	// 5) For each cluster, read live annotations and save them in clusters.json
	for _, c := range targets {
		m.snapshotClusterAnnotations(c.ID)
	}

	// 6) Undeploy ONLY the clusters this warmup run deployed itself.
	//    Clusters that were already running before warmup are left untouched.
	for _, c := range targets {
		if !deployedByWarmup[c.ID] {
			continue
		}
		log.Printf("[warmup] undeploying cluster %s (deployed by this warmup run)", c.ID)
		if err := m.undeployCluster(c.ID); err != nil {
			log.Printf("[warmup] ERROR undeploying cluster %s: %v", c.ID, err)
		}
	}

	log.Printf("[warmup] complete for %d cluster(s)", len(targets))
}

// clusterStatus returns the current Status of a cluster (or "" if not found).
func (m *Manager) clusterStatus(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.config.Clusters {
		if c.ID == id {
			return c.Status
		}
	}
	return ""
}

// snapshotClusterAnnotations reads annotations from master + worker deployments
// and saves them under SavedAnnotations in clusters.json.
func (m *Manager) snapshotClusterAnnotations(clusterID string) {
	m.mu.RLock()
	var cluster *ClusterConfig
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == clusterID {
			c := m.config.Clusters[i]
			cluster = &c
			break
		}
	}
	m.mu.RUnlock()
	if cluster == nil {
		log.Printf("[warmup] cluster %s not found for snapshot", clusterID)
		return
	}

	saved := map[string]map[string]string{}

	// Master deployment
	masterDeploy := "local-ai-" + clusterID
	masterAnn := fetchDeploymentAnnotations(masterDeploy)
	if masterAnn != nil {
		// Keep only the keys relevant to the scheduler
		filtered := filterSchedulerAnnotations(masterAnn)
		if len(filtered) > 0 {
			saved["master"] = filtered
		}
	}

	// Worker deployments — iterate over current Workers (those that actually exist)
	for _, w := range cluster.Workers {
		suffix := w.NodeName
		if suffix == "" {
			// Worker without a pinned node — find its deployment by listing
			// deployments with cluster-id label and role=worker.
			continue
		}
		workerDeploy := fmt.Sprintf("localai-worker-%s-%s", clusterID, suffix)
		ann := fetchDeploymentAnnotations(workerDeploy)
		if ann != nil {
			filtered := filterSchedulerAnnotations(ann)
			if len(filtered) > 0 {
				saved[suffix] = filtered
			}
		}
	}

	// Also list any worker deployments not pinned (NodeName=="")
	listOut, err := exec.Command("kubectl", "get", "deployment",
		"-n", "local-ai",
		"-l", fmt.Sprintf("cluster-id=%s,role=worker", clusterID),
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
	).Output()
	if err == nil {
		for _, name := range strings.Split(strings.TrimSpace(string(listOut)), "\n") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			// Skip already-snapshotted (pinned) workers
			prefix := fmt.Sprintf("localai-worker-%s-", clusterID)
			suffix := strings.TrimPrefix(name, prefix)
			if _, already := saved[suffix]; already {
				continue
			}
			ann := fetchDeploymentAnnotations(name)
			if ann != nil {
				filtered := filterSchedulerAnnotations(ann)
				if len(filtered) > 0 {
					saved[suffix] = filtered
				}
			}
		}
	}

	if len(saved) == 0 {
		log.Printf("[warmup] no annotations found for cluster %s — nothing to save", clusterID)
		return
	}

	// Persist
	m.mu.Lock()
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == clusterID {
			m.config.Clusters[i].SavedAnnotations = saved
			break
		}
	}
	m.saveConfig()
	m.mu.Unlock()

	keys := []string{}
	for k := range saved {
		keys = append(keys, k)
	}
	log.Printf("[warmup] saved annotations for cluster %s: %v", clusterID, keys)
}

// fetchDeploymentAnnotations reads .metadata.annotations from a deployment.
func fetchDeploymentAnnotations(deployName string) map[string]string {
	out, err := exec.Command("kubectl", "get", "deployment", deployName,
		"-n", "local-ai",
		"-o", "jsonpath={.metadata.annotations}",
	).Output()
	if err != nil {
		log.Printf("[warmup] kubectl get deployment %s: %v", deployName, err)
		return nil
	}
	raw := strings.TrimSpace(string(out))
	if raw == "" || raw == "{}" {
		return nil
	}
	var ann map[string]string
	if err := json.Unmarshal([]byte(raw), &ann); err != nil {
		log.Printf("[warmup] parse annotations of %s: %v", deployName, err)
		return nil
	}
	return ann
}

// filterSchedulerAnnotations keeps only the annotation keys the scheduler reads.
// (cpu-usage, memory-usage, traffic.*) — drops Kubernetes-managed annotations
// like deployment.kubernetes.io/revision, kubectl.kubernetes.io/last-applied-...
func filterSchedulerAnnotations(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		if k == "cpu-usage" || k == "memory-usage" || strings.HasPrefix(k, "traffic.") {
			out[k] = v
		}
	}
	return out
}

// buildAnnotationBlock renders a YAML annotations block (indented 6 spaces, to
// fit under "template:\n      metadata:\n") for a given saved annotation map.
// Returns empty string if no annotations.
func buildAnnotationBlock(saved map[string]string, baseIndent string) string {
	if len(saved) == 0 {
		return ""
	}
	var sb strings.Builder
	for k, v := range saved {
		sb.WriteString(baseIndent)
		sb.WriteString(k)
		sb.WriteString(": \"")
		sb.WriteString(v)
		sb.WriteString("\"\n")
	}
	return sb.String()
}