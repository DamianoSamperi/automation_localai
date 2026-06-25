package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	// Gateway API types
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
)

const (
	namespace          = "local-ai"
	configMapName      = "localai-worker-registry"
	httpTimeout        = 10 * time.Second
	pollInterval       = 3 * time.Second
	offlineWaitTimeout = 60 * time.Second
	onlineWaitTimeout  = 120 * time.Second
)

// ── API types ─────────────────────────────────────────────────────────────────

type ModelList struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}
type Model struct {
	ID string `json:"id"`
}
type P2PResponse struct {
	Nodes []P2PNode `json:"nodes"`
}
type P2PNode struct {
	ID       string    `json:"id"`
	IsOnline bool      `json:"isOnline"`
	LastSeen time.Time `json:"lastSeen"`
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	nodeName := os.Getenv("NODE_NAME")
	podName := os.Getenv("POD_NAME")
	token := os.Getenv("TOKEN")
	clusterID := os.Getenv("CLUSTER_ID") // injected by orchestrator

	if nodeName == "" || token == "" || podName == "" {
		log.Fatal("[sidecar] NODE_NAME, POD_NAME and TOKEN env vars are required")
	}
	log.Printf("[sidecar] Starting on node=%s pod=%s clusterID=%s", nodeName, podName, clusterID)

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("[sidecar] in-cluster config: %v", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[sidecar] k8s client: %v", err)
	}
	gwClient, err := gatewayclient.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("[sidecar] gateway client: %v", err)
	}

	// Discover master URL via Istio Gateway (retried until ready)
	var masterURL string
	for {
		masterURL, err = discoverMasterViaGateway(k8s, gwClient, clusterID, token)
		if err == nil {
			break
		}
		// Fallback: try ClusterIP service discovery
		masterURL, err = discoverMasterViaService(k8s, token)
		if err == nil {
			break
		}
		log.Printf("[sidecar] Master not ready yet (%v), retrying in %s...", err, pollInterval)
		time.Sleep(pollInterval)
	}
	log.Printf("[sidecar] Master URL: %s", masterURL)

	client := &http.Client{Timeout: httpTimeout}

	baselineOnline, err := getOnlineNodes(client, masterURL)
	if err != nil {
		log.Printf("[sidecar] WARNING baseline swarm: %v", err)
		baselineOnline = map[string]struct{}{}
	}
	log.Printf("[sidecar] Baseline swarm online: %v", setKeys(baselineOnline))

	deploymentPrefix := getDeploymentPrefix(podName)
	log.Printf("[sidecar] My deployment prefix: %s", deploymentPrefix)

	prevRegistry, _ := loadRegistry(k8s)
	log.Printf("[sidecar] Previous registry: %v", prevRegistry)

	allRunningPods, err := getAllRunningWorkerPods(k8s)
	if err != nil {
		log.Printf("[sidecar] WARNING k8s pods: %v", err)
		allRunningPods = map[string]string{}
	}
	allRunningPods[podName] = nodeName
	log.Printf("[sidecar] All running worker pods (including self): %v", allRunningPods)

	triggerUnload := false
	reason := ""

	if len(allRunningPods) > len(prevRegistry) && len(prevRegistry) > 0 {
		triggerUnload = true
		reason = fmt.Sprintf("Scale-up detected (Pods: %d -> %d)", len(prevRegistry), len(allRunningPods))
	}

	droppedNodes := map[string]struct{}{}
	for regPodName, regNodeName := range prevRegistry {
		if _, stillRunning := allRunningPods[regPodName]; !stillRunning {
			if getDeploymentPrefix(regPodName) == deploymentPrefix && regPodName != podName {
				triggerUnload = true
				reason = fmt.Sprintf("Pod replacement/drop detected for %s", regPodName)
				droppedNodes[regNodeName] = struct{}{}
			}
		}
	}

	if len(prevRegistry) == 0 {
		triggerUnload = true
		reason = "Fresh start (Empty registry)"
	}

	if !triggerUnload {
		log.Printf("[sidecar] No changes requiring model unload. Updating registry and sleeping.")
		saveRegistry(k8s, allRunningPods)
		for {
			time.Sleep(1 * time.Hour)
		}
	}

	log.Printf("[sidecar] TRIGGERING UNLOAD. Reason: %s", reason)

	log.Printf("[sidecar] Waiting for self online (timeout=%s)...", onlineWaitTimeout)
	if !waitForSelfOnline(client, masterURL, baselineOnline, onlineWaitTimeout) {
		log.Printf("[sidecar] WARNING: timed out waiting for self online, proceeding anyway")
	} else {
		log.Printf("[sidecar] Self is online in swarm")
	}

	if len(droppedNodes) > 0 {
		log.Printf("[sidecar] Waiting for dropped nodes offline: %v", setKeys(droppedNodes))
		waitForDroppedOffline(client, masterURL, droppedNodes, offlineWaitTimeout)
	}

	log.Printf("[sidecar] Unloading all models on %s", masterURL)
	if err := unloadAllModels(client, masterURL); err != nil {
		log.Printf("[sidecar] WARNING unload: %v", err)
	}

	saveRegistry(k8s, allRunningPods)
	log.Printf("[sidecar] Done. Sleeping forever (keeps pod Ready).")
	for {
		time.Sleep(1 * time.Hour)
	}
}

// ── Gateway-based master discovery ───────────────────────────────────────────
// discore MAster Via Gateaway , finds the gateaway for this cluster, resolve its address
// The Gateway is labelled group=<clusterID> by the orchestrator.
func discoverMasterViaGateway(k8s *kubernetes.Clientset, gwc *gatewayclient.Clientset, clusterID, myToken string) (string, error) {
	ctx := context.Background()

	// Find the Gateway object for this cluster
	gateways, err := gwc.GatewayV1().Gateways(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("group=%s", clusterID),
	})
	if err != nil || len(gateways.Items) == 0 {
		return "", fmt.Errorf("no gateway found for group=%s", clusterID)
	}
	gw := gateways.Items[0]

	// Istio creates a Service named after the Gateway in the same namespace
	// with format: <gatewayName>-<listenerName> or simply <gatewayName>
	// The status.addresses field has the assigned IP/hostname once ready.
	if len(gw.Status.Addresses) == 0 {
		return "", fmt.Errorf("gateway %s has no addresses yet", gw.Name)
	}
	addr := gw.Status.Addresses[0].Value

	// Find the port from the gateway listener
	if len(gw.Spec.Listeners) == 0 {
		return "", fmt.Errorf("gateway %s has no listeners", gw.Name)
	}
	port := int(gw.Spec.Listeners[0].Port)

	// The gateway address from status may be a hostname or ClusterIP.
	// From inside the cluster we can also look at the Service NodePort
	// if the gateway service type is NodePort (Istio default in many setups).
	gwSvcPort, err := resolveGatewayNodePort(k8s, gw.Name)
	if err == nil && gwSvcPort > 0 {
		// Prefer NodePort via the cluster node IP for reachability from pods
		port = gwSvcPort
	}

	url := fmt.Sprintf("http://%s:%d", addr, port)

	// Verify token matches
	client := &http.Client{Timeout: httpTimeout}
	tok, err := fetchToken(client, url)
	if err != nil {
		return "", fmt.Errorf("gateway reachability check failed: %v", err)
	}
	if tok != myToken {
		return "", fmt.Errorf("token mismatch on gateway %s", url)
	}
	return url, nil
}

// resolveGatewayNodePort finds the NodePort assigned to the Istio-managed
// Service for the Gateway (name pattern: <gatewayName>).
func resolveGatewayNodePort(k8s *kubernetes.Clientset, gatewayName string) (int, error) {
	ctx := context.Background()
	svc, err := k8s.CoreV1().Services(namespace).Get(ctx, gatewayName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	for _, p := range svc.Spec.Ports {
		if p.NodePort > 0 {
			return int(p.NodePort), nil
		}
	}
	return 0, fmt.Errorf("no NodePort on gateway service %s", gatewayName)
}

// discoverMasterViaService is the fallback: scan ClusterIP services in namespace
// and match by P2P token (same as original sidecar logic).
func discoverMasterViaService(k8s *kubernetes.Clientset, myToken string) (string, error) {
	ctx := context.Background()
	services, err := k8s.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list services: %w", err)
	}
	client := &http.Client{Timeout: httpTimeout}
	for _, svc := range services.Items {
		if !strings.HasPrefix(svc.Name, "local-ai-") {
			continue
		}
		if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" || len(svc.Spec.Ports) == 0 {
			continue
		}
		url := fmt.Sprintf("http://%s:%d", svc.Spec.ClusterIP, svc.Spec.Ports[0].Port)
		log.Printf("[sidecar] Trying ClusterIP: %s (svc=%s)", url, svc.Name)
		tok, err := fetchToken(client, url)
		if err != nil {
			continue
		}
		if tok == myToken {
			log.Printf("[sidecar] Token match via ClusterIP")
			return url, nil
		}
	}
	return "", fmt.Errorf("no matching master found via service scan")
}

// ── Registry helpers ──────────────────────────────────────────────────────────

func loadRegistry(k8s *kubernetes.Clientset) (map[string]string, error) {
	ctx := context.Background()
	cm, err := k8s.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return map[string]string{}, err
	}
	result := map[string]string{}
	for k, v := range cm.Data {
		result[k] = v
	}
	return result, nil
}

func saveRegistry(k8s *kubernetes.Clientset, pods map[string]string) {
	ctx := context.Background()
	existing, err := k8s.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err == nil {
		existing.Data = pods
		_, err = k8s.CoreV1().ConfigMaps(namespace).Update(ctx, existing, metav1.UpdateOptions{})
	} else if apierrors.IsNotFound(err) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: namespace},
			Data:       pods,
		}
		_, err = k8s.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			existing, _ = k8s.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
			existing.Data = pods
			_, err = k8s.CoreV1().ConfigMaps(namespace).Update(ctx, existing, metav1.UpdateOptions{})
		}
	}
	if err != nil {
		log.Printf("[sidecar] WARNING: registry save failed: %v", err)
	} else {
		log.Printf("[sidecar] Registry saved: %v", pods)
	}
}

// ── K8s helpers ───────────────────────────────────────────────────────────────

func getDeploymentPrefix(podName string) string {
	parts := strings.Split(podName, "-")
	if len(parts) < 3 {
		return podName
	}
	return strings.Join(parts[:len(parts)-2], "-")
}

func getAllRunningWorkerPods(k8s *kubernetes.Clientset) (map[string]string, error) {
	ctx := context.Background()
	pods, err := k8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	result := map[string]string{}
	for _, pod := range pods.Items {
		if !strings.HasPrefix(pod.Name, "localai-worker-") {
			continue
		}
		if pod.DeletionTimestamp != nil {
			log.Printf("[sidecar] Pod %s is Terminating, skipping", pod.Name)
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		result[pod.Name] = pod.Spec.NodeName
	}
	return result, nil
}

// ── Swarm helpers ─────────────────────────────────────────────────────────────

func getOnlineNodes(client *http.Client, masterURL string) (map[string]struct{}, error) {
	resp, err := client.Get(masterURL + "/api/p2p/workers")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var p2p P2PResponse
	if err := json.NewDecoder(resp.Body).Decode(&p2p); err != nil {
		return nil, err
	}
	online := map[string]struct{}{}
	for _, node := range p2p.Nodes {
		if node.IsOnline {
			online[node.ID] = struct{}{}
		}
	}
	return online, nil
}

func waitForSelfOnline(client *http.Client, masterURL string, baseline map[string]struct{}, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current, err := getOnlineNodes(client, masterURL)
		if err != nil {
			time.Sleep(pollInterval)
			continue
		}
		for id := range current {
			if _, inBaseline := baseline[id]; !inBaseline {
				log.Printf("[sidecar] New node online in swarm: %s", id)
				return true
			}
		}
		time.Sleep(pollInterval)
	}
	return false
}

func waitForDroppedOffline(client *http.Client, masterURL string, droppedK8sNodes map[string]struct{}, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(masterURL + "/api/p2p/workers")
		if err != nil {
			time.Sleep(pollInterval)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var p2p P2PResponse
		if err := json.Unmarshal(body, &p2p); err != nil {
			time.Sleep(pollInterval)
			continue
		}
		stillOnline := []string{}
		for _, node := range p2p.Nodes {
			if !node.IsOnline {
				continue
			}
			for dropped := range droppedK8sNodes {
				if strings.HasPrefix(node.ID, dropped+"-") || node.ID == dropped {
					stillOnline = append(stillOnline, node.ID)
				}
			}
		}
		if len(stillOnline) == 0 {
			log.Printf("[sidecar] All dropped nodes offline")
			return
		}
		log.Printf("[sidecar] Still online (waiting offline): %v", stillOnline)
		time.Sleep(pollInterval)
	}
	log.Printf("[sidecar] Timeout waiting dropped offline — proceeding anyway")
}

// ── Model unload ──────────────────────────────────────────────────────────────

func unloadAllModels(client *http.Client, masterURL string) error {
	resp, err := client.Get(masterURL + "/v1/models")
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}
	defer resp.Body.Close()
	var ml ModelList
	if err := json.NewDecoder(resp.Body).Decode(&ml); err != nil {
		return fmt.Errorf("decode models: %w", err)
	}
	if len(ml.Data) == 0 {
		log.Printf("[sidecar] No models to unload")
		return nil
	}
	for _, model := range ml.Data {
		body := fmt.Sprintf(`{"model": "%s"}`, model.ID)
		req, _ := http.NewRequest("POST", masterURL+"/backend/shutdown", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r, err := client.Do(req)
		if err != nil {
			log.Printf("[sidecar] Unload %s: %v", model.ID, err)
			continue
		}
		rb, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode >= 400 {
			log.Printf("[sidecar] Unload %s: HTTP %d: %s", model.ID, r.StatusCode, string(rb))
			if strings.Contains(string(rb), "no such process") {
				log.Printf("[sidecar] INFO: Backend for %s already missing, master will re-align.", model.ID)
			}
		} else {
			log.Printf("[sidecar] Unload %s: OK", model.ID)
		}
	}
	return nil
}

// ── Token fetch ───────────────────────────────────────────────────────────────

func fetchToken(client *http.Client, baseURL string) (string, error) {
	resp, err := client.Get(baseURL + "/api/p2p/token")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)), nil
}

// ── Utils ─────────────────────────────────────────────────────────────────────

func setKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Suppress unused import warning for gatewayv1 (used via gwc above)
var _ = gatewayv1.Gateway{}
