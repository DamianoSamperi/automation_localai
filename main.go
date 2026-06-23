package main

import (
	"bytes"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"localai-orchestrator/experiment"
)

//go:embed templates/*
var templateFS embed.FS

// ── Data Structures ──────────────────────────────────────────────────────────

type NodeConfig struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	User     string `json:"user"`
	Arch     string `json:"arch"`
	IsOrin   bool   `json:"isOrin"`
	GPUType  string `json:"gpuType"`
	GPUModel string `json:"gpuModel"`
}

type WorkerConfig struct {
	NodeName      string `json:"nodeName"` // set by scheduler, empty = not yet placed
	Replicas      int    `json:"replicas"`
	Arch          string `json:"arch"`
	Mem           int    `json:"mem"`
	ManualBackend bool   `json:"manualBackend"`
}

type ClusterConfig struct {
	// Sequential numeric ID (auto-assigned, never reused)
	SeqID      int            `json:"seqId"`
	ID         string         `json:"id"`   // "cluster-<seqId>"
	Name       string         `json:"name"` // human label
	MasterNode string         `json:"masterNode"`
	Workers    []WorkerConfig `json:"workers"`
	// Desired number of workers — scheduler fills Workers[] based on this
	WorkerCount        int                           `json:"workerCount"`
	P2PToken           string                        `json:"p2pToken"`
	StorageGB          int                           `json:"storageGb"`
	Portbind           int                           `json:"port"`
	WorkerMemory       int                           `json:"workerMemory"`
	Model              string                        `json:"model"`
	Status             string                        `json:"status,omitempty"`
	GatewayHost        string                        `json:"gatewayHost,omitempty"`
	GatewayPort        int                           `json:"gatewayPort,omitempty"`
	SavedAnnotations   map[string]map[string]string  `json:"savedAnnotations,omitempty"`
	UseCustomScheduler bool                          `json:"useCustomScheduler,omitempty"`
	UseGPU             bool                          `json:"useGpu,omitempty"`
}

type AppConfig struct {
	Nodes      []NodeConfig    `json:"nodes"`
	Clusters   []ClusterConfig `json:"clusters"`
	NextSeqID  int             `json:"nextSeqId"`
	Prometheus string          `json:"prometheus"`
}

type Manager struct {
	mu         sync.RWMutex
	config     AppConfig
	cfgFile    string
	logStreams map[string]chan string
	expManager *experiment.Manager
}

// ── Manager Init ─────────────────────────────────────────────────────────────

func NewManager(cfgFile string) *Manager {
	m := &Manager{
		cfgFile:    cfgFile,
		logStreams: make(map[string]chan string),
		config: AppConfig{
			Nodes:      []NodeConfig{},
			Clusters:   []ClusterConfig{},
			NextSeqID:  0,
			Prometheus: "http://192.168.0.200:32090",
		},
	}
	m.loadConfig()
	m.expManager = experiment.NewManager("experiments", m.config.Prometheus)

	mc := NewMetricsController(
		m.config.Prometheus,
		func() []NodeConfig {
			m.mu.RLock()
			defer m.mu.RUnlock()
			nodes := make([]NodeConfig, len(m.config.Nodes))
			copy(nodes, m.config.Nodes)
			return nodes
		},
		func() []ClusterConfig {
			m.mu.RLock()
			defer m.mu.RUnlock()
			clusters := make([]ClusterConfig, len(m.config.Clusters))
			copy(clusters, m.config.Clusters)
			return clusters
		},
	)
	mc.Start()

	return m
}

func (m *Manager) loadConfig() {
	data, err := os.ReadFile(m.cfgFile)
	if err != nil {
		log.Printf("Config not found, starting fresh: %v", err)
		return
	}
	if err := json.Unmarshal(data, &m.config); err != nil {
		log.Printf("Config parse error: %v", err)
	}
	changed := false
	for i := range m.config.Clusters {
		if m.config.Clusters[i].SeqID == 0 && m.config.Clusters[i].ID != "" {
			m.config.NextSeqID++
			m.config.Clusters[i].SeqID = m.config.NextSeqID
			changed = true
		}
		if m.config.Clusters[i].Portbind == 0 || m.config.Clusters[i].Portbind == 8080 {
			m.config.Clusters[i].Portbind = 8080 + m.config.Clusters[i].SeqID
			changed = true
		}
	}
	if changed {
		m.saveConfig()
	}
}

func (m *Manager) saveConfig() error {
	data, err := json.MarshalIndent(m.config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(m.cfgFile, data, 0644)
}

func (m *Manager) handleCancelExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", 400)
		return
	}
	if err := m.expManager.Cancel(id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
}

// ── P2P Token Generation ──────────────────────────────────────────────────────

func generateP2PToken(clusterID string) string {
	randStr := func(n int) string {
		b := make([]byte, n)
		rand.Read(b)
		return base64.RawURLEncoding.EncodeToString(b)[:n]
	}
	room := randStr(30)
	rendezvous := randStr(30)
	mdns := "LocalAI-P2P-" + clusterID
	dhtKey := randStr(43)
	cryptoKey := randStr(43)

	yaml := fmt.Sprintf(`otp:
  dht:
    interval: 360
    key: %s
    length: 43
  crypto:
    interval: 9000
    key: %s
    length: 43
room: %s
rendezvous: %s
mdns: %s
max_message_size: 20971520
`, dhtKey, cryptoKey, room, rendezvous, mdns)

	return base64.StdEncoding.EncodeToString([]byte(yaml))
}

// ── Sequential ID ─────────────────────────────────────────────────────────────

func (m *Manager) nextClusterID() (int, string) {
	m.config.NextSeqID++
	seq := m.config.NextSeqID
	return seq, fmt.Sprintf("cluster-%d", seq)
}

// ── YAML Generation ──────────────────────────────────────────────────────────

func (m *Manager) CreateNamespace() error {
	nsYAML := `apiVersion: v1
kind: Namespace
metadata:
  name: local-ai
  labels:
    istio-injection: enabled`
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(nsYAML)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("namespace error: %s", string(out))
	}
	return nil
}

func (m *Manager) ensureRBAC() error {
	rbacYAML := `apiVersion: v1
kind: ServiceAccount
metadata:
  name: localai-worker-sidecar
  namespace: local-ai
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: localai-worker-sidecar
  namespace: local-ai
rules:
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["list", "get"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "create", "update"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list", "get"]
  - apiGroups: ["gateway.networking.k8s.io"]
    resources: ["gateways", "httproutes"]
    verbs: ["list", "get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: localai-worker-sidecar
  namespace: local-ai
subjects:
  - kind: ServiceAccount
    name: localai-worker-sidecar
    namespace: local-ai
roleRef:
  kind: Role
  name: localai-worker-sidecar
  apiGroup: rbac.authorization.k8s.io`
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(rbacYAML)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("RBAC error: %s", string(out))
	}
	return nil
}

func (m *Manager) ensureEmptyRegistry() error {
	cmYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: localai-worker-registry
  namespace: local-ai
data: {}`
	check := exec.Command("kubectl", "get", "configmap", "localai-worker-registry", "-n", "local-ai")
	if check.Run() == nil {
		return nil
	}
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(cmYAML)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("registry configmap error: %s", string(out))
	}
	return nil
}

// generateMasterYAML produces the Deployment + ClusterIP Service for a master.
// MasterNode can be empty ("Let scheduler decide") — in that case the pod is
// constrained to amd64, non-control-plane nodes, and the GPU/runtimeClass
// block is never set (we can't know in advance which node will be picked).
func (m *Manager) generateMasterYAML(cluster *ClusterConfig, nodes map[string]*NodeConfig) string {
	hostname := ""
	masterArch := "amd64"
	masterGPUType := "none"
	if cluster.MasterNode != "" {
		if master, ok := nodes[cluster.MasterNode]; ok && master != nil {
			hostname = master.Name
			masterArch = master.Arch
			masterGPUType = master.GPUType
		}
	}

	image := "localai/localai:master-aio-cpu"
	masterArchPath := "/backends/amd64"
	if masterArch == "arm64" {
		image = "localai/localai:latest-nvidia-l4t-arm64"
		masterArchPath = "/backends/arm64"
	}

	schedulerLine := ""
	if cluster.UseCustomScheduler {
		schedulerLine = "      schedulerName: scheduler-plugins-scheduler"
	}

	// GPU enabled only if explicitly requested AND the pinned node has NVIDIA.
	// When unpinned, GPU block always stays empty (node unknown ahead of time).
	runtimeLine := ""
	gpuLimits := ""
	if cluster.UseGPU && masterGPUType == "nvidia" {
		runtimeLine = `runtimeClassName: "nvidia"`
		gpuLimits = `nvidia.com/gpu.shared: 1`
	}

	gpuLimitsBlock := ""
	if gpuLimits != "" {
		gpuLimitsBlock = fmt.Sprintf(`          resources:
            limits:
              %s`, gpuLimits)
	}

	var nodeSelectorBlock string
	if hostname != "" {
		nodeSelectorBlock = fmt.Sprintf("      nodeSelector:\n        kubernetes.io/hostname: %s", hostname)
	} else {
		nodeSelectorBlock = `      nodeSelector:
        kubernetes.io/arch: amd64
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: node-role.kubernetes.io/control-plane
                    operator: DoesNotExist
                  - key: node-role
                    operator: NotIn
                    values:
                      - management`
	}

	modelEnv := ""
	if cluster.Model != "" {
		modelEnv = fmt.Sprintf(`            - name: MODELS
              value: "%s"`, cluster.Model)
	}

	masterSavedAnn := ""
	if cluster.SavedAnnotations != nil {
		masterSavedAnn = buildAnnotationBlock(cluster.SavedAnnotations["master"], "        ")
	}

	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: local-ai-%s
  namespace: local-ai
  labels:
    app: %s_master
    cluster-id: "%s"
    role: master
spec:
  selector:
    matchLabels:
      app: %s_master
  replicas: 1
  template:
    metadata:
      labels:
        app: %s_master
        cluster-id: "%s"
        role: master
      annotations:
%s        prometheus.io/scrape: "true"
        prometheus.io/port: "%d"
        prometheus.io/path: "/metrics"
        localai-backends-path: "%s"
    spec:
%s
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      %s
%s
      containers:
        - name: local-ai
          image: %s
          imagePullPolicy: IfNotPresent
          args:
            - run
            - --p2p
            - "--address"
            - ":%d"
            - --p2ptoken
            - "%s"
          env:
            - name: DEBUG
              value: "true"
            - name: LOCALAI_BACKENDS_PATH
              valueFrom:
                fieldRef:
                  fieldPath: metadata.annotations['localai-backends-path']
%s
%s
          volumeMounts:
            - name: local-models
              mountPath: /models
            - name: local-backends
              mountPath: /backends
      volumes:
        - name: local-models
          persistentVolumeClaim:
            claimName: localai-shared-models-pvc
        - name: local-backends
          persistentVolumeClaim:
            claimName: localai-backend-all-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: local-ai-%s
  namespace: local-ai
  labels:
    app: %s_master
    cluster-id: "%s"
spec:
  selector:
    app: %s_master
  type: ClusterIP
  ports:
    - name: http
      protocol: TCP
      port: 8080
      targetPort: %d
`,
		cluster.ID,
		cluster.ID, cluster.ID,
		cluster.ID,
		cluster.ID, cluster.ID,
		masterSavedAnn,
		cluster.Portbind,
		masterArchPath,
		schedulerLine,
		runtimeLine,
		nodeSelectorBlock,
		image,
		cluster.Portbind,
		cluster.P2PToken,
		modelEnv,
		gpuLimitsBlock,
		cluster.ID,
		cluster.ID, cluster.ID,
		cluster.ID,
		cluster.Portbind,
	)
}

// generateGatewayYAML produces the Istio Gateway + HTTPRoute for one master.
func (m *Manager) generateGatewayYAML(cluster *ClusterConfig) string {
	gatewayPort := 8000 + cluster.SeqID
	return fmt.Sprintf(`apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: gateway-%s
  namespace: local-ai
  labels:
    cluster-id: "%s"
    app: %s_gateway
spec:
  gatewayClassName: istio
  listeners:
    - name: http
      protocol: HTTP
      port: %d
      allowedRoutes:
        namespaces:
          from: Same
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: route-%s
  namespace: local-ai
  labels:
    cluster-id: "%s"
spec:
  parentRefs:
    - name: gateway-%s
      namespace: local-ai
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: local-ai-%s
          port: 8080
`,
		cluster.ID, cluster.ID, cluster.ID,
		gatewayPort,
		cluster.ID, cluster.ID,
		cluster.ID,
		cluster.ID,
	)
}

// generateWorkerYAML produces the Deployment for one worker replica set.
func (m *Manager) generateWorkerYAML(cluster *ClusterConfig, worker WorkerConfig, port int) string {
	image := "localai/localai:master-cpu"
	if worker.Arch == "arm64" {
		image = "localai/localai:latest-nvidia-l4t-arm64"
	}
	sidecarImage := "dami00/localai-worker-sidecar:latest"

	nodeSelector := fmt.Sprintf(`        kubernetes.io/arch: %s`, worker.Arch)
	if worker.NodeName != "" {
		nodeSelector = fmt.Sprintf(`        kubernetes.io/hostname: %s`, worker.NodeName)
	} else if worker.Arch == "" {
		nodeSelector = `        {}`
	}

	memLimit := ""
	if cluster.WorkerMemory > 0 {
		memLimit = fmt.Sprintf(`      resources:
            limits:
              memory: "%dGi"`, cluster.WorkerMemory)
	}

	schedulerLine := ""
	if cluster.UseCustomScheduler {
		schedulerLine = "      schedulerName: scheduler-plugins-scheduler"
	}

	workerArchPath := "/backends/amd64"
	if worker.Arch == "arm64" {
		workerArchPath = "/backends/arm64"
	}

	workerSuffix := worker.NodeName
	if workerSuffix == "" {
		workerSuffix = fmt.Sprintf("worker-%d", port)
	}
	workerLabel := fmt.Sprintf("%s_worker_%s", cluster.ID, workerSuffix)

	var containerSpec string
	if worker.ManualBackend {
		containerSpec = fmt.Sprintf(`
        - name: worker
          image: %s
          command:
            - /bin/sh
            - -c
            - |
              /backends/llama-cpp-amd/lib/ld.so \
                --library-path /backends/llama-cpp-amd/lib \
                /backends/llama-cpp-amd/llama-cpp-rpc-server --host 127.0.0.1 --port %d &
              exec /build/aio/entrypoint.sh worker p2p-llama-cpp-rpc
          env:
            - name: DEBUG
              value: "true"
            - name: TOKEN
              value: "%s"
            - name: MODELS
              value: "[]"
            - name: LOCALAI_LOG_LEVEL
              value: "debug"
            - name: LOCALAI_BACKENDS_PATH
              value: "/backends"
            - name: BACKENDS_PATH
              value: "/backends"
            - name: LOCALAI_EXTERNAL_BACKENDS
              value: "llama-cpp:/backends/llama-cpp-amd/run.sh"
            - name: LOCALAI_BACKEND_ASSETS_PATH
              value: "/backends/assets"
            - name: BACKEND_ASSETS_PATH
              value: "/backends/assets"
            - name: LOCALAI_BACKEND_GALLERIES
              value: '[{"name":"custom","url":"oci://dami00/localai-backend-llama-cpp-amd:latest"}]'
            - name: NO_RUNNER
              value: "true"
            - name: LOCALAI_RUNNER_ADDRESS
              value: "127.0.0.1"
            - name: LOCALAI_RUNNER_PORT
              value: "%d"
          volumeMounts:
            - name: lxcfs-proc-meminfo
              mountPath: /proc/meminfo
              readOnly: true
            - name: local-backends
              mountPath: /backends`,
			image, port, cluster.P2PToken, port)
	} else {
		containerSpec = fmt.Sprintf(`
        - name: worker
          image: %s
          imagePullPolicy: IfNotPresent
          args:
            - p2p-worker
            - p2p-llama-cpp-rpc
          env:
            - name: DEBUG
              value: "true"
            - name: TOKEN
              value: "%s"
            - name: MODELS
              value: "[]"
            - name: LOCALAI_LOG_LEVEL
              value: "debug"
            - name: LOCALAI_BACKENDS_PATH
              valueFrom:
                fieldRef:
                  fieldPath: metadata.annotations['localai-backends-path']
          volumeMounts:
            - name: lxcfs-proc-meminfo
              mountPath: /proc/meminfo
              readOnly: true
            - name: local-backends
              mountPath: /backends`,
			image, cluster.P2PToken)
	}

	volumesBlock := `
      volumes:
        - name: lxcfs-proc-meminfo
          hostPath:
            path: /var/lib/lxcfs/proc/meminfo
        - name: local-backends
          persistentVolumeClaim:
            claimName: localai-backend-all-pvc`

	workerSavedAnn := ""
	if cluster.SavedAnnotations != nil {
		workerSavedAnn = buildAnnotationBlock(cluster.SavedAnnotations[workerSuffix], "        ")
	}

	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: localai-worker-%s-%s
  namespace: local-ai
  labels:
    app: %s
    cluster-id: "%s"
    role: worker
spec:
  replicas: %d
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
        cluster-id: "%s"
        role: worker
      annotations:
%s        prometheus.io/scrape: "true"
        prometheus.io/port: "2112"
        localai-backends-path: "%s"
    spec:
%s
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      serviceAccountName: localai-worker-sidecar
      nodeSelector:
%s
      containers:%s
        - name: worker-watcher
          image: %s
          imagePullPolicy: Always
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: TOKEN
              value: "%s"
            - name: CLUSTER_ID
              value: "%s"
%s
%s
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: node-role.kubernetes.io/control-plane
                    operator: DoesNotExist
                  - key: node-role
                    operator: NotIn
                    values:
                      - management
`,
		cluster.ID, workerSuffix,
		workerLabel, cluster.ID,
		worker.Replicas,
		workerLabel,
		workerLabel, cluster.ID,
		workerSavedAnn,
		workerArchPath,
		schedulerLine,
		nodeSelector,
		containerSpec,
		sidecarImage,
		cluster.P2PToken,
		cluster.ID,
		volumesBlock,
		memLimit,
	)
}

// ── PV/PVC helpers ──────────────────────────────────────────────────────────

func (m *Manager) createStaticPV(name, path string, storage int) error {
	pvYAML := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolume
metadata:
  name: %s
spec:
  capacity:
    storage: %dGi
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  nfs:
    server: 192.168.1.51
    path: %s
`, name, storage, path)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pvYAML)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("PV %s error: %s", name, string(out))
	}
	return nil
}

func (m *Manager) ensureNFSPVC(pvcName, targetPV string, storage int) error {
	pvcYAML := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: local-ai
spec:
  accessModes: [ReadWriteMany]
  volumeName: %s
  storageClassName: ""
  resources: { requests: { storage: %dGi } }
`, pvcName, targetPV, storage)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pvcYAML)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("PVC %s error: %s", pvcName, string(out))
	}
	return nil
}

func (m *Manager) EnsureSharedPVs() {
	pvMap := map[string]string{
		"pv-models":     "/data/nfs/ds-cluster-01/modelli-localai",
		"pv-backend-arm": "/data/nfs/ds-cluster-01/backends-localai-master",
		"pv-backend-amd": "/data/nfs/ds-cluster-01/backends-localai-worker",
		"pv-backend-all": "/data/nfs/ds-cluster-01/backends-localai-all",
	}
	for name, path := range pvMap {
		check := exec.Command("kubectl", "get", "pv", name)
		if check.Run() == nil {
			continue
		}
		storage := 5
		if name == "pv-models" || name == "pv-backend-all" {
			storage = 10
		}
		if err := m.createStaticPV(name, path, storage); err != nil {
			log.Printf("WARN: PV %s: %v", name, err)
		} else {
			log.Printf("PV %s created", name)
		}
	}
}

// ── Gateway Port Discovery ────────────────────────────────────────────────────

func discoverGatewayPort(clusterID string) (int, error) {
	out, err := exec.Command("kubectl", "get", "service",
		"-n", "local-ai",
		"-l", fmt.Sprintf("cluster-id=%s", clusterID),
		"-o", "jsonpath={.items[*].spec.ports[0].nodePort}",
	).Output()
	if err != nil {
		return 0, err
	}
	port := 0
	fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &port)
	if port == 0 {
		return 0, fmt.Errorf("gateway port not yet assigned for %s", clusterID)
	}
	return port, nil
}

// ── Deployment Logic ─────────────────────────────────────────────────────────

func (m *Manager) deployCluster(clusterID string) {
	m.mu.Lock()
	var cluster *ClusterConfig
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == clusterID {
			cluster = &m.config.Clusters[i]
			break
		}
	}
	if cluster == nil {
		m.mu.Unlock()
		return
	}
	m.saveConfig()
	m.mu.Unlock()

	logCh := make(chan string, 200)
	m.mu.Lock()
	m.logStreams[clusterID] = logCh
	m.mu.Unlock()

	addLog := func(msg string) {
		select {
		case logCh <- msg:
		default:
		}
		log.Printf("[%s] %s", clusterID, msg)
	}

	defer func() {
		time.Sleep(15 * time.Second)
		port, err := discoverGatewayPort(clusterID)
		if err == nil {
			m.mu.Lock()
			for i := range m.config.Clusters {
				if m.config.Clusters[i].ID == clusterID {
					m.config.Clusters[i].GatewayPort = port
					break
				}
			}
			m.saveConfig()
			m.mu.Unlock()
			addLog(fmt.Sprintf("Gateway port discovered: %d", port))
		} else {
			addLog(fmt.Sprintf("WARN: could not discover gateway port yet: %v", err))
		}
		close(logCh)
	}()

	m.setClusterStatus(clusterID, "deploying")

	if err := m.CreateNamespace(); err != nil {
		addLog("WARN namespace: " + err.Error())
	} else {
		addLog("Namespace local-ai ready (istio-injection enabled)")
	}

	if err := m.ensureRBAC(); err != nil {
		addLog("WARN RBAC: " + err.Error())
	} else {
		addLog("RBAC ready")
	}

	if err := m.ensureEmptyRegistry(); err != nil {
		addLog("WARN registry: " + err.Error())
	} else {
		addLog("Worker registry ConfigMap ready")
	}

	addLog("Ensuring shared PVCs...")
	m.ensureNFSPVC("localai-shared-models-pvc", "pv-models", 10)
	m.ensureNFSPVC("localai-backend-arm64-pvc", "pv-backend-arm", 5)
	m.ensureNFSPVC("localai-backend-amd64-pvc", "pv-backend-amd", 5)
	m.ensureNFSPVC("localai-backend-all-pvc", "pv-backend-all", 10)

	nodeMap := make(map[string]*NodeConfig)
	m.mu.RLock()
	for i := range m.config.Nodes {
		nodeMap[m.config.Nodes[i].Name] = &m.config.Nodes[i]
	}
	clusterIdx := 0
	for i, c := range m.config.Clusters {
		if c.ID == clusterID {
			clusterIdx = i
			break
		}
	}
	clusterCopy := *cluster
	m.mu.RUnlock()

	basePort := 19000 + clusterIdx*100

	masterYAML := m.generateMasterYAML(&clusterCopy, nodeMap)
	masterFile := filepath.Join(os.TempDir(), fmt.Sprintf("master-%s.yaml", clusterID))
	if err := os.WriteFile(masterFile, []byte(masterYAML), 0644); err != nil {
		addLog("ERROR master YAML write: " + err.Error())
		m.setClusterStatus(clusterID, "error")
		return
	}
	if err := m.kubectlApply(masterFile, addLog); err != nil {
		addLog("ERROR kubectl apply master: " + err.Error())
		m.setClusterStatus(clusterID, "error")
		return
	}
	addLog("Master deployed")

	gatewayYAML := m.generateGatewayYAML(&clusterCopy)
	gatewayFile := filepath.Join(os.TempDir(), fmt.Sprintf("gateway-%s.yaml", clusterID))
	if err := os.WriteFile(gatewayFile, []byte(gatewayYAML), 0644); err != nil {
		addLog("ERROR gateway YAML write: " + err.Error())
	} else if err := m.kubectlApply(gatewayFile, addLog); err != nil {
		addLog("ERROR kubectl apply gateway: " + err.Error())
	} else {
		addLog(fmt.Sprintf("Gateway + HTTPRoute deployed for %s", clusterID))
	}

	// Workers — deploya sempre WorkerCount worker.
	// Se NodeName è vuoto, il kube-scheduler (default o custom) decide il nodo.
	workerTotal := clusterCopy.WorkerCount
	for i := 0; i < workerTotal; i++ {
		var worker WorkerConfig
		if i < len(clusterCopy.Workers) {
			worker = clusterCopy.Workers[i]
		} else {
			worker = WorkerConfig{
				NodeName: "",
				Replicas: 1,
			}
		}
		port := basePort + i
		workerYAML := m.generateWorkerYAML(&clusterCopy, worker, port)
		suffix := worker.NodeName
		if suffix == "" {
			suffix = fmt.Sprintf("worker-%d", i)
		}
		workerFile := filepath.Join(os.TempDir(), fmt.Sprintf("worker-%s-%s.yaml", clusterID, suffix))
		if err := os.WriteFile(workerFile, []byte(workerYAML), 0644); err != nil {
			addLog(fmt.Sprintf("ERROR worker %d YAML: %v", i, err))
			continue
		}
		if err := m.kubectlApply(workerFile, addLog); err != nil {
			addLog(fmt.Sprintf("ERROR worker %d apply: %v", i, err))
			continue
		}
		if worker.NodeName != "" {
			addLog(fmt.Sprintf("Worker %s deployed (pinned node)", worker.NodeName))
		} else {
			addLog(fmt.Sprintf("Worker %d deployed (node chosen by scheduler)", i))
		}
	}

	addLog("Deployment complete — waiting for gateway port assignment...")
	m.setClusterStatus(clusterID, "running")
}

func (m *Manager) undeployCluster(clusterID string) error {
	args := []string{
		"delete", "deployment,service,gateway,httproute,pvc",
		"-n", "local-ai",
		"-l", fmt.Sprintf("cluster-id=%s", clusterID),
		"--ignore-not-found=true",
	}
	cmd := exec.Command("kubectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("undeploy error: %s", string(out))
	}
	cleanWorkerRegistry()
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == clusterID {
			m.config.Clusters[i].Status = "stopped"
			m.config.Clusters[i].GatewayPort = 0
			break
		}
	}
	return m.saveConfig()
}

func (m *Manager) deleteCluster(clusterID string) error {
	if err := m.undeployCluster(clusterID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, c := range m.config.Clusters {
		if c.ID == clusterID {
			m.config.Clusters = append(m.config.Clusters[:i], m.config.Clusters[i+1:]...)
			break
		}
	}
	return m.saveConfig()
}

func cleanWorkerRegistry() {
	exec.Command("kubectl", "delete", "configmap", "localai-worker-registry",
		"-n", "local-ai", "--ignore-not-found=true").Run()
}

func (m *Manager) kubectlApply(file string, logFn func(string)) error {
	cmd := exec.Command("kubectl", "apply", "-f", file)
	out, err := cmd.CombinedOutput()
	logFn("kubectl: " + strings.TrimSpace(string(out)))
	return err
}

func (m *Manager) setClusterStatus(id, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == id {
			m.config.Clusters[i].Status = status
			break
		}
	}
}

func (m *Manager) generateYAMLPreview(clusterID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var cluster *ClusterConfig
	clusterIdx := 0

	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == clusterID {
			cluster = &m.config.Clusters[i]
			clusterIdx = i
			break
		}
	}
	if cluster == nil {
		return ""
	}
	nodeMap := make(map[string]*NodeConfig)
	for i := range m.config.Nodes {
		nodeMap[m.config.Nodes[i].Name] = &m.config.Nodes[i]
	}
	basePort := 19000 + clusterIdx*100
	var buf strings.Builder
	buf.WriteString("# === MASTER ===\n")
	buf.WriteString(m.generateMasterYAML(cluster, nodeMap))
	buf.WriteString("\n# === GATEWAY ===\n")
	buf.WriteString(m.generateGatewayYAML(cluster))
	workerTotal := cluster.WorkerCount
	if workerTotal == 0 {
		workerTotal = len(cluster.Workers)
	}
	for i := 0; i < workerTotal; i++ {
		var w WorkerConfig
		if i < len(cluster.Workers) {
			w = cluster.Workers[i]
		} else {
			w = WorkerConfig{Replicas: 1}
		}
		suffix := w.NodeName
		if suffix == "" {
			suffix = fmt.Sprintf("worker-%d", basePort+i)
		}
		buf.WriteString(fmt.Sprintf("\n# === WORKER %d: %s ===\n", i, suffix))
		buf.WriteString(m.generateWorkerYAML(cluster, w, basePort+i))
	}
	return buf.String()
}

// ── HTTP Handlers ─────────────────────────────────────────────────────────────

func (m *Manager) handleGetState(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.config)
}

func (m *Manager) handleSaveNodes(w http.ResponseWriter, r *http.Request) {
	var nodes []NodeConfig
	if err := json.NewDecoder(r.Body).Decode(&nodes); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	m.mu.Lock()
	m.config.Nodes = nodes
	m.saveConfig()
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (m *Manager) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	var cluster ClusterConfig
	if err := json.NewDecoder(r.Body).Decode(&cluster); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	m.mu.Lock()
	seq, id := m.nextClusterID()
	cluster.SeqID = seq
	cluster.ID = id
	cluster.P2PToken = generateP2PToken(id)
	cluster.Portbind = 8080 + seq
	cluster.Status = "created"
	m.config.Clusters = append(m.config.Clusters, cluster)
	m.saveConfig()
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cluster)
}

func (m *Manager) handleUpdateCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var updated ClusterConfig
	if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if updated.ID == "" {
		http.Error(w, "missing id", 400)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, c := range m.config.Clusters {
		if c.ID == updated.ID {
			updated.Status = m.config.Clusters[i].Status
			updated.GatewayPort = m.config.Clusters[i].GatewayPort
			updated.GatewayHost = m.config.Clusters[i].GatewayHost
			updated.SeqID = m.config.Clusters[i].SeqID
			if updated.P2PToken == "" {
				updated.P2PToken = m.config.Clusters[i].P2PToken
			}
			if updated.Portbind == 0 {
				updated.Portbind = m.config.Clusters[i].Portbind
			}
			if updated.Portbind == 0 {
				updated.Portbind = 8080 + updated.SeqID
			}
			if updated.SavedAnnotations == nil {
				updated.SavedAnnotations = m.config.Clusters[i].SavedAnnotations
			}
			m.config.Clusters[i] = updated
			m.saveConfig()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
	}
	http.Error(w, "cluster not found", 404)
}

func (m *Manager) handleScaleWorkers(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", 400)
		return
	}
	targetStr := r.URL.Query().Get("workers")
	target := 0
	fmt.Sscanf(targetStr, "%d", &target)
	if target < 0 {
		http.Error(w, "workers must be >= 0", 400)
		return
	}

	m.mu.Lock()
	var cluster *ClusterConfig
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == id {
			cluster = &m.config.Clusters[i]
			break
		}
	}
	if cluster == nil {
		m.mu.Unlock()
		http.Error(w, "cluster not found", 404)
		return
	}

	current := len(cluster.Workers)
	clusterCopy := *cluster
	m.mu.Unlock()

	log.Printf("[%s] scaling workers %d → %d", id, current, target)

	if target > current {
		basePort := 19000 + clusterCopy.SeqID*100
		for i := current; i < target; i++ {
			newWorker := WorkerConfig{
				NodeName: "",
				Replicas: 1,
			}
			port := basePort + i
			workerYAML := m.generateWorkerYAML(&clusterCopy, newWorker, port)
			workerFile := filepath.Join(os.TempDir(), fmt.Sprintf("worker-%s-scale-%d.yaml", id, i))
			if err := os.WriteFile(workerFile, []byte(workerYAML), 0644); err != nil {
				http.Error(w, "yaml write error: "+err.Error(), 500)
				return
			}
			cmd := exec.Command("kubectl", "apply", "-f", workerFile)
			out, err := cmd.CombinedOutput()
			if err != nil {
				http.Error(w, fmt.Sprintf("kubectl error: %s", string(out)), 500)
				return
			}
			log.Printf("[%s] scale-up: deployed worker %d", id, i)

			m.mu.Lock()
			for j := range m.config.Clusters {
				if m.config.Clusters[j].ID == id {
					m.config.Clusters[j].Workers = append(m.config.Clusters[j].Workers, newWorker)
					m.config.Clusters[j].WorkerCount = target
					break
				}
			}
			m.saveConfig()
			m.mu.Unlock()
		}
	} else if target < current {
		m.mu.Lock()
		for j := range m.config.Clusters {
			if m.config.Clusters[j].ID != id {
				continue
			}
			for i := current - 1; i >= target; i-- {
				w := m.config.Clusters[j].Workers[i]
				suffix := w.NodeName
				if suffix == "" {
					basePort := 19000 + clusterCopy.SeqID*100
					suffix = fmt.Sprintf("worker-%d", basePort+i)
				}
				deployName := fmt.Sprintf("localai-worker-%s-%s", id, suffix)
				exec.Command("kubectl", "delete", "deployment", deployName,
					"-n", "local-ai", "--ignore-not-found=true").Run()
				log.Printf("[%s] scale-down: removed worker %d (%s)", id, i, deployName)
			}
			m.config.Clusters[j].Workers = m.config.Clusters[j].Workers[:target]
			m.config.Clusters[j].WorkerCount = target
		}
		m.saveConfig()
		m.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"cluster": id,
		"workers": target,
	})
}

func (m *Manager) handleGetClusterWorkers(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.config.Clusters {
		if c.ID == id {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"workers":     c.Workers,
				"workerCount": c.WorkerCount,
			})
			return
		}
	}
	http.Error(w, "not found", 404)
}

func (m *Manager) handleDeployCluster(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", 400)
		return
	}
	go m.deployCluster(id)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deploying"})
}

func (m *Manager) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", 400)
		return
	}
	if err := m.deleteCluster(id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (m *Manager) handleUndeployCluster(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if err := m.undeployCluster(id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "undeployed"})
}

func (m *Manager) handleYAMLPreview(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	yaml := m.generateYAMLPreview(id)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, yaml)
}

func (m *Manager) handleSSELogs(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	m.mu.RLock()
	ch := m.logStreams[id]
	m.mu.RUnlock()

	if ch == nil {
		fmt.Fprintf(w, "data: no active stream\n\n")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	for msg := range ch {
		fmt.Fprintf(w, "data: %s\n\n", msg)
		flusher.Flush()
	}
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// ── Experiment Handlers ───────────────────────────────────────────────────────

type ExperimentRequest struct {
	Name     string                     `json:"name"`
	Profile  string                     `json:"profile"`
	Clusters []experiment.ClusterTarget `json:"clusters"`
}

func (m *Manager) handleStartExperiment(w http.ResponseWriter, r *http.Request) {
	var req ExperimentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	m.mu.RLock()
	for i := range req.Clusters {
		for _, c := range m.config.Clusters {
			if c.ID == req.Clusters[i].ClusterID {
				req.Clusters[i].GatewayPort = c.GatewayPort
				req.Clusters[i].Model = c.Model
				if req.Clusters[i].Model == "" {
					req.Clusters[i].Model = "Llama-3.2-3B-Instruct-GGUF"
				}
				for _, n := range m.config.Nodes {
					if n.Name == c.MasterNode {
						req.Clusters[i].GatewayHost = n.Host
						break
					}
				}
				// Master unpinned ("let scheduler decide") — resolve host via
				// the actually-running master pod's node.
				if req.Clusters[i].GatewayHost == "" {
					out, _ := exec.Command("kubectl", "get", "pod",
						"-n", "local-ai",
						"-l", fmt.Sprintf("cluster-id=%s,role=master", c.ID),
						"-o", "jsonpath={.items[0].spec.nodeName}").Output()
					podNode := strings.TrimSpace(string(out))
					if podNode != "" {
						for _, n := range m.config.Nodes {
							if n.Name == podNode {
								req.Clusters[i].GatewayHost = n.Host
								break
							}
						}
					}
				}
			}
		}
	}
	m.mu.RUnlock()

	expID, err := m.expManager.Start(req.Name, req.Profile, req.Clusters)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"experimentId": expID})
}

func (m *Manager) handleListExperiments(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.expManager.List())
}

func (m *Manager) handleExperimentStatus(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	exp := m.expManager.Get(id)
	if exp == nil {
		http.Error(w, "not found", 404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(exp)
}

func (m *Manager) handleExperimentSSE(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	ch := m.expManager.Subscribe(id)
	if ch == nil {
		fmt.Fprintf(w, "data: experiment not found\n\n")
		return
	}
	for msg := range ch {
		fmt.Fprintf(w, "data: %s\n\n", msg)
		flusher.Flush()
	}
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

func (m *Manager) handleGetReport(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	path := m.expManager.ReportPath(id)
	if path == "" {
		http.Error(w, "report not ready", 404)
		return
	}
	http.ServeFile(w, r, path)
}

func (m *Manager) handleGetNodes(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.config.Nodes)
}

func (m *Manager) nodeIP(nodeName string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, n := range m.config.Nodes {
		if n.Name == nodeName {
			return n.Host
		}
	}
	return ""
}

func (m *Manager) handleRefreshGateway(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	port, err := discoverGatewayPort(id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	m.mu.Lock()
	for i := range m.config.Clusters {
		if m.config.Clusters[i].ID == id {
			m.config.Clusters[i].GatewayPort = port
			break
		}
	}
	m.saveConfig()
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"gatewayPort": port})
}

func (m *Manager) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmplData, err := templateFS.ReadFile("templates/index.html")
	if err != nil {
		http.Error(w, "template not found", 500)
		return
	}
	tmpl, err := template.New("index").Parse(string(tmplData))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var buf bytes.Buffer
	tmpl.Execute(&buf, nil)
	w.Header().Set("Content-Type", "text/html")
	w.Write(buf.Bytes())
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	cfgFile := "clusters.json"
	if len(os.Args) > 1 {
		cfgFile = os.Args[1]
	}

	manager := NewManager(cfgFile)
	manager.EnsureSharedPVs()

	mux := http.NewServeMux()

	mux.HandleFunc("/", manager.handleIndex)

	mux.HandleFunc("/api/state", manager.handleGetState)
	mux.HandleFunc("/api/nodes", manager.handleGetNodes)
	mux.HandleFunc("/api/nodes/save", manager.handleSaveNodes)
	mux.HandleFunc("/api/cluster/warmup", manager.handleWarmup)

	mux.HandleFunc("/api/cluster/create", manager.handleCreateCluster)
	mux.HandleFunc("/api/cluster/update", manager.handleUpdateCluster)
	mux.HandleFunc("/api/cluster/deploy", manager.handleDeployCluster)
	mux.HandleFunc("/api/cluster/undeploy", manager.handleUndeployCluster)
	mux.HandleFunc("/api/cluster/delete", manager.handleDeleteCluster)
	mux.HandleFunc("/api/cluster/scale", manager.handleScaleWorkers)
	mux.HandleFunc("/api/cluster/workers", manager.handleGetClusterWorkers)
	mux.HandleFunc("/api/cluster/yaml", manager.handleYAMLPreview)
	mux.HandleFunc("/api/cluster/stream", manager.handleSSELogs)
	mux.HandleFunc("/api/cluster/gateway/refresh", manager.handleRefreshGateway)

	mux.HandleFunc("/api/experiment/start", manager.handleStartExperiment)
	mux.HandleFunc("/api/experiment/list", manager.handleListExperiments)
	mux.HandleFunc("/api/experiment/status", manager.handleExperimentStatus)
	mux.HandleFunc("/api/experiment/stream", manager.handleExperimentSSE)
	mux.HandleFunc("/api/experiment/report", manager.handleGetReport)
	mux.HandleFunc("/api/experiment/cancel", manager.handleCancelExperiment)

	port := ":8090"
	fmt.Printf("LocalAI Orchestrator running → http://localhost%s\n", port)
	log.Fatal(http.ListenAndServe(port, mux))
}