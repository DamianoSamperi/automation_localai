# LocalAI Orchestrator

Dashboard Go per deploy, gestione e testing di mini-cluster LocalAI su Kubernetes con Istio Gateway API.

## Struttura progetto

```
localai-orchestrator/
├── main.go                     # Dashboard HTTP server (porta 8090)
├── experiment/
│   ├── runner.go               # Orchestratore esperimenti paralleli + generatore locustfile
│   └── report.go               # Generatore report HTML Plotly
├── sidecar/
│   ├── main.go                 # Sidecar worker (discovery master via Istio Gateway)
│   ├── go.mod
│   └── Dockerfile
├── templates/
│   └── index.html              # UI dashboard
├── experiments/
│   ├── locustfiles/            # Locustfile generati (uno per cluster)
│   └── results/                # JSON Prometheus + report HTML
├── clusters.json               # Stato persistente
├── go.mod
└── Makefile
```

### Risorse Kubernetes per mini-cluster

Ogni mini-cluster genera:

| Risorsa K8s | Nome | Label |
|---|---|---|
| Deployment master | `local-ai-<clusterID>` | `cluster-id=<id>` `app=<id>_master` `role=master` |
| Service ClusterIP | `local-ai-<clusterID>` | `cluster-id=<id>` |
| Deployment worker | `localai-worker-<clusterID>-<nodeName>` | `cluster-id=<id>` `app=<id>_worker_<nodeName>` `role=worker` |
| Gateway (Istio) | `gateway-<clusterID>` | `cluster-id=<id>` |
| HTTPRoute | `route-<clusterID>` | `cluster-id=<id>` |

Le label `app` seguono il formato `<clusterID>_master` / `<clusterID>_worker_<nodeName>` per lo scraping Istio per-cluster in Prometheus.

---

## Prerequisiti

### Macchina locale

```bash
go version          # Go 1.22+
kubectl get nodes   # kubectl configurato sul cluster
docker info         # Docker (per build sidecar)
make check-locust   # locust (per gli esperimenti)
```

### Cluster Kubernetes

- Kubernetes 1.27+
- NFS server su `192.168.1.51` (modifica `createStaticPV()` in `main.go` se diverso)
- Nodi labellati con `kubernetes.io/hostname`

---

## Setup iniziale (una volta sola)

```bash
# 1. Installa Gateway API CRDs
make install-gateway-crds

# 2. Scarica istioctl
curl -L https://istio.io/downloadIstio | sh -
export PATH=$PWD/istio-*/bin:$PATH

# 3. Installa Istio + abilita injection
make setup-cluster

# 4. Build e push sidecar
make push-sidecar

# 5. Installa locust
make install-locust
```

---

## Avvio

```bash
make run
# oppure senza rebuild:
make run-dev
```

Apri: **http://localhost:8090**

---

## Flusso d'uso

### 1. Deploy mini-cluster

1. **Nodes** → aggiungi nodi se non presenti
2. **Clusters** → **+ New Cluster**
   - Scegli master node, aggiungi worker, imposta modello
3. **🚀 Deploy** → log in tempo reale
4. **🔄 GW** → scopre la porta Gateway Istio assegnata

### 2. Avvia esperimento

1. **Experiments** → **▶ New Experiment**
2. Nome + profilo CSV (formato `second,users`)
3. Seleziona i cluster (eseguiti **in parallelo**)
4. **▶ Start** → log live
5. Al termine → **📊 Open Report**

### Formato profilo di carico

```csv
second,users
0,1
30,3
60,5
120,3
180,1
```

---

## Report HTML (Plotly)

Il report include quattro sezioni navigabili:

| Sezione | Contenuto |
|---|---|
| **Comparison** | Bar chart cross-cluster: latency avg/P95/P99, throughput, error rate, RAM, CPU |
| **Per Cluster** | Line chart: latency nel tempo, req/s, utenti concorrenti |
| **Istio / Network** | Req/s Istio, P50/P99 latency Envoy, TX/RX |
| **Infrastructure** | RAM e CPU master + worker nel tempo |

---

## Metriche Prometheus raccolte

Per ogni cluster, usando label `cluster-id` e `app`:

| Nome | Query base |
|---|---|
| `ram_master` / `ram_workers` | `container_memory_usage_bytes` |
| `cpu_master` / `cpu_workers` | `container_cpu_usage_seconds_total` |
| `network_tx/rx_*` | `container_network_transmit/receive_bytes_total` |
| `istio_req_total` | `istio_requests_total` (destination_app) |
| `istio_req_duration_p50/p99` | `istio_request_duration_milliseconds_bucket` |
| `istio_tcp_sent/recv` | `istio_tcp_sent/received_bytes_total` |
| `network_drops` | `container_network_transmit_packets_dropped_total` |
| `gpu_usage_jetson` | `gpu_usage_percentage` (exporter Jetson) |
| `ram_free_jetson` | `ram_usage` (exporter Jetson) |

---

## `clusters.json` — schema

```jsonc
{
  "nextSeqId": 3,           // Contatore globale monotono — non resettare
  "prometheus": "http://192.168.0.200:32090",
  "nodes": [ ... ],
  "clusters": [
    {
      "seqId": 1,           // Auto-assegnato
      "id": "cluster-1",    // Derivato da seqId
      "name": "label umano",
      "masterNode": "jetsonorigin",
      "model": "Llama-3.2-3B-Instruct-GGUF",
      "port": 8080,         // Porta interna LocalAI (non NodePort)
      "storageGb": 8,
      "workerMemory": 0,    // Limit GiB worker, 0 = no limit
      "p2pToken": "...",    // Auto-generato
      "workers": [ ... ],
      "status": "stopped",
      "gatewayPort": 0      // Aggiornato dopo deploy
    }
  ]
}
```

> `nextSeqId` è monotonicamente crescente: anche eliminando un cluster, il successivo prenderà sempre `nextSeqId + 1`.

---

## Debug

```bash
make status                      # Pods, services, gateways
make logs-master  CLUSTER=cluster-1
make logs-workers CLUSTER=cluster-1
make clean-cluster CLUSTER=cluster-1
make clean-all                   # ⚠ Elimina TUTTO in local-ai
make clean-experiments           # Rimuove risultati esperimenti
```

---

## Gateway discovery — come funziona

Il sidecar trova il master con questo flusso:
1. Cerca il `Gateway` con label `cluster-id=<CLUSTER_ID>` (env var iniettata dall'orchestratore)
2. Legge `status.addresses` del Gateway
3. Risolve la NodePort dal Service Istio associato
4. Verifica il P2P token (`/api/p2p/token`) per confermare il match
5. **Fallback:** scansione ClusterIP Service (comportamento originale)

---

## NFS PVC

| PVC | Path NFS |
|---|---|
| `localai-shared-models-pvc` | `/data/nfs/ds-cluster-01/modelli-localai` |
| `localai-backend-arm64-pvc` | `/data/nfs/ds-cluster-01/backends-localai-master` |
| `localai-backend-amd64-pvc` | `/data/nfs/ds-cluster-01/backends-localai-worker` |

Modifica `createStaticPV()` in `main.go` per cambiare server o path.
