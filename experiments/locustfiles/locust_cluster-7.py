# Auto-generated locustfile for cluster: cluster-7
# Model: qwen3.5-27b
import csv, io, os, time
from locust import HttpUser, task, between, LoadTestShape, events

MODEL = os.getenv("MODEL_NAME", "qwen3.5-27b")
CLUSTER_ID = os.getenv("CLUSTER_ID", "cluster-7")

PROFILE_CSV = """second,users
0,1
600,2
1200,3
2400,0"""

profile = []
try:
    reader = csv.DictReader(io.StringIO(PROFILE_CSV))
    for row in reader:
        profile.append({"second": int(row["second"]), "users": int(row["users"])})
    print(f"[{CLUSTER_ID}] Profile loaded: {len(profile)} entries, last={profile[-1] if profile else 'none'}", flush=True)
except Exception as e:
    print(f"Profile parse error: {e}", flush=True)

# Event hooks to log every request as it completes
@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    status = "FAIL" if exception else "OK"
    print(f"[{CLUSTER_ID}] REQ {status} {request_type} {name} rt={response_time:.0f}ms len={response_length}", flush=True)

@events.test_start.add_listener
def on_start(**kwargs):
    print(f"[{CLUSTER_ID}] TEST START at {time.time()}", flush=True)

@events.test_stop.add_listener
def on_stop(**kwargs):
    print(f"[{CLUSTER_ID}] TEST STOP at {time.time()}", flush=True)


class LLMUser(HttpUser):
    wait_time = between(1, 15)
    long_context = "Questo è un test di contesto. " * 10 + "/no_think"

    @task
    def ask_llm(self):
        print(f"[{CLUSTER_ID}] sending request...", flush=True)
        payload = {
            "model": MODEL,
            "messages": [{"role": "user", "content": self.long_context}],
            "temperature": 0.7,
            "max_tokens": 600
        }
        with self.client.post(
            "/v1/chat/completions",
            json=payload,
            timeout=3600,
            catch_response=True,
            name=f"/v1/chat/completions [{CLUSTER_ID}]"
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"HTTP {response.status_code}: {response.text[:200]}")


class CSVStrategy(LoadTestShape):
    def tick(self):
        if not profile:
            print("WARNING: profile is empty, stopping", flush=True)
            return None
        run_time = round(self.get_run_time())
        current_users = 0
        for entry in profile:
            if entry["second"] <= run_time:
                current_users = entry["users"]
            else:
                break
        # Terminate as soon as we hit profile end (last entry users=0)
        if run_time >= profile[-1]["second"]:
            return None
        return (current_users, 1)
