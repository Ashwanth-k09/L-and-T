import json, os

STORE_FILE = os.path.expanduser("~/pipeline_agent/data/pipelines.json")

def load():
    if not os.path.exists(STORE_FILE):
        return []
    with open(STORE_FILE) as f:
        return json.load(f)

def save_pipeline(entry):
    pipelines = load()
    # Update if same dag_id exists
    for i, p in enumerate(pipelines):
        if p.get("dag_id") == entry.get("dag_id"):
            pipelines[i] = entry
            _write(pipelines)
            return
    pipelines.insert(0, entry)
    _write(pipelines)

def _write(data):
    os.makedirs(os.path.dirname(STORE_FILE), exist_ok=True)
    with open(STORE_FILE, "w") as f:
        json.dump(data, f, indent=2)
