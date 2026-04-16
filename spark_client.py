from fastmcp import Client
import asyncio
import json
import os

SERVER_URL = "http://10.185.180.230:8000/mcp"
CONNECTIONS_FILE = "spark_connections.json"

# ── LOAD / SAVE CONNECTIONS ───────────────────────────────────
def load_connections():
    if os.path.exists(CONNECTIONS_FILE):
        with open(CONNECTIONS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_connections(connections):
    with open(CONNECTIONS_FILE, "w") as f:
        json.dump(connections, f, indent=2)
    print(f"✅ Connection saved to {CONNECTIONS_FILE}")

# ── SHOW SAVED CONNECTIONS ────────────────────────────────────
def show_connections(connections):
    if not connections:
        print("  (no saved connections)")
        return
    for name, info in connections.items():
        print(f"  [{name}]  Master: {info['master_host']}  Workers: {info['worker_hosts']}")

# ── GET CONNECTION (new or existing) ─────────────────────────
def get_connection():
    connections = load_connections()

    print("\n=== CONNECTION SETUP ===")
    print("1. New connection")
    print("2. Use existing connection")
    conn_choice = input("\nEnter choice: ").strip()

    if conn_choice == "2":
        if not connections:
            print("❌ No saved connections found. Creating new one.")
        else:
            print("\nSaved connections:")
            show_connections(connections)
            name = input("\nEnter connection name to use: ").strip()
            if name in connections:
                c = connections[name]
                print(f"✅ Using connection: {name}")
                return c
            else:
                print("❌ Connection not found. Creating new one.")

    # ── NEW CONNECTION ────────────────────────────────────────
    print("\n--- Master Node ---")
    master_host     = input("Master IP: ").strip()
    master_user     = input("Master Username: ").strip()
    master_password = input("Master Password: ").strip()

    num_workers = int(input("\nHow many worker nodes? ").strip())
    worker_hosts = []
    for i in range(num_workers):
        w = input(f"  Worker {i+1} IP: ").strip()
        worker_hosts.append(w)

    worker_user     = input("Worker Username (same for all): ").strip()
    worker_password = input("Worker Password (same for all): ").strip()

    conn = {
        "master_host":     master_host,
        "master_user":     master_user,
        "master_password": master_password,
        "worker_hosts":    worker_hosts,
        "worker_user":     worker_user,
        "worker_password": worker_password,
    }

    # Save to existing connections
    save_name = input("\nSave this connection as (name): ").strip()
    if save_name:
        connections[save_name] = conn
        save_connections(connections)

    return conn

# ── MAIN ──────────────────────────────────────────────────────
async def main():
    async with Client(SERVER_URL) as client:

        tools = await client.list_tools()
        print("✅ Available tools:", [t.name for t in tools])

        # Get connection (new or existing)
        conn = get_connection()

        master_host     = conn["master_host"]
        master_user     = conn["master_user"]
        master_password = conn["master_password"]
        worker_hosts    = conn["worker_hosts"]
        worker_user     = conn["worker_user"]
        worker_password = conn["worker_password"]

        print(f"\n✅ Connected to cluster — Master: {master_host} | Workers: {worker_hosts}")

        # ── ACTION MENU ───────────────────────────────────────
        print("\n=== SPARK AGENT MENU ===")
        print("1. Install Spark")
        print("2. Verify Spark Cluster")
        print("3. Delete / Stop Spark Cluster")

        choice = input("\nEnter choice: ").strip()

        # ── 1. INSTALL ────────────────────────────────────────
        if choice == "1":
            print("\n[1/3] Checking SSH connections...")

            # Check master
            result = await client.call_tool("check_ssh_connection", {
                "host": master_host, "username": master_user, "password": master_password
            })
            print(f"  Master {master_host}: {result.data}")

            # Check workers
            for w in worker_hosts:
                result = await client.call_tool("check_ssh_connection", {
                    "host": w, "username": worker_user, "password": worker_password
                })
                print(f"  Worker {w}: {result.data}")

            print("\n[2/3] Installing Spark on all nodes...")

            # Install on master
            result = await client.call_tool("install_spark_on_node", {
                "host": master_host, "username": master_user, "password": master_password
            })
            print(f"  Master: {result.data}")

            # Install on workers
            for w in worker_hosts:
                result = await client.call_tool("install_spark_on_node", {
                    "host": w, "username": worker_user, "password": worker_password
                })
                print(f"  Worker {w}: {result.data}")

            print("\n[3/3] Configuring and starting cluster...")

            result = await client.call_tool("configure_spark_cluster", {
                "master_host":     master_host,
                "master_user":     master_user,
                "master_password": master_password,
                "worker_hosts":    worker_hosts,
                "worker_user":     worker_user,
                "worker_password": worker_password,
            })
            print(f"  Configure: {result.data}")

            result = await client.call_tool("start_spark_cluster", {
                "master_host":     master_host,
                "master_user":     master_user,
                "master_password": master_password,
                "worker_hosts":    worker_hosts,
                "worker_user":     worker_user,
                "worker_password": worker_password,
            })
            print(f"  Start: {result.data}")

            print(f"\n✅ Spark cluster is UP!")
            print(f"   Master URL : spark://{master_host}:7077")
            print(f"   Master UI  : http://{master_host}:8080")

        # ── 2. VERIFY ─────────────────────────────────────────
        elif choice == "2":
            result = await client.call_tool("verify_spark_cluster", {
                "master_host":     master_host,
                "master_user":     master_user,
                "master_password": master_password,
                "worker_hosts":    worker_hosts,
                "worker_user":     worker_user,
                "worker_password": worker_password,
            })
            print(result.data)

            result = await client.call_tool("get_spark_cluster_info", {
                "master_host":     master_host,
                "master_user":     master_user,
                "master_password": master_password,
            })
            print(result.data)

        # ── 3. DELETE / STOP ──────────────────────────────────
        elif choice == "3":
            confirm = input("⚠️  Are you sure you want to stop the cluster? (yes/no): ").strip()
            if confirm.lower() == "yes":
                result = await client.call_tool("stop_spark_cluster", {
                    "master_host":     master_host,
                    "master_user":     master_user,
                    "master_password": master_password,
                })
                print(result.data)
                print("✅ Spark cluster stopped.")
            else:
                print("Cancelled.")

        else:
            print("❌ Invalid choice")

asyncio.run(main())
