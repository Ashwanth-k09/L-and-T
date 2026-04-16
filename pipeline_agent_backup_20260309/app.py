import streamlit as st
import pandas as pd
import os, sys, subprocess, json
from datetime import datetime
sys.path.insert(0, os.path.expanduser("~/pipeline_agent"))

st.set_page_config(page_title="Pipeline Agent", page_icon="⚡", layout="wide")
st.markdown("""<style>
html,body,.stApp{background:#070b14!important;color:#c9d1d9;font-family:'Inter',sans-serif;}
[data-testid="stSidebar"]{background:#0d1117!important;border-right:1px solid #21262d;}
.agent-box{background:#0d1117;border:1px solid #3fb95055;border-radius:12px 12px 12px 2px;padding:14px 18px;margin:10px 0;font-size:14px;color:#e6edf3;}
.agent-lbl{color:#3fb950;font-weight:700;margin-bottom:6px;font-size:12px;}
.ok-box{background:#0d2818;border:1px solid #3fb95055;border-radius:8px;padding:12px 16px;color:#3fb950;font-size:13px;margin:8px 0;}
.err-box{background:#2d0d0d;border:1px solid #f8514955;border-radius:8px;padding:12px 16px;color:#f85149;font-size:14px;margin:8px 0;}
.warn-box{background:#2d1f00;border:1px solid #d2992255;border-radius:8px;padding:12px 16px;color:#d29922;font-size:13px;margin:8px 0;}
.info-box{background:#0c1a2e;border:1px solid #58a6ff55;border-radius:8px;padding:12px 16px;color:#58a6ff;font-size:13px;margin:8px 0;}
.path-box{background:#0a1628;border:1px solid #1f3a5f;border-radius:8px;padding:10px 14px;font-family:monospace;font-size:12px;color:#3fb950;word-break:break-all;margin:6px 0;}
.mysql-card{background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:12px 16px;margin:4px 0;}
.step-done{background:#0d1117;border:1px solid #3fb950;border-radius:10px;padding:12px 16px;margin:6px 0;color:#3fb950;}
.step-run{background:#0d1117;border:1px solid #58a6ff;border-radius:10px;padding:12px 16px;margin:6px 0;color:#58a6ff;}
.step-wait{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:12px 16px;margin:6px 0;color:#484f58;}
.pipe-card{background:#0d1117;border:1px solid #21262d;border-radius:12px;padding:16px 20px;margin:8px 0;}
.pipe-card:hover{border-color:#3fb95055;}
.chat-user{background:#0c1a2e;border:1px solid #1f3a5f;border-radius:12px 12px 2px 12px;padding:10px 14px;margin:8px 0;margin-left:20%;font-size:13px;}
.chat-bot{background:#0d1117;border:1px solid #21262d;border-radius:12px 12px 12px 2px;padding:10px 14px;margin:8px 0;font-size:13px;}
.chat-sql{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px 14px;font-family:monospace;font-size:12px;color:#79c0ff;margin:4px 0;}
.chat-result{background:#0a1628;border:1px solid #1f3a5f;border-radius:8px;padding:10px 14px;font-family:monospace;font-size:11px;color:#e6edf3;margin:4px 0;white-space:pre;overflow-x:auto;}
.run-success{background:#0d2818;border:1px solid #3fb950;border-radius:8px;padding:12px 16px;color:#3fb950;margin:8px 0;}
.run-fail{background:#2d0d0d;border:1px solid #f85149;border-radius:8px;padding:12px 16px;color:#f85149;margin:8px 0;}
.run-running{background:#0c1a2e;border:1px solid #58a6ff;border-radius:8px;padding:12px 16px;color:#58a6ff;margin:8px 0;}
.stButton>button{background:linear-gradient(135deg,#1f6feb,#388bfd);color:white;border:none;border-radius:8px;padding:8px 20px;font-weight:600;font-size:13px;width:100%;}
#MainMenu,footer,header{visibility:hidden;}
</style>""", unsafe_allow_html=True)

AIRFLOW_BIN = "/home/vboxuser/airflow_venv/bin/airflow"

defaults = {
    "step": "choose",
    "source": None,
    "selected_table": None,
    "upload_path": None,
    "table_name": None,
    "dag_name": None,
    "warehouse": None,
    "full_table": None,
    "hdfs_table_path": None,
    "row_count": None,
    "dag_file": None,
    "dag_code": None,
    "dag_id": None,
    "spark": None,
    "schema": [],
    "chat_history": [],
    "active_warehouse": None,
    "dag_run_status": None,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

def get_spark(warehouse=None):
    from src.data_chat import create_spark
    wh = (warehouse or st.session_state.warehouse or
          "hdfs://localhost:8020/data/iceberg")
    if not st.session_state.spark:
        st.session_state.spark = create_spark(wh)
    elif warehouse and warehouse != st.session_state.active_warehouse:
        st.session_state.spark.conf.set(
            "spark.sql.catalog.hadoop_prod.warehouse", wh.rstrip("/"))
    st.session_state.active_warehouse = wh
    return st.session_state.spark

def reset():
    for k, v in defaults.items():
        st.session_state[k] = v

def trigger_dag(dag_id):
    """Clear queued runs then trigger fresh"""
    import time, uuid

    # Unpause
    subprocess.run([AIRFLOW_BIN, "dags", "unpause", dag_id],
                   capture_output=True, timeout=15)

    # Clear any queued/running/failed states
    subprocess.run(
        [AIRFLOW_BIN, "dags", "clear", dag_id, "--yes"],
        capture_output=True, timeout=20)
    time.sleep(2)

    # Poll until registered (max 60s)
    registered = False
    for i in range(20):
        r = subprocess.run(
            [AIRFLOW_BIN, "dags", "list"],
            capture_output=True, text=True, timeout=20)
        for line in r.stdout.split("\n"):
            col = line.split("|")[0].strip()
            if col == dag_id:
                registered = True
                break
        if registered:
            break
        time.sleep(3)

    if not registered:
        return False, f"DAG '{dag_id}' not registered. Wait 30s and retry."

    # Trigger with unique run_id
    run_id = f"manual__{datetime.now().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    r = subprocess.run(
        [AIRFLOW_BIN, "dags", "trigger", dag_id, "--run-id", run_id],
        capture_output=True, text=True, timeout=30)
    if r.returncode == 0:
        return True, f"Triggered! Run ID: {run_id}"
    err = [l.strip() for l in r.stderr.strip().split("\n")
           if l.strip() and "Warning" not in l
           and "graphviz" not in l and "INFO" not in l]
    return False, err[-1] if err else "Trigger failed"

def get_dag_status(dag_id):
    """Get latest DAG run status"""
    try:
        r = subprocess.run(
            [AIRFLOW_BIN, "dags", "list-runs", "--dag-id", dag_id],
            capture_output=True, text=True, timeout=30)  # increased to 30s
        output = r.stdout + r.stderr

        state   = "unknown"
        run_id  = ""
        start_d = ""
        end_d   = ""

        for line in output.split("\n"):
            line_lower = line.lower()
            for s in ["success","failed","running","queued","up_for_retry"]:
                if s in line_lower and dag_id in line:
                    state = s
                    parts = [p.strip() for p in line.split("|")]
                    parts = [p for p in parts if p]
                    if len(parts) >= 2:
                        run_id  = parts[1] if len(parts) > 1 else ""
                        start_d = parts[3] if len(parts) > 3 else ""
                        end_d   = parts[4] if len(parts) > 4 else ""
                    break
            if state != "unknown":
                break

        if state == "unknown":
            for s in ["success","failed","running","queued"]:
                if s in output.lower():
                    state = s
                    break

        return {"state": state, "run_id": run_id,
                "start_date": start_d, "end_date": end_d}
    except subprocess.TimeoutExpired:
        return {"state": "checking...", "run_id": "Status check timed out — DAG may still be running",
                "start_date": "", "end_date": ""}
    except Exception as e:
        return {"state": "error", "run_id": str(e),
                "start_date": "", "end_date": ""}

def save_pipeline_record(entry):
    from src.pipeline_store import save_pipeline
    save_pipeline(entry)

def load_pipelines():
    from src.pipeline_store import load
    return load()

# ── Sidebar ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚡ Pipeline Agent")
    st.markdown("---")
    try:
        import mysql.connector
        c = mysql.connector.connect(host="localhost",
            user="debian-sys-maint", password="1zMY11BxPZTX9zM8")
        c.close(); st.markdown("🟢 MySQL")
    except: st.markdown("🔴 MySQL")
    try:
        r = subprocess.run(["/usr/local/hadoop/bin/hdfs","dfs","-ls","/"],
                           capture_output=True, timeout=5)
        st.markdown("🟢 HDFS" if r.returncode==0 else "🔴 HDFS")
    except: st.markdown("🟡 HDFS")
    st.markdown("🟢 Groq" if os.environ.get("GROQ_API_KEY") else "🔴 Groq Key Missing")
    st.markdown("---")
    if st.session_state.full_table:
        st.markdown(f"**Active:** `{st.session_state.table_name}`")
        st.markdown(f"**Rows:** {st.session_state.row_count:,}")
    st.markdown("---")
    if st.button("🔄 Start Over"): reset(); st.rerun()

st.markdown("## ⚡ Autonomous Data Pipeline Agent")
tab_agent, tab_saved, tab_chat = st.tabs(["🤖 AI Agent", "🗄️ Saved Pipelines", "💬 Chat with Data"])

# ══════════════════════════════════════════════════════════
# TAB 1 — AI AGENT
# ══════════════════════════════════════════════════════════
with tab_agent:

    # STEP: choose
    if st.session_state.step == "choose":
        st.markdown("""<div class="agent-box"><div class="agent-lbl">🤖 AI Agent</div>
        Hello! I will help you store data in HDFS Iceberg format and create an Airflow DAG.<br><br>
        How do you want to provide your data?</div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🗄️ Push MySQL Table", use_container_width=True):
                st.session_state.source = "mysql"
                st.session_state.step   = "mysql_tables"
                st.rerun()
        with c2:
            if st.button("📁 Upload a File", use_container_width=True):
                st.session_state.source = "file"
                st.session_state.step   = "file_upload"
                st.rerun()

    # STEP: mysql tables
    elif st.session_state.step == "mysql_tables":
        st.markdown("""<div class="agent-box"><div class="agent-lbl">🤖 AI Agent</div>
        Here are all tables in your MySQL. Click <b>Select</b> to choose one.</div>""",
        unsafe_allow_html=True)
        from src.db_connector import list_all_tables
        all_tbls = list_all_tables()
        if "error" in all_tbls:
            st.markdown(f'<div class="err-box">❌ {all_tbls["error"]}</div>',
                        unsafe_allow_html=True)
        else:
            for db, tbls in all_tbls.items():
                st.markdown(f"""<div class="mysql-card">
                <span style="color:#bc8cff;font-weight:700;font-family:monospace">🗄️ {db}</span>
                </div>""", unsafe_allow_html=True)
                for tbl in tbls:
                    c1, c2 = st.columns([5, 1])
                    with c1:
                        st.markdown(f"""<div style="background:#161b22;border:1px solid #21262d;
                        border-radius:6px;padding:8px 14px;margin:3px 0;
                        font-family:monospace;font-size:13px;color:#58a6ff">📋 {tbl}</div>""",
                        unsafe_allow_html=True)
                    with c2:
                        if st.button("Select", key=f"s_{db}_{tbl}",
                                     use_container_width=True):
                            from src.db_connector import search_table
                            info = search_table(tbl)
                            if info["found"]:
                                st.session_state.selected_table = info
                                st.session_state.table_name     = tbl
                                st.session_state.step = "ask_hdfs_path"
                                st.rerun()
        if st.button("← Back"):
            st.session_state.step = "choose"; st.rerun()

    # STEP: file upload
    elif st.session_state.step == "file_upload":
        st.markdown("""<div class="agent-box"><div class="agent-lbl">🤖 AI Agent</div>
        Upload your file and give the table a name.</div>""", unsafe_allow_html=True)
        uploaded = st.file_uploader("File", type=["csv","xlsx","xls","json","parquet"],
                                    label_visibility="collapsed")
        tname = st.text_input("Table name", placeholder="e.g. sales_data")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("← Back", use_container_width=True):
                st.session_state.step = "choose"; st.rerun()
        with c2:
            if st.button("Next →", use_container_width=True):
                if not uploaded: st.error("Upload a file")
                elif not tname.strip(): st.error("Enter table name")
                else:
                    d = os.path.expanduser("~/pipeline_agent/data/uploads")
                    os.makedirs(d, exist_ok=True)
                    p = os.path.join(d, uploaded.name)
                    with open(p, "wb") as f: f.write(uploaded.getbuffer())
                    st.session_state.upload_path = p
                    st.session_state.table_name  = (tname.strip().lower()
                        .replace(" ","_").replace("-","_"))
                    st.session_state.step = "ask_hdfs_path"
                    st.rerun()

    # STEP: ask hdfs path + dag name
    elif st.session_state.step == "ask_hdfs_path":
        info  = st.session_state.selected_table
        tname = st.session_state.table_name

        if info:
            st.markdown(f"""<div class="ok-box">
            ✅ <b>{info['table']}</b> from <b>{info['db']}</b> —
            {info['rows']:,} rows · {len(info['columns'])} columns
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="ok-box">
            ✅ File: <b>{os.path.basename(st.session_state.upload_path or "")}</b> →
            table: <b>{tname}</b></div>""", unsafe_allow_html=True)

        HDFS_HOST    = "hdfs://localhost:8020"
        DEFAULT_PATH = f"data/{tname}"

        st.markdown(f"""<div class="agent-box"><div class="agent-lbl">🤖 AI Agent</div>
        Where should I store <b>{tname}</b> in HDFS?<br><br>
        The base address is fixed: <code>{HDFS_HOST}/</code><br>
        Just tell me the path after that.
        </div>""", unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            user_path = st.text_input(
                f"📁 Path  ( {HDFS_HOST}/ + your path )",
                value=DEFAULT_PATH,
                placeholder=f"e.g. {DEFAULT_PATH}",
                help=f"Default: {HDFS_HOST}/{DEFAULT_PATH}")
        with c2:
            dag_input = st.text_input(
                "✈️ DAG name",
                value=f"dag_{tname}",
                placeholder=f"dag_{tname}")

        # Clean path — strip leading slash, spaces
        clean_path = user_path.strip().strip("/").replace(" ","_")
        hdfs_full  = f"{HDFS_HOST}/{clean_path}" if clean_path else ""

        if clean_path:
            dag_id_preview = (dag_input.strip().lower().replace(" ","_")
                              if dag_input.strip() else f"dag_{tname}")
            st.markdown(f"""<div class="path-box">
            📁 <b>HDFS address :</b> {HDFS_HOST}/<b style="color:#ffffff">{clean_path}</b><br>
            🧊 <b>Table stored :</b> {hdfs_full}/pipeline_db/{tname}<br>
            ✈️  <b>DAG id       :</b> {dag_id_preview}
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="path-box" style="color:#484f58">
            📁 {HDFS_HOST}/<i>your/path</i>/pipeline_db/{tname}
            </div>""", unsafe_allow_html=True)

        bc1, bc2 = st.columns(2)
        with bc1:
            if st.button("← Back", use_container_width=True):
                st.session_state.step = ("mysql_tables"
                    if st.session_state.source == "mysql" else "file_upload")
                st.rerun()
        with bc2:
            if st.button("🚀 Run Pipeline", use_container_width=True):
                if not clean_path:
                    st.error("❌ Please enter a path")
                elif not dag_input.strip():
                    st.error("❌ Please enter a DAG name")
                else:
                    st.session_state.warehouse = hdfs_full
                    st.session_state.dag_name  = dag_input.strip()
                    st.session_state.step      = "running"
                    st.rerun()

    elif st.session_state.step == "running":
        warehouse  = st.session_state.warehouse
        table_name = st.session_state.table_name
        dag_name   = st.session_state.dag_name
        source     = st.session_state.source

        st.markdown("""<div class="agent-box"><div class="agent-lbl">🤖 AI Agent</div>
        Running pipeline... please wait ⏳</div>""", unsafe_allow_html=True)

        p1, p2, p3 = st.empty(), st.empty(), st.empty()

        def show(ph, icon, title, state, detail=""):
            cls = "step-done" if state=="done" else "step-run" if state=="run" else "step-wait"
            badge = "✅ Done" if state=="done" else "⏳ Running" if state=="run" else "○ Waiting"
            with ph.container():
                st.markdown(f"""<div class="{cls}">
                <b>{icon} {title}</b> <span style="font-size:11px">{badge}</span>
                {"<br><span style='font-size:11px'>" + detail + "</span>" if detail else ""}
                </div>""", unsafe_allow_html=True)

        show(p1, "📥", "Read & write to HDFS Iceberg", "run")
        show(p2, "✈️", "Generate Airflow DAG", "wait")
        show(p3, "🚀", "Ready to run DAG", "wait")

        err = None
        try:
            spark = get_spark(warehouse)
            if source == "mysql":
                from src.db_connector import push_mysql_to_hdfs
                result = push_mysql_to_hdfs(spark,
                    st.session_state.selected_table, warehouse)
            else:
                from src.db_connector import push_file_to_hdfs
                result = push_file_to_hdfs(spark,
                    st.session_state.upload_path, table_name, warehouse)

            show(p1, "📥", "Read & write to HDFS Iceberg", "done",
                 f"{result['row_count']:,} rows → {result['hdfs_path']}")
            st.session_state.full_table      = result["full_table"]
            st.session_state.hdfs_table_path = result["hdfs_path"]
            st.session_state.row_count       = result["row_count"]

        except Exception as e:
            err = str(e)
            st.markdown(f'<div class="err-box">❌ {e}</div>', unsafe_allow_html=True)

        if not err:
            show(p2, "✈️", "Generate Airflow DAG", "run")
            try:
                from src.dag_generator import generate_and_save_dag
                dag_file, dag_code, dag_id = generate_and_save_dag(
                    table_name, warehouse, dag_name)
                st.session_state.dag_file = dag_file
                st.session_state.dag_code = dag_code
                st.session_state.dag_id   = dag_id
                show(p2, "✈️", "Generate Airflow DAG", "done",
                     f"DAG id: {dag_id} | File: {os.path.basename(dag_file)}")
            except Exception as e:
                st.warning(f"DAG warning: {e}")

            try:
                from src.data_chat import get_schema
                st.session_state.schema = get_schema(
                    spark, table_name, warehouse)
            except: pass

            # Save to pipeline store
            save_pipeline_record({
                "dag_id":     st.session_state.dag_id,
                "table_name": table_name,
                "full_table": st.session_state.full_table,
                "warehouse":  warehouse,
                "hdfs_path":  st.session_state.hdfs_table_path,
                "row_count":  st.session_state.row_count,
                "dag_file":   st.session_state.dag_file,
                "source":     source,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            })
            show(p3, "🚀", "Ready to run DAG", "done",
                 "Click Run DAG button below")
            st.session_state.step = "done"
            st.rerun()

    # STEP: done
    elif st.session_state.step == "done":
        st.markdown(f"""<div class="ok-box" style="font-size:14px">
        ✅ <b>Pipeline Complete!</b><br><br>
        🧊 Table    : <b>{st.session_state.full_table}</b><br>
        📁 HDFS     : <b>{st.session_state.hdfs_table_path}</b><br>
        🔢 Rows     : <b>{(st.session_state.row_count or 0):,}</b><br>
        ✈️  DAG id   : <b>{st.session_state.dag_id}</b><br>
        📄 DAG file : <b>{os.path.basename(st.session_state.dag_file or '')}</b>
        </div>""", unsafe_allow_html=True)

        if st.session_state.dag_code:
            with st.expander("✈️ View DAG Code"):
                st.code(st.session_state.dag_code, language="python")

        st.markdown("---")
        st.markdown("### 🚀 Run DAG")
        st.markdown("""<div class="agent-box"><div class="agent-lbl">🤖 AI Agent</div>
        Your data is stored in HDFS. Click the button below to trigger the Airflow DAG
        and process the data.</div>""", unsafe_allow_html=True)

        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            if st.button("▶ Run DAG Now", use_container_width=True):
                dag_id = st.session_state.dag_id
                with st.spinner(f"Waiting for Airflow to register {dag_id}... (up to 60s)"):
                    # Delete old runs first so @once DAG can re-trigger
                    subprocess.run(
                        [AIRFLOW_BIN,"dags","delete", dag_id,"-y"],
                        capture_output=True, timeout=15)
                    import time; time.sleep(3)
                    ok, msg = trigger_dag(dag_id)
                if ok:
                    st.session_state.dag_run_status = "triggered"
                    st.markdown(f'<div class="run-running">⏳ DAG <b>{dag_id}</b> triggered! Checking status...</div>',
                                unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="run-fail">❌ Trigger failed: {msg}</div>',
                                unsafe_allow_html=True)

        with rc2:
            if st.button("🔄 Check Status", use_container_width=True):
                status = get_dag_status(st.session_state.dag_id)
                state  = status["state"]
                color = {"success":"#3fb950","failed":"#f85149","running":"#58a6ff","queued":"#d29922","up_for_retry":"#d29922","queued":"#d29922","up_for_retry":"#d29922"}.get(state,"#484f58")
                icon   = {"success":"✅","failed":"❌","running":"⏳","queued":"🔵","up_for_retry":"🔄"}.get(state,"🔵")
                st.markdown(f"""<div style="background:#0d1117;border:2px solid {color};
                border-radius:8px;padding:14px;margin:8px 0;">
                <div style="font-size:16px;font-weight:700;color:{color}">{icon} {state.upper()}</div>
                <div style="font-size:11px;color:#484f58;margin-top:6px">
                Run ID: {status['run_id']}<br>
                Start : {status['start_date']}<br>
                End   : {status['end_date']}</div></div>""",
                unsafe_allow_html=True)

        with rc3:
            if st.button("🔄 New Pipeline", use_container_width=True):
                reset(); st.rerun()

        st.markdown("---")
        st.markdown("👉 Go to **💬 Chat with Data** tab to query your data.")
        st.markdown("👉 Go to **🗄️ Saved Pipelines** to see all tables.")

# ══════════════════════════════════════════════════════════
# TAB 2 — SAVED PIPELINES
# ══════════════════════════════════════════════════════════
with tab_saved:
    st.markdown("### 🗄️ Saved Pipelines")
    pipelines = load_pipelines()

    if not pipelines:
        st.markdown("""<div class="info-box">No pipelines yet.
        Run a pipeline from the 🤖 AI Agent tab first.</div>""",
        unsafe_allow_html=True)
    else:
        for p in pipelines:
            with st.container():
                st.markdown(f"""<div class="pipe-card">
                <div style="display:flex;justify-content:space-between;align-items:center">
                  <div>
                    <span style="color:#3fb950;font-weight:700;font-family:monospace;font-size:15px">
                    🧊 {p.get('table_name','')}</span>
                    <span style="color:#484f58;font-size:11px;margin-left:10px">
                    {p.get('created_at','')}</span>
                  </div>
                  <span style="color:#58a6ff;font-family:monospace;font-size:12px">
                  {p.get('row_count',0):,} rows</span>
                </div>
                <div style="font-size:11px;color:#484f58;margin-top:6px;font-family:monospace">
                  DAG: {p.get('dag_id','')} &nbsp;|&nbsp;
                  Path: {p.get('hdfs_path','')}</div>
                </div>""", unsafe_allow_html=True)

                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    if st.button("💬 Load & Chat",
                                 key=f"chat_{p['dag_id']}",
                                 use_container_width=True):
                        spark = get_spark(p["warehouse"])
                        st.session_state.update({
                            "full_table":       p["full_table"],
                            "table_name":       p["table_name"],
                            "warehouse":        p["warehouse"],
                            "hdfs_table_path":  p["hdfs_path"],
                            "row_count":        p["row_count"],
                            "dag_id":           p["dag_id"],
                            "dag_file":         p.get("dag_file",""),
                            "chat_history":     [],
                        })
                        try:
                            from src.data_chat import get_schema
                            st.session_state.schema = get_schema(
                                spark, p["table_name"], p["warehouse"])
                        except: pass
                        st.success(f"✅ Loaded {p['table_name']}")
                        st.rerun()

                with c2:
                    if st.button("▶ Run DAG",
                                 key=f"run_{p['dag_id']}",
                                 use_container_width=True):
                        dag_id = p["dag_id"]
                        subprocess.run([AIRFLOW_BIN,"dags","delete",dag_id,"-y"],
                                       capture_output=True, timeout=15)
                        import time; time.sleep(3)
                        ok, msg = trigger_dag(dag_id)
                        if ok:
                            st.markdown(
                                f'<div class="run-running">⏳ {dag_id} triggered!</div>',
                                unsafe_allow_html=True)
                        else:
                            st.markdown(
                                f'<div class="run-fail">❌ {msg}</div>',
                                unsafe_allow_html=True)

                with c3:
                    if st.button("📊 DAG Status",
                                 key=f"status_{p['dag_id']}",
                                 use_container_width=True):
                        status = get_dag_status(p["dag_id"])
                        state  = status["state"]
                        color = {"success":"#3fb950","failed":"#f85149","running":"#58a6ff","queued":"#d29922","up_for_retry":"#d29922","queued":"#d29922","up_for_retry":"#d29922"}.get(state,"#484f58")
                        icon   = {"success":"✅","failed":"❌","running":"⏳","queued":"🔵","up_for_retry":"🔄"}.get(state,"🔵")
                        st.markdown(
                            f'<div style="border:1px solid {color};border-radius:6px;'
                            f'padding:8px 12px;color:{color};font-size:13px">'
                            f'{icon} {state.upper()}<br>'
                            f'<span style="font-size:10px;color:#484f58">'
                            f'{status["start_date"]}</span></div>',
                            unsafe_allow_html=True)

                with c4:
                    if st.button("🔍 View DAG",
                                 key=f"view_{p['dag_id']}",
                                 use_container_width=True):
                        dag_f = p.get("dag_file","")
                        if dag_f and os.path.exists(dag_f):
                            with open(dag_f) as f:
                                st.code(f.read(), language="python")
                        else:
                            st.warning("DAG file not found")

                st.markdown("<hr style='border-color:#21262d;margin:8px 0'>",
                            unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════
# TAB 3 — CHAT
# ══════════════════════════════════════════════════════════
with tab_chat:
    st.markdown("## 💬 Chat with Your Data")

    if not st.session_state.full_table:
        st.markdown("""<div class="info-box">
        No table loaded. Go to 🤖 AI Agent or 🗄️ Saved Pipelines and load a table first.
        </div>""", unsafe_allow_html=True)
    else:
        from src.data_chat import ask_ai, execute_sql, get_schema

        spark = get_spark(st.session_state.warehouse)
        if not st.session_state.schema:
            st.session_state.schema = get_schema(
                spark, st.session_state.table_name,
                st.session_state.warehouse)

        st.markdown(f"""<div class="path-box">
        <b style="color:#3fb950">{st.session_state.full_table}</b>
        &nbsp;&nbsp;<span style="color:#58a6ff">
        {(st.session_state.row_count or 0):,} rows</span>
        &nbsp;&nbsp;<span style="color:#484f58;font-size:10px">
        {st.session_state.warehouse}</span>
        </div>""", unsafe_allow_html=True)

        st.markdown("**Quick Actions:**")
        qc = st.columns(5)
        quick_map = {
            "Top 10":      "show top 10 rows",
            "Count Rows":  "count total rows",
            "Schema":      "show all columns and types",
            "Null Check":  "count null values per column",
            "Statistics":  "show min max avg for numeric columns",
        }
        quick_input = None
        for col, (label, query) in zip(qc, quick_map.items()):
            if col.button(label, key=f"q_{label}", use_container_width=True):
                quick_input = query

        if st.session_state.schema:
            with st.expander("📋 Schema"):
                st.dataframe(pd.DataFrame(st.session_state.schema),
                             use_container_width=True, height=150)

        st.markdown("---")

        for msg in st.session_state.chat_history:
            st.markdown(f'<div class="chat-user">🧑 {msg["user"]}</div>',
                        unsafe_allow_html=True)
            st.markdown(f'<div class="chat-bot">🤖 {msg["explanation"]}</div>',
                        unsafe_allow_html=True)
            st.markdown(f'<div class="chat-sql">SQL → {msg["sql"]}</div>',
                        unsafe_allow_html=True)
            if msg.get("error"):
                st.markdown(f'<div class="err-box">❌ {msg["error"]}</div>',
                            unsafe_allow_html=True)
            elif msg.get("warning"):
                st.markdown(
                    f'<div class="warn-box">⚠️ {msg["warning"]}<br>'
                    f'Run manually to confirm: <code>{msg["sql"]}</code></div>',
                    unsafe_allow_html=True)
            elif msg.get("result"):
                st.markdown(
                    f'<div class="chat-result">{msg["result"]}</div>',
                    unsafe_allow_html=True)
                if msg.get("rows") is not None:
                    st.caption(f"↳ {msg['rows']:,} rows")
            st.markdown("")

        user_input  = st.chat_input("Ask: 'show top 20', 'count by year', 'avg salary'...")
        final_input = quick_input or user_input

        if final_input:
            with st.spinner("🤖 AI thinking..."):
                hist = [{"user": m["user"], "assistant": m["explanation"]}
                        for m in st.session_state.chat_history[-4:]]
                try:
                    ai_result = ask_ai(
                        final_input,
                        st.session_state.table_name,
                        st.session_state.schema,
                        st.session_state.warehouse,
                        hist)
                    sql         = ai_result.get("sql","")
                    explanation = ai_result.get("explanation","")
                    destructive = ai_result.get("is_destructive", False)
                    warning     = ai_result.get("warning","")

                    if destructive:
                        st.session_state.chat_history.append({
                            "user": final_input, "sql": sql,
                            "explanation": explanation,
                            "warning": warning, "result": None,
                            "rows": None, "error": None})
                    else:
                        out, rows, error = execute_sql(
                            spark, sql, st.session_state.warehouse)
                        st.session_state.chat_history.append({
                            "user": final_input, "sql": sql,
                            "explanation": explanation,
                            "result": out, "rows": rows,
                            "error": error, "warning": None})
                except Exception as e:
                    st.session_state.chat_history.append({
                        "user": final_input, "sql": "",
                        "explanation": "Error", "result": None,
                        "rows": None, "error": str(e), "warning": None})
            st.rerun()

        if st.session_state.chat_history:
            if st.button("🗑️ Clear Chat"):
                st.session_state.chat_history = []; st.rerun()
