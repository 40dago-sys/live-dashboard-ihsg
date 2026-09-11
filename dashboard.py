import streamlit as st
import websocket
import json
import duckdb
import threading
import time
from datetime import datetime, timedelta
import pytz
import requests

# ==========================================
# BLOK 1: KONFIGURASI, DATABASE & STATE
# ==========================================
st.set_page_config(page_title="Institutional Tape Reading", layout="wide", initial_sidebar_state="expanded")
WIB = pytz.timezone('Asia/Jakarta')

@st.cache_resource
def init_db():
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            timestamp TIMESTAMP, ticker VARCHAR, price DOUBLE, 
            lot BIGINT, value DOUBLE, type VARCHAR
        )
    """)
    con.execute("CREATE TABLE IF NOT EXISTS sys_logs (waktu TIMESTAMP, pesan VARCHAR)")
    return con

db = init_db()

@st.cache_resource
def get_sys_config():
    # Master_watchlist untuk Kabel Server
    return {"auto_export": False, "interval": 30, "last_export": int(time.time()), "master_watchlist": ["BBCA", "BMRI"]}

sys_cfg = get_sys_config()

def catat_log(pesan):
    ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
    try: db.execute("INSERT INTO sys_logs VALUES (?, ?)", (ts, str(pesan)))
    except: pass

# ==========================================
# BLOK 2: BACKGROUND ENGINE (WSS & WORKER)
# ==========================================
@st.cache_resource
def start_engines():
    def on_message(ws, message):
        try:
            msg = json.loads(message)
            if msg.get("type") == "trade":
                trade = msg.get("data", {})
                ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
                ticker = trade.get('t', '')
                price = float(trade.get('p', 0))
                lot = int(trade.get('l', 0))
                val = price * lot * 100
                type_action = str(trade.get('c', '')).upper()
                db.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", (ts, ticker, price, lot, val, type_action))
        except Exception as e:
            pass 

    def on_error(ws, error): catat_log(f"WS ERROR: {error}")
    def on_close(ws, c_code, c_msg): catat_log(f"WS CLOSED: {c_code}")

    def run_ws():
        ws_url = "wss://stock.arjum.com/ws/running-trade"
        try: api_key = st.secrets["ARJUM_API_KEY"]
        except: api_key = "sk_live_WI0TxPlSGJ_rMZyJfzAkXIihSJsFD72QkKTNnFVo16E" 
            
        headers = [f"X-API-Key: {api_key}"]
        ws = websocket.WebSocketApp(ws_url, header=headers, on_message=on_message, on_error=on_error, on_close=on_close)
        while True:
            catat_log("Menyambungkan WSS...")
            ws.run_forever()
            time.sleep(3)

    def run_cron():
        # ⏰ SETELAN JAM DIVERGENT FEEDER
        jadwal_feeder = [(18, 20)]
        
        while True:
            time.sleep(1)
            now_wib = datetime.now(WIB)
            
            # Auto Purge Database Jam 16:30
            if now_wib.hour == 16 and now_wib.minute == 30 and now_wib.second == 0:
                catat_log("AUTO-PURGE 16:30: Mengosongkan RAM DB...")
                db.execute("DELETE FROM trades")
                time.sleep(2) 
                
            # Trigger Divergent Feeder via Webhook GAS
            for jam, menit in jadwal_feeder:
                if now_wib.hour == jam and now_wib.minute == menit and now_wib.second == 0:
                    catat_log(f"Alarm {jam}:{menit} - Menembak Trigger Divergent ke GSheets...")
                    try:
                        wh_url = st.secrets.get("WEBHOOK_URL", "")
                        if wh_url:
                            res = requests.post(wh_url, json={"kategori": "TRIGGER_DIVERGENT"})
                            catat_log(f"Respons Divergent: {res.text}")
                    except Exception as e:
                        catat_log(f"Error Divergent: {str(e)}")
                        
            # Auto-Export WSS Data ke GSheets
            if sys_cfg["auto_export"]:
                curr_time = int(time.time())
                if curr_time - sys_cfg["last_export"] >= sys_cfg["interval"]:
                    try:
                        wh_url = st.secrets.get("WEBHOOK_URL", "")
                        if wh_url:
                            # 1. Tembak Radar Flow (Logika Net Accumulation)
                            q_top = """
                                SELECT ticker, 
                                       SUM(CASE WHEN type = 'BUY' THEN value WHEN type = 'SELL' THEN -value ELSE 0 END) as Net_Value 
                                FROM trades GROUP BY ticker ORDER BY Net_Value DESC LIMIT 10
                            """
                            df_top = db.execute(q_top).df()
                            if not df_top.empty:
                                df_top.insert(0, 'waktu', datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S'))
                                df_top['waktu'] = df_top['waktu'].astype(str)
                                r1 = requests.post(wh_url, json={"kategori": "Top_Summary", "data": df_top.values.tolist()})
                                catat_log(f"Auto Radar Flow: {r1.text}")
                                
                            # 2. Tembak Global Whales (Fixed minimal 50jt)
                            df_whale = db.execute("SELECT * FROM trades WHERE value >= 50000000 ORDER BY timestamp DESC LIMIT 30").df()
                            if not df_whale.empty:
                                df_whale['timestamp'] = df_whale['timestamp'].astype(str)
                                r2 = requests.post(wh_url, json={"kategori": "Global_Whales", "data": df_whale.values.tolist()})
                                catat_log(f"Auto Global Whales: {r2.text}")

                            # 3. Tembak Watchlist (Membaca Kabel Server)
                            wl = sys_cfg.get("master_watchlist", [])
                            if wl:
                                tup_saham = tuple(wl) if len(wl) > 1 else f"('{wl[0]}')"
                                df_wl = db.execute(f"SELECT * FROM trades WHERE ticker IN {tup_saham} ORDER BY timestamp DESC LIMIT 50").df()
                                if not df_wl.empty:
                                    df_wl['timestamp'] = df_wl['timestamp'].astype(str)
                                    r3 = requests.post(wh_url, json={"kategori": "Watchlist_Alerts", "data": df_wl.values.tolist()})
                                    catat_log(f"Auto Watchlist: {r3.text}")
                    except Exception as e:
                        catat_log(f"Auto-Export Error: {str(e)}")
                        
                    sys_cfg["last_export"] = curr_time

    threading.Thread(target=run_ws, daemon=True).start()
    threading.Thread(target=run_cron, daemon=True).start()

start_engines()

# ==========================================
# BLOK 3: EXPORTER MANUAL (FUNGSI)
# ==========================================
def kirim_ke_gsheets(kategori, df):
    try:
        if df.empty: return False, "Data kosong."
        webhook_url = st.secrets.get("WEBHOOK_URL", "")
        if not webhook_url: return False, "Webhook URL tidak diset."
        
        if 'timestamp' in df.columns: df['timestamp'] = df['timestamp'].astype(str)
        if 'waktu' in df.columns: df['waktu'] = df['waktu'].astype(str)
        
        res = requests.post(webhook_url, json={"kategori": kategori, "data": df.values.tolist()})
        return (True, res.text) if res.status_code == 200 else (False, f"HTTP {res.status_code}")
    except Exception as e:
        return False, str(e)

# ==========================================
# BLOK 4: USER INTERFACE & FORMATTING
# ==========================================
st.title("Arjum Institutional Terminal")

cfg_std = {
    "timestamp": st.column_config.DatetimeColumn("Waktu", format="HH:mm:ss"),
    "ticker": st.column_config.TextColumn("Emiten"),
    "price": st.column_config.NumberColumn("Harga", format="%d"),
    "lot": st.column_config.NumberColumn("Volume (Lot)", format="%,d"),
    "value": st.column_config.NumberColumn("Nilai (Rp)", format="%,d"),
    "type": st.column_config.TextColumn("Action")
}
cfg_top = {
    "ticker": st.column_config.TextColumn("Emiten"),
    "Net_Value": st.column_config.NumberColumn("Net Akumulasi (Rp)", format="%,d")
}

# --- SIDEBAR KONTROL ---
st.sidebar.header("🕹️ Control Panel")
pause_scroll = st.sidebar.checkbox("⏸️ Pause Live View (Scrolling)", value=False)

opsi_paus = {
    "Rp 50,000,000": 50000000, "Rp 75,000,000": 75000000, 
    "Rp 100,000,000": 100000000, "Rp 200,000,000": 200000000, 
    "Rp 300,000,000": 300000000, "Rp 400,000,000": 400000000, 
    "Rp 500,000,000": 500000000, "Rp 1,000,000,000": 1000000000
}
pilihan_paus = st.sidebar.selectbox("Batas Paus (Layar)", list(opsi_paus.keys()), index=0)
whale_limit = opsi_paus[pilihan_paus] 

radar_time = st.sidebar.selectbox("Timeframe Radar (Menit)", [5, 15, 30, 60, 120, 240], index=0)

# TAMBAHAN BARU: Slider Performa Limit Baris
st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ UI Performance Tuning")
limit_wl = st.sidebar.slider("Limit Baris Watchlist", min_value=100, max_value=2000, value=500, step=100)
limit_tape = st.sidebar.slider("Limit Baris Full Tape", min_value=1000, max_value=15000, value=5000, step=1000)

st.sidebar.markdown("---")
st.sidebar.subheader("📡 Kabel Server (Auto-Export)")

master_wl_input = st.sidebar.text_input("Target Emiten ke GSheets:", ", ".join(sys_cfg["master_watchlist"]))
sys_cfg["master_watchlist"] = [x.strip().upper() for x in master_wl_input.split(",") if x.strip()]

sys_cfg["auto_export"] = st.sidebar.toggle("Aktifkan Auto-Export", value=False)
sys_cfg["interval"] = st.sidebar.slider("Interval (Detik)", min_value=10, max_value=180, value=30, step=10)

t1, t2, t3, t4 = st.tabs(["The Cockpit", "Screener", "Raw Market", "System & Exporter"])

with t1:
    c1, c2 = st.columns([1, 2])
    
    with c1:
        st.subheader(f"Radar Net Flow ({radar_time} Menit)")
        f_radar = st.radio("Aksi Radar:", ["All", "BUY", "SELL"], horizontal=True, key="rradar")
        
        if f_radar == "All":
            q_netflow = f"""
                SELECT ticker, 
                       SUM(CASE WHEN type = 'BUY' THEN value WHEN type = 'SELL' THEN -value ELSE 0 END) as Net_Value 
                FROM trades WHERE timestamp >= NOW() - INTERVAL {radar_time} MINUTE
                GROUP BY ticker ORDER BY Net_Value DESC LIMIT 15
            """
        else:
            q_netflow = f"""
                SELECT ticker, SUM(value) as Net_Value 
                FROM trades WHERE timestamp >= NOW() - INTERVAL {radar_time} MINUTE AND type = '{f_radar}'
                GROUP BY ticker ORDER BY Net_Value DESC LIMIT 15
            """
            
        df_top = db.execute(q_netflow).df()
        st.dataframe(df_top, column_config=cfg_top, hide_index=True, height=500, use_container_width=True)
        
    with c2:
        st.subheader(f"Whale Trades (>= {pilihan_paus})")
        f_wh = st.radio("Aksi Paus:", ["All", "BUY", "SELL"], horizontal=True, key="rwh")
        q_wh_type = f"AND type = '{f_wh}'" if f_wh != "All" else ""
        df_whale = db.execute(f"SELECT * FROM trades WHERE value >= {whale_limit} {q_wh_type} ORDER BY timestamp DESC LIMIT 200").df()
        st.dataframe(df_whale, column_config=cfg_std, hide_index=True, height=500, use_container_width=True)

with t2:
    st.subheader("Watchlist Orderbook (Layar Pribadi)")
    input_ui_wl = st.text_input("Ketik Emiten (Pisahkan koma):", "BREN, AMMN", key="ui_wl_input")
    f_wl = st.radio("Aksi Emiten:", ["All", "BUY", "SELL"], horizontal=True, key="rwl")
    
    list_ui_emiten = [x.strip().upper() for x in input_ui_wl.split(",") if x.strip()]
    if list_ui_emiten:
        cols = st.columns(min(len(list_ui_emiten), 3))
        for i, saham in enumerate(list_ui_emiten):
            with cols[i % 3]:
                st.markdown(f"**{saham}**")
                q_wl_type = f"AND type = '{f_wl}'" if f_wl != "All" else ""
                # LIMIT dihubungkan ke slider limit_wl
                df_saham = db.execute(f"SELECT timestamp, price, lot, value, type FROM trades WHERE ticker = '{saham}' {q_wl_type} ORDER BY timestamp DESC LIMIT {limit_wl}").df()
                st.dataframe(df_saham, column_config=cfg_std, hide_index=True, height=400, use_container_width=True)

with t3:
    st.subheader("Historical Tape (Full Market)")
    f_raw = st.radio("Aksi Tape:", ["All", "BUY", "SELL"], horizontal=True, key="rraw")
    q_raw = f"WHERE type = '{f_raw}'" if f_raw != "All" else ""
    # LIMIT dihubungkan ke slider limit_tape
    df_raw = db.execute(f"SELECT * FROM trades {q_raw} ORDER BY timestamp DESC LIMIT {limit_tape}").df()
    st.dataframe(df_raw, column_config=cfg_std, hide_index=True, height=600, use_container_width=True)

with t4:
    st.subheader("Manual Exporter & Log")
    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("Manual: Kirim Radar Flow"):
            q_ex_netflow = f"""
                SELECT ticker, SUM(CASE WHEN type = 'BUY' THEN value WHEN type = 'SELL' THEN -value ELSE 0 END) as Net_Value 
                FROM trades WHERE timestamp >= NOW() - INTERVAL {radar_time} MINUTE GROUP BY ticker ORDER BY Net_Value DESC LIMIT 10
            """
            df_ex = db.execute(q_ex_netflow).df()
            df_ex.insert(0, 'waktu', datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S'))
            sukses, msg = kirim_ke_gsheets("Top_Summary", df_ex)
            if sukses: st.info(f"Respons Google: {msg}")
            else: st.error(msg)
    with colB:
        if st.button("Manual: Kirim Target GSheets"):
            wl_master = sys_cfg.get("master_watchlist", [])
            if wl_master:
                tup_saham = tuple(wl_master) if len(wl_master) > 1 else f"('{wl_master[0]}')"
                df_ex = db.execute(f"SELECT * FROM trades WHERE ticker IN {tup_saham} ORDER BY timestamp DESC LIMIT 50").df()
                sukses, msg = kirim_ke_gsheets("Watchlist_Alerts", df_ex)
                if sukses: st.info(f"Respons Google: {msg}")
                else: st.error(msg)
            else: st.warning("Kabel Server (Target Emiten) kosong.")
    with colC:
        if st.button("Manual: Kirim Global Whales"):
            df_ex = db.execute(f"SELECT * FROM trades WHERE value >= {whale_limit} ORDER BY timestamp DESC LIMIT 50").df()
            sukses, msg = kirim_ke_gsheets("Global_Whales", df_ex)
            if sukses: st.info(f"Respons Google: {msg}")
            else: st.error(msg)

    st.markdown("---")
    with st.expander("📡 Status Koneksi WSS & Log Mesin"):
        df_logs = db.execute("SELECT * FROM sys_logs ORDER BY waktu DESC LIMIT 15").df()
        st.dataframe(df_logs, use_container_width=True)

    st.markdown("---")
    st.subheader("🧪 UAT Mode (Market Closed Simulator)")
    if st.button("Suntik 100 Data Dummy"):
        import random
        ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
        saham_list = ["BBCA", "BREN", "BMRI", "AMMN", "ASII", "CUAN", "TPIA"]
        for _ in range(100):
            ticker = random.choice(saham_list)
            price = random.randint(1000, 9000)
            lot = random.randint(10, 15000) 
            val = price * lot * 100
            tipe = random.choice(["BUY", "SELL"])
            db.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", (ts, ticker, price, lot, val, tipe))
        st.success("✅ 100 Baris Data Dummy berhasil disuntikkan!")

if not pause_scroll:
    time.sleep(3)
    st.rerun()
