import streamlit as st
import websocket
import json
import duckdb
import threading
import time
from datetime import datetime
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
            vol BIGINT, value DOUBLE, type VARCHAR
        )
    """)
    con.execute("CREATE TABLE IF NOT EXISTS sys_logs (waktu TIMESTAMP, pesan VARCHAR)")
    return con

db = init_db()

@st.cache_resource
def get_sys_config():
    # Menyimpan status background worker antar-thread
    return {"auto_export": False, "interval": 30, "last_export": int(time.time())}

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
    # 2A. WSS ENGINE
    def on_message(ws, message):
        try:
            msg = json.loads(message)
            if msg.get("type") == "trade":
                trade = msg.get("data", {})
                ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
                ticker = trade.get('t', '')
                price = float(trade.get('p', 0))
                lot = int(trade.get('l', 0))
                
                vol = lot * 100
                val = price * vol
                type_action = str(trade.get('c', '')).upper()
                
                db.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", (ts, ticker, price, vol, val, type_action))
        except Exception as e:
            pass # Silent fail untuk kecepatan

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

    # 2B. CRON JOB WORKER (Auto-Export & Alarm 16:30)
    def run_cron():
        while True:
            time.sleep(1)
            now_wib = datetime.now(WIB)
            
            # Alarm Purge 16:30 WIB
            if now_wib.hour == 16 and now_wib.minute == 30 and now_wib.second == 0:
                catat_log("AUTO-PURGE 16:30: Mengosongkan RAM DB...")
                db.execute("DELETE FROM trades")
                time.sleep(2) # Mencegah trigger ganda
                
            # Auto Export GSheets
            if sys_cfg["auto_export"]:
                curr_time = int(time.time())
                if curr_time - sys_cfg["last_export"] >= sys_cfg["interval"]:
                    # Eksekusi Tembakan
                    try:
                        wh_url = st.secrets.get("WEBHOOK_URL", "")
                        if wh_url:
                            # Top Summary
                            df_top = db.execute("SELECT ticker, SUM(value) as Net_Value FROM trades GROUP BY ticker ORDER BY Net_Value DESC LIMIT 10").df()
                            if not df_top.empty:
                                df_top.insert(0, 'waktu', datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S'))
                                df_top['waktu'] = df_top['waktu'].astype(str)
                                requests.post(wh_url, json={"kategori": "Top_Summary", "data": df_top.values.tolist()})
                                
                            # Global Whales (Hardcode limit 50jt untuk background)
                            df_whale = db.execute("SELECT * FROM trades WHERE value >= 50000000 ORDER BY timestamp DESC LIMIT 30").df()
                            if not df_whale.empty:
                                df_whale['timestamp'] = df_whale['timestamp'].astype(str)
                                requests.post(wh_url, json={"kategori": "Global_Whales", "data": df_whale.values.tolist()})
                    except:
                        pass
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

# Format Kolom (Rapih & Delimiter)
cfg_std = {
    "timestamp": st.column_config.DatetimeColumn("Waktu", format="HH:mm:ss"),
    "ticker": st.column_config.TextColumn("Emiten"),
    "price": st.column_config.NumberColumn("Harga", format="%d"),
    "vol": st.column_config.NumberColumn("Volume (Lembar)", format="%,d"),
    "value": st.column_config.NumberColumn("Nilai (Rp)", format="%,d"),
    "type": st.column_config.TextColumn("Action")
}
cfg_top = {
    "ticker": st.column_config.TextColumn("Emiten"),
    "Net_Value": st.column_config.NumberColumn("Total Akumulasi (Rp)", format="%,d")
}

# --- SIDEBAR KONTROL ---
st.sidebar.header("🕹️ Control Panel")
pause_scroll = st.sidebar.checkbox("⏸️ Pause Live View (Untuk Scrolling)", value=False)
whale_limit = st.sidebar.number_input("Batas Paus (Rp)", value=50000000, step=10000000)

st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ Auto-Export GSheets")
sys_cfg["auto_export"] = st.sidebar.toggle("Aktifkan Auto-Export", value=False)
sys_cfg["interval"] = st.sidebar.slider("Interval (Detik)", min_value=10, max_value=180, value=30, step=10)

# --- TAB DASHBOARD ---
t1, t2, t3, t4 = st.tabs(["The Cockpit", "Screener", "Raw Market", "System & Exporter"])

with t1:
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("Radar Velocity")
        f_top = st.radio("Aksi:", ["All", "BUY", "SELL"], horizontal=True, key="rtop")
        q_top = f"WHERE type = '{f_top}'" if f_top != "All" else ""
        df_top = db.execute(f"SELECT ticker, SUM(value) as Net_Value FROM trades {q_top} GROUP BY ticker ORDER BY Net_Value DESC LIMIT 15").df()
        st.dataframe(df_top, column_config=cfg_top, hide_index=True, height=500, use_container_width=True)
        
    with c2:
        st.subheader(f"Whale Trades (> Rp {whale_limit:,.0f})")
        f_wh = st.radio("Aksi Paus:", ["All", "BUY", "SELL"], horizontal=True, key="rwh")
        q_wh_type = f"AND type = '{f_wh}'" if f_wh != "All" else ""
        df_whale = db.execute(f"SELECT * FROM trades WHERE value >= {whale_limit} {q_wh_type} ORDER BY timestamp DESC LIMIT 200").df()
        st.dataframe(df_whale, column_config=cfg_std, hide_index=True, height=500, use_container_width=True)

with t2:
    st.subheader("Watchlist Orderbook")
    input_em = st.text_input("Ketik Emiten (Pisahkan koma):", "BBCA, BREN, BMRI, AMMN")
    f_wl = st.radio("Aksi Emiten:", ["All", "BUY", "SELL"], horizontal=True, key="rwl")
    
    list_emiten = [x.strip().upper() for x in input_em.split(",") if x.strip()]
    if list_emiten:
        cols = st.columns(len(list_emiten) if len(list_emiten) <= 4 else 4)
        for i, saham in enumerate(list_emiten):
            with cols[i % 4]:
                st.markdown(f"**{saham}**")
                q_wl_type = f"AND type = '{f_wl}'" if f_wl != "All" else ""
                df_saham = db.execute(f"SELECT timestamp, price, vol, value, type FROM trades WHERE ticker = '{saham}' {q_wl_type} ORDER BY timestamp DESC LIMIT 100").df()
                st.dataframe(df_saham, column_config=cfg_std, hide_index=True, height=400, use_container_width=True)

with t3:
    st.subheader("Historical Tape (Full Market)")
    f_raw = st.radio("Aksi Tape:", ["All", "BUY", "SELL"], horizontal=True, key="rraw")
    q_raw = f"WHERE type = '{f_raw}'" if f_raw != "All" else ""
    df_raw = db.execute(f"SELECT * FROM trades {q_raw} ORDER BY timestamp DESC LIMIT 2000").df()
    st.dataframe(df_raw, column_config=cfg_std, hide_index=True, height=600, use_container_width=True)

with t4:
    st.subheader("Manual Exporter & Log")
    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("Manual: Kirim Top Summary"):
            df_ex = db.execute("SELECT ticker, SUM(value) as Net_Value FROM trades GROUP BY ticker ORDER BY Net_Value DESC LIMIT 10").df()
            df_ex.insert(0, 'waktu', datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S'))
            sukses, msg = kirim_ke_gsheets("Top_Summary", df_ex)
            if sukses: st.success("Terkirim!")
            else: st.error(msg)
    with colB:
        if st.button("Manual: Kirim Watchlist"):
            if list_emiten:
                tup_saham = tuple(list_emiten) if len(list_emiten) > 1 else f"('{list_emiten[0]}')"
                df_ex = db.execute(f"SELECT * FROM trades WHERE ticker IN {tup_saham} ORDER BY timestamp DESC LIMIT 50").df()
                sukses, msg = kirim_ke_gsheets("Watchlist_Alerts", df_ex)
                if sukses: st.success("Terkirim!")
                else: st.error(msg)
    with colC:
        if st.button("Manual: Kirim Global Whales"):
            df_ex = db.execute(f"SELECT * FROM trades WHERE value >= {whale_limit} ORDER BY timestamp DESC LIMIT 50").df()
            sukses, msg = kirim_ke_gsheets("Global_Whales", df_ex)
            if sukses: st.success("Terkirim!")
            else: st.error(msg)

    st.markdown("---")
    with st.expander("📡 Status Koneksi WSS (Klik untuk buka log)"):
        df_logs = db.execute("SELECT * FROM sys_logs ORDER BY waktu DESC LIMIT 15").df()
        st.dataframe(df_logs, use_container_width=True)

# ==========================================
# TRIGGER REFRESH & SCROLL CONTROL
# ==========================================
if not pause_scroll:
    time.sleep(3)
    st.rerun()
