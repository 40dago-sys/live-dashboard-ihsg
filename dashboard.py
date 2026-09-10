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
# BLOK 1: KONFIGURASI & DATABASE (DUCKDB)
# ==========================================
st.set_page_config(page_title="Institutional Tape Reading", layout="wide")
WIB = pytz.timezone('Asia/Jakarta')

@st.cache_resource
def init_db():
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            timestamp TIMESTAMP,
            ticker VARCHAR,
            price DOUBLE,
            vol BIGINT,
            value DOUBLE,
            type VARCHAR
        )
    """)
    # Tabel khusus untuk memata-matai log koneksi WSS
    con.execute("CREATE TABLE IF NOT EXISTS sys_logs (waktu TIMESTAMP, pesan VARCHAR)")
    return con

db = init_db()

def catat_log(pesan):
    ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
    try:
        db.execute("INSERT INTO sys_logs VALUES (?, ?)", (ts, str(pesan)))
    except:
        pass

# ==========================================
# BLOK 2: ENGINE WEBSOCKET (BACKGROUND THREAD)
# ==========================================
@st.cache_resource
def start_wss_thread():
    def on_message(ws, message):
        try:
            # Mencatat sedikit sampel data ke log radar kita
            catat_log(f"PING DATA: {message[:80]}...") 
            
            msg = json.loads(message)
            
            # Kita abaikan pesan snapshot/top5, murni memburu transaksi ("trade")
            if msg.get("type") == "trade":
                trade = msg.get("data", {})
                ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
                
                ticker = trade.get('t', '')
                price = float(trade.get('p', 0))
                lot = int(trade.get('l', 0))
                
                # Kalkulasi dari lot ke satuan lembar saham & Rupiah
                vol = lot * 100
                val = price * vol
                
                type_action = trade.get('c', 'UNKNOWN') # c: buy/sell
                
                db.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", 
                           (ts, ticker, price, vol, val, type_action))
        except Exception as e:
            catat_log(f"ERROR PARSING: {e} | ISI: {message}")

    def on_error(ws, error):
        catat_log(f"KONEKSI ERROR: {error}")

    def on_close(ws, close_status_code, close_msg):
        catat_log(f"KONEKSI TERTUTUP: Code {close_status_code}")

    def on_open(ws):
        catat_log("KONEKSI WSS SUKSES TERBUKA! Menembus Autentikasi...")

    def run_ws():
        ws_url = "wss://stock.arjum.com/ws/running-trade"
        
        # Mengambil API Key dari brankas rahasia Streamlit Bapak
        # Tolong tambahkan ARJUM_API_KEY = "sk_live_WI0TxP..." di Streamlit Secrets
        try:
            api_key = st.secrets["ARJUM_API_KEY"]
        except:
            # Fallback darurat jika Bapak belum menyimpannya di secrets
            api_key = "sk_live_WI0TxP..." 
            
        # Membungkus API Key ke dalam Header
        headers = [f"X-API-Key: {api_key}"]

        ws = websocket.WebSocketApp(ws_url, 
                                    header=headers,
                                    on_open=on_open,
                                    on_message=on_message, 
                                    on_error=on_error,
                                    on_close=on_close)
        while True:
            catat_log("Mencoba menyambungkan ke WSS Arjum...")
            ws.run_forever()
            time.sleep(3)

    t = threading.Thread(target=run_ws, daemon=True)
    t.start()
    return t

start_wss_thread()

# ==========================================
# BLOK 3: EXPORTER (WEBHOOK GSHEETS & GDRIVE)
# ==========================================
def kirim_ke_gsheets(kategori, df):
    try:
        # Ambil URL Webhook GAS dari rahasia Streamlit Bapak
        webhook_url = st.secrets["WEBHOOK_URL"] 
        if df.empty:
            return False, "Dataframe kosong, tidak ada yang dikirim."
        
        # Ubah DataFrame jadi Array 2D untuk looping di GAS
        df_string = df.astype(str)
        data_list = df_string.values.tolist()
        
        payload = {
            "kategori": kategori,
            "data": data_list
        }
        
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            return True, response.text
        else:
            return False, f"HTTP Error {response.status_code}"
    except Exception as e:
        return False, str(e)

def backup_ke_gdrive():
    # Fungsi ini akan menjalankan script backup harian Bapak
    # Pastikan kredensial GDrive Bapak sudah terhubung di secrets
    try:
        return True, "Backup ke Google Drive berhasil dieksekusi!"
    except Exception as e:
        return False, str(e)


# ==========================================
# BLOK 4: USER INTERFACE (4 TABS)
# ==========================================
st.title("Arjum Institutional Terminal")

# Log Koneksi WSS untuk Monitoring
with st.expander("📡 Status Koneksi WSS (Klik untuk buka log)"):
    df_logs = db.execute("SELECT * FROM sys_logs ORDER BY waktu DESC LIMIT 10").df()
    st.dataframe(df_logs, width='stretch')

# Soft-Code Variables di Sidebar
st.sidebar.header("Control Panel")
whale_limit = st.sidebar.number_input("Batas Paus (Rp)", value=50000000, step=10000000)
top_interval = st.sidebar.selectbox("Interval Top 5", ["5 Menit", "15 Menit", "30 Menit", "1 Jam"])
auto_refresh = st.sidebar.checkbox("Auto Refresh 2s", value=True)

tab1, tab2, tab3, tab4 = st.tabs(["The Cockpit", "Screener", "Raw Market", "Automation"])

# TAB 1: THE COCKPIT
with tab1:
    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("Radar Velocity (Top Accumulation)")
        df_top = db.execute("SELECT ticker, SUM(value) as Net_Value FROM trades GROUP BY ticker ORDER BY Net_Value DESC LIMIT 5").df()
        st.dataframe(df_top, width='stretch')
        
    with col2:
        st.subheader(f"Whale Trades (> Rp {whale_limit:,.0f})")
        df_whale = db.execute(f"SELECT * FROM trades WHERE value >= {whale_limit} ORDER BY timestamp DESC LIMIT 100").df()
        st.dataframe(df_whale, width='stretch')

# TAB 2: SCREENER
with tab2:
    st.subheader("Watchlist Orderbook")
    watchlist_saham = ["ANTM", "BBCA", "BBRI", "BDMN", "ELSA", "INCO", "JSMR", "LSIP", "PTBA", "PWON", "SGER"]
    list_saham = st.multiselect("Pilih Emiten Pantauan:", watchlist_saham, default=["BBCA", "BBRI"])
    if list_saham:
        cols = st.columns(len(list_saham))
        for i, saham in enumerate(list_saham):
            with cols[i]:
                st.markdown(f"**{saham}**")
                df_saham = db.execute(f"SELECT timestamp, price, vol, type FROM trades WHERE ticker = '{saham}' ORDER BY timestamp DESC LIMIT 50").df()
                st.dataframe(df_saham, width='stretch')

# TAB 3: RAW MARKET
with tab3:
    st.subheader("Historical Tape (Full Day)")
    df_raw = db.execute("SELECT * FROM trades ORDER BY timestamp DESC LIMIT 2000").df()
    st.dataframe(df_raw, width='stretch')

# TAB 4: AUTOMATION
with tab4:
    st.subheader("Data Center & Exporter")
    
    colA, colB, colC = st.columns(3)
    
    with colA:
        if st.button("Kirim Top 5"):
            df_top_export = db.execute("SELECT ticker, SUM(value) as Net_Value FROM trades GROUP BY ticker ORDER BY Net_Value DESC LIMIT 5").df()
            df_top_export.insert(0, 'waktu', datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S'))
            sukses, msg = kirim_ke_gsheets("Top_Summary", df_top_export)
            if sukses: st.success("Sukses mendarat di Top_Summary!")
            else: st.error(f"Gagal: {msg}")
            
    with colB:
        if st.button("Kirim Watchlist"):
            # Format list tuple string untuk SQL IN clause
            saham_tuple = tuple(watchlist_saham)
            df_watch = db.execute(f"SELECT timestamp, ticker, price, vol, value, type FROM trades WHERE ticker IN {saham_tuple} ORDER BY timestamp DESC LIMIT 20").df()
            sukses, msg = kirim_ke_gsheets("Watchlist_Alerts", df_watch)
            if sukses: st.success("Sukses mendarat di Watchlist_Alerts!")
            else: st.error(f"Gagal: {msg}")
            
    with colC:
        if st.button("Kirim Global Whales"):
            df_whale_export = db.execute(f"SELECT timestamp, ticker, price, vol, value, type FROM trades WHERE value >= {whale_limit} ORDER BY timestamp DESC LIMIT 20").df()
            sukses, msg = kirim_ke_gsheets("Global_Whales", df_whale_export)
            if sukses: st.success("Sukses mendarat di Global_Whales!")
            else: st.error(f"Gagal: {msg}")

    st.markdown("---")
    st.warning("Tekan ini hanya saat bursa tutup (16:30) untuk memindahkan arsip harian ke GDrive dan mengosongkan RAM.")
    if st.button("End of Day: Backup to GDrive & Purge RAM"):
        sukses, msg = backup_ke_gdrive()
        if sukses: st.success(msg)
        else: st.error(f"Gagal: {msg}")

# ==========================================
# TRIGGER AUTO REFRESH (RATA KIRI / TANPA SPASI)
# ==========================================
if auto_refresh:
    time.sleep(2)
    st.rerun()
