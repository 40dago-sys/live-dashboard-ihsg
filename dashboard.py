import streamlit as st
import websocket
import json
import pandas as pd
import duckdb
import requests
import threading
import time
from datetime import datetime
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import tempfile
import os

# ==========================================
# BLOK 1: KONFIGURASI & DATABASE (DUCKDB)
# ==========================================
st.set_page_config(page_title="Institutional Tape Reading", layout="wide")
WIB = pytz.timezone('Asia/Jakarta')

@st.cache_resource
def init_db():
    # Menggunakan DuckDB in-memory database
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
    return con

db = init_db()

# ==========================================
# BLOK 2: ENGINE WEBSOCKET (BACKGROUND THREAD)
# ==========================================
@st.cache_resource
def start_wss_thread():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            # Adaptasi sesuai struktur JSON Arjum API Bapak
            if 'data' in data:
                trade = data['data']
                ts = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
                ticker = trade.get('code', '')
                price = float(trade.get('price', 0))
                vol = int(trade.get('volume', 0))
                val = float(trade.get('value', 0))
                type_action = trade.get('type', 'UNKNOWN') # Buy/Sell
                
                # Insert langsung ke DuckDB
                db.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?)", 
                           (ts, ticker, price, vol, val, type_action))
        except Exception as e:
            pass

    def on_error(ws, error):
        pass

    def run_ws():
        ws_url = "wss://stream.arjum.com/..." # Ganti dengan URL WSS Arjum Bapak
        ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error)
        while True:
            ws.run_forever()
            time.sleep(3) # Auto-reconnect jika putus

    t = threading.Thread(target=run_ws, daemon=True)
    t.start()
    return t

# Jalankan WSS di background
start_wss_thread()

# ==========================================
# BLOK 3: JEMBATAN GOOGLE (SHEETS & DRIVE)
# ==========================================
def kirim_ke_gsheets(kategori, dataframe):
    url = st.secrets["WEBHOOK_URL"]
    # Ubah DF ke list of lists
    data_list = dataframe.values.tolist()
    payload = {
        "kategori": kategori,
        "data": data_list
    }
    try:
        res = requests.post(url, json=payload)
        return True, res.text
    except Exception as e:
        return False, str(e)

def backup_ke_gdrive():
    try:
        # Tarik semua data hari ini
        df_all = db.execute("SELECT * FROM trades").df()
        if df_all.empty:
            return False, "Tidak ada data untuk dibackup."

        # Simpan ke CSV sementara
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"backup_trades_{datetime.now(WIB).strftime('%Y%m%d_%H%M')}.csv")
        df_all.to_csv(file_path, index=False)

        # Autentikasi GDrive via Secrets
        creds_json = json.loads(st.secrets["GCP_JSON"])
        creds = service_account.Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)

        # Upload File
        folder_id = st.secrets["GDRIVE_FOLDER_ID"]
        file_metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}
        media = MediaFileUpload(file_path, mimetype='text/csv')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        # Bersihkan memori harian
        db.execute("DELETE FROM trades")
        return True, f"Sukses! File ID: {file.get('id')}"
    except Exception as e:
        return False, str(e)

# ==========================================
# BLOK 4: USER INTERFACE (4 TABS)
# ==========================================
st.title("Arjum Institutional Terminal")

# Soft-Code Variables di Sidebar
st.sidebar.header("Control Panel")
whale_limit = st.sidebar.number_input("Batas Paus (Rp)", value=50000000, step=10000000)
top_interval = st.sidebar.selectbox("Interval Top 5", ["5 Menit", "15 Menit", "30 Menit", "1 Jam"])
auto_refresh = st.sidebar.checkbox("Auto Refresh 2s", value=True)

if auto_refresh:
    time.sleep(2)
    st.rerun()

tab1, tab2, tab3, tab4 = st.tabs(["The Cockpit", "Screener", "Raw Market", "Automation"])

# TAB 1: THE COCKPIT
with tab1:
    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("Radar Velocity (Top Accumulation)")
        # Hitung Net Buy (Bisa disesuaikan dengan logika exact arjum: Buy - Sell)
        df_top = db.execute(f"SELECT ticker, SUM(value) as Net_Value FROM trades GROUP BY ticker ORDER BY Net_Value DESC LIMIT 5").df()
        st.dataframe(df_top, use_container_width=True)
        
    with col2:
        st.subheader(f"Whale Trades (> Rp {whale_limit:,.0f})")
        df_whale = db.execute(f"SELECT * FROM trades WHERE value >= {whale_limit} ORDER BY timestamp DESC LIMIT 100").df()
        st.dataframe(df_whale, use_container_width=True)

# TAB 2: SCREENER
with tab2:
    st.subheader("Watchlist Orderbook")
    list_saham = st.multiselect("Pilih Emiten Pantauan:", ["BBCA", "BBRI", "BMRI", "BREN", "AMMN", "PGAS"])
    if list_saham:
        cols = st.columns(len(list_saham))
        for i, saham in enumerate(list_saham):
            with cols[i]:
                st.markdown(f"**{saham}**")
                df_saham = db.execute(f"SELECT timestamp, price, vol, type FROM trades WHERE ticker = '{saham}' ORDER BY timestamp DESC LIMIT 50").df()
                st.dataframe(df_saham, use_container_width=True)

# TAB 3: RAW MARKET
with tab3:
    st.subheader("Historical Tape (Full Day)")
    df_raw = db.execute("SELECT * FROM trades ORDER BY timestamp DESC LIMIT 2000").df()
    st.dataframe(df_raw, use_container_width=True)

# TAB 4: AUTOMATION
with tab4:
    st.subheader("Data Center & Exporter")
    
    if st.button("Kirim Top 5 ke GSheets"):
        df_top_export = db.execute("SELECT ticker, SUM(value) FROM trades GROUP BY ticker ORDER BY SUM(value) DESC LIMIT 5").df()
        # Tambahkan kolom waktu agar di excel ketahuan jam berapa
        df_top_export.insert(0, 'waktu', datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S'))
        sukses, msg = kirim_ke_gsheets("Top_Summary", df_top_export)
        if sukses:
            st.success("Terkirim ke Tab Top_Summary di GSheets!")
        else:
            st.error(f"Gagal: {msg}")

    st.markdown("---")
    st.warning("Tekan ini hanya saat bursa tutup (16:30) untuk memindahkan arsip harian dan mengosongkan RAM.")
    if st.button("End of Day: Backup to GDrive & Purge RAM"):
        sukses, msg = backup_ke_gdrive()
        if sukses:
            st.success(msg)
        else:
            st.error(f"Gagal: {msg}")
