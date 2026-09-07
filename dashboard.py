import streamlit as st
import pandas as pd
import json
import threading
import time
from websocket import create_connection
from collections import deque

# ==========================================
# 1. PENGATURAN HALAMAN & MEMORI GLOBAL (CLOUD SAFE)
# ==========================================
st.set_page_config(page_title="Whale Tracker IHSG", layout="wide")

@st.cache_resource
def get_global_memory():
    return deque(maxlen=500) # Batas 500 baris agar RAM aman

@st.cache_resource
def start_wss_worker():
    memory = get_global_memory()
    
    def wss_listener():
        uri = "wss://stock.arjum.com/ws/running-trade"
        api_key = "sk_live_WI0TxPlSGJ_rMZyJfzAkXIihSJsFD72QkKTNnFVo16E"
        headers = [f"X-API-Key: {api_key}", "User-Agent: Mozilla/5.0"]
        
        while True: # Auto-Reconnect Loop
            try:
                ws = create_connection(uri, header=headers)
                while True:
                    result = ws.recv()
                    data = json.loads(result)
                    memory.appendleft(data)
            except Exception as e:
                print(f"Koneksi WSS Terputus, mencoba ulang dalam 3 detik... Error: {e}")
                time.sleep(3)
                
    worker = threading.Thread(target=wss_listener, daemon=True)
    worker.start()
    return worker

trade_data = get_global_memory()
start_wss_worker()

# ==========================================
# 2. MODUL UI (TAMPILAN DEPAN)
# ==========================================
def render_sidebar():
    st.sidebar.title("⚙️ Kendali Paus")
    st.sidebar.markdown("---")
    
    whale_limit_jt = st.sidebar.slider("Batas Whale (Juta Rp)", min_value=10, max_value=1000, value=100, step=10)
    whale_limit = whale_limit_jt * 1_000_000 
    
    auto_refresh = st.sidebar.checkbox("🟢 Live Auto-Refresh", value=True)
    
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Total Data Terkumpul di Server: {len(trade_data)} baris")
    return whale_limit, auto_refresh

def render_running_trade(whale_limit):
    st.subheader(f"🌊 Smart Running Trade (> Rp {whale_limit/1_000_000:,.0f} Juta)")
    
    raw_list = list(trade_data)
    
    if len(raw_list) == 0:
        st.info("Menunggu transaksi masuk dari Arjum...")
        return

    st.write("🔍 **Log Data Mentah (Inspeksi struktur JSON):**")
    st.json(raw_list[:3])

# ==========================================
# 3. MESIN UTAMA
# ==========================================
def main():
    whale_limit, auto_refresh = render_sidebar()
    st.title("📈 Tape Reading Institusional - Online Pilot")
    
    render_running_trade(whale_limit)
    
    if auto_refresh:
        time.sleep(1)
        st.rerun()

if __name__ == "__main__":
    main()
