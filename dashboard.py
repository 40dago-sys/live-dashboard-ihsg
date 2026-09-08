import streamlit as st
import pandas as pd
import json
import threading
import time
from websocket import create_connection
from collections import deque

# ==========================================
# 1. PENGATURAN HALAMAN & MEMORI GLOBAL
# ==========================================
st.set_page_config(page_title="Tape Reading Institusional", layout="wide")

@st.cache_resource
def get_global_memory():
    return deque(maxlen=500) 

@st.cache_resource
def start_wss_worker():
    memory = get_global_memory()
    
    def wss_listener():
        uri = "wss://stock.arjum.com/ws/running-trade"
        api_key = "sk_live_WI0TxPlSGJ_rMZyJfzAkXIihSJsFD72QkKTNnFVo16E"
        headers = [f"X-API-Key: {api_key}", "User-Agent: Mozilla/5.0"]
        
        while True: 
            try:
                ws = create_connection(uri, header=headers)
                while True:
                    result = ws.recv()
                    data = json.loads(result)
                    memory.appendleft(data)
            except Exception as e:
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
    st.sidebar.title("⚙️ Kendali Filter")
    st.sidebar.markdown("---")
    
    whale_limit_jt = st.sidebar.slider("Batas Normal (Juta Rp)", min_value=10, max_value=1000, value=100, step=10)
    whale_limit = whale_limit_jt * 1_000_000 
    
    super_whale_limit_m = st.sidebar.slider("Batas Super Whale (Miliar Rp)", min_value=1, max_value=10, value=2, step=1)
    super_whale_limit = super_whale_limit_m * 1_000_000_000
    
    auto_refresh = st.sidebar.checkbox("🟢 Live Auto-Refresh", value=True)
    
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Total Data di RAM Server: {len(trade_data)} baris")
    return whale_limit, super_whale_limit, auto_refresh

def format_rupiah(angka):
    if angka >= 1_000_000_000:
        return f"Rp {angka/1_000_000_000:.2f} M"
    elif angka >= 1_000_000:
        return f"Rp {angka/1_000_000:.0f} Jt"
    return f"Rp {angka:,.0f}"

def render_dashboard(whale_limit, super_whale_limit):
    raw_list = list(trade_data)
    
    if len(raw_list) == 0:
        st.info("Menunggu aliran transaksi dari API Arjum...")
        return

    # A. PROSES EKSTRAKSI DATA (ELT)
    processed_data = []
    for item in raw_list:
        if item.get("type") == "trade" and "data" in item:
            trade = item["data"]
            val = trade.get("v", 0)
            
            if val >= whale_limit:
                action_code = trade.get("c", "")
                action = "Buy" if action_code == "buy" else "Sell" if action_code == "sell" else action_code
                
                processed_data.append({
                    "Waktu": trade.get("a", ""),
                    "Ticker": trade.get("t", ""),
                    "Harga": trade.get("p", 0),
                    "Lot": trade.get("l", 0),
                    "Nilai (Rp)": val,
                    "Aksi": action
                })

    if not processed_data:
        st.warning(f"Belum ada transaksi di atas batas {format_rupiah(whale_limit)} di memori saat ini.")
        return

    df = pd.DataFrame(processed_data)

    # B. KALKULASI LIQUIDITY PRESSURE (METRIK)
    total_buy = df[df['Aksi'] == 'Buy']['Nilai (Rp)'].sum()
    total_sell = df[df['Aksi'] == 'Sell']['Nilai (Rp)'].sum()
    net_flow = total_buy - total_sell

    # C. KALKULASI RADAR VELOCITY (AGREGASI TICKER)
    # Menghitung net buy per emiten dalam rolling window
    radar_df = df.groupby('Ticker').apply(
        lambda x: pd.Series({
            'Net Value': x[x['Aksi'] == 'Buy']['Nilai (Rp)'].sum() - x[x['Aksi'] == 'Sell']['Nilai (Rp)'].sum(),
            'Freq': len(x)
        })
    ).reset_index().sort_values('Net Value', ascending=False)
    
    # Pisahkan Top Buy dan Top Sell untuk Radar
    top_buy_df = radar_df[radar_df['Net Value'] > 0].head(5).copy()
    top_buy_df['Net Value'] = top_buy_df['Net Value'].apply(format_rupiah)
    
    # D. TATA LETAK LAYAR (KIRI & KANAN)
    col_left, col_right = st.columns([1, 2.5])
    
    # --- PANEL KIRI: RADAR VELOCITY ---
    with col_left:
        st.subheader("🎯 Top 5 Accumulation")
        st.caption("Emiten dengan Net Buy tertinggi di layar saat ini.")
        if not top_buy_df.empty:
            st.dataframe(top_buy_df, use_container_width=True, hide_index=True)
        else:
            st.info("Belum ada akumulasi dominan.")
            
    # --- PANEL KANAN: METRIK & RUNNING TRADE ---
    with col_right:
        # Metrik Likuiditas
        m1, m2, m3 = st.columns(3)
        m1.metric("🔥 Total Buy", format_rupiah(total_buy))
        m2.metric("🩸 Total Sell", format_rupiah(total_sell))
        m3.metric("⚡ Net Flow", format_rupiah(net_flow))
        
        st.markdown("---")
        st.subheader(f"🌊 Smart Running Trade")
        
        # Siapkan tabel utama untuk ditampilkan
        display_df = df.copy()
        display_df['Harga'] = display_df['Harga'].apply(lambda x: f"{x:,.0f}")
        display_df['Lot'] = display_df['Lot'].apply(lambda x: f"{x:,.0f}")
        
        # Format nilai dan tandai Super Whale
        def highlight_super_whale(row):
            if row['Nilai (Rp)'] >= super_whale_limit:
                return ['background-color: rgba(255, 215, 0, 0.2)'] * len(row) # Warna emas transparan
            return [''] * len(row)
            
        def style_action_text(val):
            if val == 'Buy':
                return 'color: #00FF00; font-weight: bold;'
            elif val == 'Sell':
                return 'color: #FF4B4B; font-weight: bold;'
            return ''

        # Terapkan format angka khusus kolom nilai sebelum di-render
        styled_df = display_df.style\
            .format({'Nilai (Rp)': lambda x: format_rupiah(x)})\
            .apply(highlight_super_whale, axis=1)\
            .map(style_action_text, subset=['Aksi'])

        st.dataframe(styled_df, use_container_width=True, hide_index=True, height=400)

# ==========================================
# 3. MESIN UTAMA
# ==========================================
def main():
    whale_limit, super_whale_limit, auto_refresh = render_sidebar()
    st.title("📈 Tape Reading Institusional - Online Pilot")
    
    render_dashboard(whale_limit, super_whale_limit)
    
    if auto_refresh:
        time.sleep(1) 
        st.rerun()

if __name__ == "__main__":
    main()
