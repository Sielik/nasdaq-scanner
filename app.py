import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
import requests
import gzip
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Konfiguracja strony
st.set_page_config(
    page_title="NASDAQ Scanner",
    page_icon="📊",
    layout="centered"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #00A3E0;
        text-align: center;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    .stats-box {
        background-color: #e8f4fd;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 1rem 0;
        font-size: 1.2rem;
        font-weight: bold;
    }
    .stButton button {
        min-width: 150px;
        font-size: 1.1rem;
    }
</style>
""", unsafe_allow_html=True)

# Stałe
CACHE_FILE = "nasdaq_cache.gz"
RVOL_THRESHOLD = 2.0
PRESCAN_THRESHOLD = 2.0
MAX_WORKERS = 30
TIMEOUT_SECONDS = 10
CACHE_MAX_AGE_HOURS = 12

# ============================================
# IMPORT STOCKHERO
# ============================================
try:
    import StockHero as stock
    STOCKHERO_AVAILABLE = True
except ImportError:
    STOCKHERO_AVAILABLE = False
    st.error("⚠️ Zainstaluj StockHero: pip install StockHero")
    st.stop()

# ============================================
# NAGŁÓWEK
# ============================================
st.markdown('<h1 class="main-header">📊 NASDAQ Stock Scanner</h1>', unsafe_allow_html=True)

# ============================================
# PRZYCISKI
# ============================================
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    scan_button = st.button("🚀 ROZPOCZNIJ SKANOWANIE", type="primary", use_container_width=True)
with col2:
    stop_button = st.button("⏹️ ZATRZYMAJ", use_container_width=True)
with col3:
    if st.button("🧹 WYCZYŚĆ CACHE", use_container_width=True):
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            st.success("Cache wyczyszczony!")
            st.rerun()

# ============================================
# FUNKCJE POMOCNICZE
# ============================================

@st.cache_data(ttl=3600)
def get_nasdaq_tickers():
    """Pobiera listę spółek NASDAQ"""
    try:
        url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
        df = pd.read_csv(url, sep='|')
        
        # Filtruj tylko akcje
        stocks = df[
            (df['NASDAQ Symbol'].notna()) & 
            (df['ETF'] == 'N')
        ]
        
        tickers = stocks['NASDAQ Symbol'].tolist()
        all_tickers = [t.strip() for t in tickers if t.strip() and len(t.strip()) <= 5]
        
        return all_tickers
    except:
        return ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "TSLA", "NFLX"]

def get_stock_data(ticker):
    """Pobiera dane ze StockHero"""
    try:
        ticker_obj = stock.Ticker(ticker)
        df = ticker_obj.nasdaq.hist_quotes_stock
        
        if df is None or len(df) < 25:
            return None
        
        # Naprawa formatowania
        for col in df.columns:
            if col != 'Date':
                df[col] = df[col].astype(str).str.replace('$', '', regex=False)
                df[col] = df[col].str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna()
        df = df.sort_values('Date').reset_index(drop=True)
        
        # Filtruj weekendy
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df['Date'].dt.dayofweek < 5]
        
        return df
    except:
        return None

def prescan_ticker(ticker):
    """Prescan - RVOL > 2 dzisiaj"""
    try:
        df = get_stock_data(ticker)
        if df is None or len(df) < 10:
            return None
        
        avg_vol = df['Volume'].tail(20).mean()
        if avg_vol == 0:
            return None
        
        today_vol = df['Volume'].iloc[-1]
        rvol = today_vol / avg_vol
        
        if rvol > PRESCAN_THRESHOLD:
            return {
                'Ticker': ticker,
                'RVOL': round(rvol, 2),
                'Cena': round(df['Close'].iloc[-1], 2)
            }
        return None
    except:
        return None

def deep_scan_ticker(ticker):
    """Głębokie skanowanie"""
    try:
        df = get_stock_data(ticker)
        if df is None or len(df) < 25:
            return None
        
        avg_volume = df['Volume'].tail(20).mean()
        if avg_volume == 0:
            return None
        
        # RVOL dla ostatnich 4 dni
        rvol_values = []
        for i in range(1, 5):
            if len(df) >= i:
                vol = df['Volume'].iloc[-i]
                rvol = vol / avg_volume
                rvol_values.append(rvol)
        
        days_over_2 = sum(1 for r in rvol_values if r > RVOL_THRESHOLD)
        rvol_ok = (days_over_2 >= 2)
        
        if not rvol_ok:
            return None
        
        # OBV
        obv = [0]
        for i in range(1, len(df)):
            if df['Close'].iloc[i] > df['Close'].iloc[i-1]:
                obv.append(obv[-1] + df['Volume'].iloc[i])
            else:
                obv.append(obv[-1] - df['Volume'].iloc[i])
        
        obv_slope = np.polyfit(range(20), obv[-20:], 1)[0] if len(obv) >= 20 else 0
        obv_ok = obv_slope > 0
        
        # A/D
        ad_line = [0]
        for i in range(1, len(df)):
            high, low, close = df['High'].iloc[i], df['Low'].iloc[i], df['Close'].iloc[i]
            if high != low:
                clv = ((close - low) - (high - close)) / (high - low)
            else:
                clv = 0
            ad_line.append(ad_line[-1] + (clv * df['Volume'].iloc[i]))
        
        ad_slope = np.polyfit(range(20), ad_line[-20:], 1)[0] if len(ad_line) >= 20 else 0
        ad_ok = ad_slope > 0
        
        # CMF
        def calculate_cmf(data, period=20):
            if len(data) < period:
                return 0
            mfv = []
            for i in range(-period, 0):
                high, low, close = data['High'].iloc[i], data['Low'].iloc[i], data['Close'].iloc[i]
                if high != low:
                    clv = ((close - low) - (high - close)) / (high - low)
                else:
                    clv = 0
                mfv.append(clv * data['Volume'].iloc[i])
            volume_sum = data['Volume'].iloc[-period:].sum()
            return sum(mfv) / volume_sum if volume_sum > 0 else 0
        
        cmf = calculate_cmf(df, 20)
        cmf_ok = cmf > 0
        
        return {
            'Ticker': ticker,
            'Cena': round(df['Close'].iloc[-1], 2),
            'RVOL': round(rvol_values[0], 2),
            'Dni>2': days_over_2,
            'OBV': '📈' if obv_ok else '📉',
            'A/D': '📈' if ad_ok else '📉',
            'CMF': round(cmf, 3),
            'Flow OK': '✅' if (obv_ok and ad_ok and cmf_ok) else '❌'
        }
    except:
        return None

def run_scan():
    """Główne skanowanie"""
    
    with st.spinner("Pobieranie listy spółek..."):
        tickers = get_nasdaq_tickers()
        st.info(f"📊 Znaleziono {len(tickers)} spółek")
    
    # PRESCAN
    st.markdown("---")
    st.markdown("### 🔍 FAZA 1: Prescan")
    st.markdown(f"*Kryterium: RVOL > {PRESCAN_THRESHOLD} dzisiaj*")
    
    prescan_results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(prescan_ticker, t): t for t in tickers}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                prescan_results.append(result)
            
            if i % 100 == 0:
                progress_bar.progress(i / len(tickers))
                status_text.text(f"Prescan: {i}/{len(tickers)} | Znaleziono: {len(prescan_results)}")
    
    progress_bar.empty()
    status_text.empty()
    
    if prescan_results:
        df_prescan = pd.DataFrame(prescan_results).sort_values('RVOL', ascending=False)
        st.markdown(f"<div class='stats-box'>✅ Znaleziono {len(df_prescan)} spółek</div>", unsafe_allow_html=True)
        st.dataframe(df_prescan, use_container_width=True)
    else:
        st.markdown("<div class='stats-box'>❌ Brak spółek w prescanie</div>", unsafe_allow_html=True)
        return
    
    # GŁĘBOKIE
    st.markdown("---")
    st.markdown("### 🔬 FAZA 2: Głębokie skanowanie")
    st.markdown("*Kryteria: RVOL > 2 w ≥2/4 dni + OBV/A/D/CMF > 0*")
    
    promising = [r['Ticker'] for r in prescan_results]
    deep_results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(deep_scan_ticker, t): t for t in promising}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                deep_results.append(result)
            
            if i % 10 == 0:
                progress_bar.progress((i + 1) / len(promising))
                status_text.text(f"Głębokie: {i+1}/{len(promising)} | Znaleziono: {len(deep_results)}")
    
    progress_bar.empty()
    status_text.empty()
    
    if deep_results:
        df_deep = pd.DataFrame(deep_results).sort_values('RVOL', ascending=False)
        st.markdown(f"<div class='stats-box'>✅ Znaleziono {len(df_deep)} spółek</div>", unsafe_allow_html=True)
        st.dataframe(df_deep, use_container_width=True)
        
        csv = df_deep.to_csv(index=False)
        st.download_button("📥 Pobierz CSV", csv, f"wyniki_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
    else:
        st.markdown("<div class='stats-box'>❌ Brak spółek w głębokim</div>", unsafe_allow_html=True)

# ============================================
# WYKONANIE
# ============================================
if scan_button:
    run_scan()

if stop_button:
    st.warning("⏹️ Zatrzymano")
    st.rerun()