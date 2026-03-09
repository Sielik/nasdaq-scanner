import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import time
import os
import requests
import pickle
import gzip
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

# Konfiguracja strony
st.set_page_config(
    page_title="NASDAQ StockHero Scanner",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
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
    .stat-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .phase-indicator {
        background-color: #f0f2f6;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
        text-align: center;
        font-weight: bold;
    }
    .free-badge {
        background-color: #28a745;
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: bold;
        display: inline-block;
    }
</style>
""", unsafe_allow_html=True)

# Stałe
CACHE_FILE = "nasdaq_stockhero_cache.gz"
RVOL_THRESHOLD = 2.0
PRESCAN_THRESHOLD = 3.0  # TEST: szukamy RVOL > 3
MAX_WORKERS = 20
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

# ============================================
# TESTY
# ============================================

st.markdown('<h1 class="main-header">📊 NASDAQ StockHero Scanner</h1>', unsafe_allow_html=True)

if not STOCKHERO_AVAILABLE:
    st.error("❌ StockHero nie jest zainstalowane!")
    st.stop()

with st.expander("🔧 TESTY", expanded=True):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 TEST - RVOL dla AAPL"):
            with st.spinner("Sprawdzam AAPL..."):
                try:
                    ticker = stock.Ticker('AAPL')
                    df = ticker.nasdaq.hist_quotes_stock
                    
                    if df is not None and len(df) > 10:
                        # Naprawa danych
                        df['Volume'] = df['Volume'].astype(str).str.replace(',', '').astype(float)
                        df['Close'] = df['Close/Last'].astype(str).str.replace(',', '').astype(float)
                        
                        # Weź ostatnie 10 dni
                        df = df.sort_values('Date', ascending=False).head(10)
                        
                        volumes = df['Volume'].values
                        avg_vol = np.mean(volumes[1:])
                        today_vol = volumes[0]
                        rvol = today_vol / avg_vol if avg_vol > 0 else 0
                        
                        st.success(f"✅ RVOL = {rvol:.2f}")
                        st.write(f"Średni wolumen (9d): {avg_vol:.0f}")
                        st.write(f"Dzisiejszy wolumen: {today_vol:.0f}")
                        st.dataframe(df[['Date', 'Volume']].head())
                    else:
                        st.error("Brak danych")
                except Exception as e:
                    st.error(f"Błąd: {e}")

# ============================================
# FUNKCJE POBIERANIA LISTY SPÓŁEK
# ============================================

@st.cache_data(ttl=3600, show_spinner=False)
def get_live_nasdaq_tickers():
    """Pobiera AKTUALNĄ listę spółek NASDAQ"""
    
    status = st.sidebar.empty()
    status.info("📡 Pobieranie listy spółek...")
    
    # Źródło 1: NASDAQ Trader
    try:
        url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
        df = pd.read_csv(url, sep='|')
        
        stocks = df[
            (df['NASDAQ Symbol'].notna()) & 
            (df['ETF'] == 'N') & 
            (df['TEST ISSUE'] == 'N')
        ]
        
        tickers = stocks['NASDAQ Symbol'].tolist()
        tickers = [t.strip() for t in tickers if t.strip()]
        tickers = sorted(list(set(tickers)))
        
        status.success(f"✅ {len(tickers)} spółek")
        return tickers
        
    except Exception as e:
        status.warning("⚠️ Używam backupu...")
    
    # Źródło 2: GitHub
    try:
        backup_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt"
        response = requests.get(backup_url, timeout=3)
        if response.status_code == 200:
            tickers = response.text.strip().split('\n')
            tickers = [t.strip().upper() for t in tickers if t.strip()]
            status.success(f"✅ {len(tickers)} spółek (backup)")
            return tickers
    except:
        pass
    
    return get_fallback_tickers()

@st.cache_data(ttl=86400, show_spinner=False)
def get_fallback_tickers():
    """Lista awaryjna"""
    return ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "TSLA", "NFLX"]

# ============================================
# NAPRAWIONA FUNKCJA POBIERANIA DANYCH
# ============================================

def get_stockhero_data(ticker):
    """
    Pobiera dane ze StockHero i naprawia formatowanie
    """
    try:
        ticker_obj = stock.Ticker(ticker)
        df = ticker_obj.nasdaq.hist_quotes_stock
        
        if df is None or len(df) < 25:
            return None
        
        # Naprawa danych - usuń przecinki i konwertuj na float
        df['Volume'] = df['Volume'].astype(str).str.replace(',', '').astype(float)
        df['Close'] = df['Close/Last'].astype(str).str.replace(',', '').astype(float)
        df['Open'] = df['Open'].astype(str).str.replace(',', '').astype(float)
        df['High'] = df['High'].astype(str).str.replace(',', '').astype(float)
        df['Low'] = df['Low'].astype(str).str.replace(',', '').astype(float)
        
        df = df.sort_values('Date').reset_index(drop=True)
        df['Ticker'] = ticker
        
        return df
        
    except Exception as e:
        return None

# ============================================
# FUNKCJA QUICK RVOL CHECK (PRESCAN)
# ============================================

def quick_rvol_check(ticker):
    """
    TEST: Szuka spółek z RVOL > 3 w ostatnich 4 dniach
    """
    try:
        df = get_stockhero_data(ticker)
        
        if df is None or len(df) < 10:
            return 0
        
        # Konwersja Date na datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # ODFILTRUJ WEEKENDY
        df = df[df['Date'].dt.dayofweek < 5]
        df = df.sort_values('Date').reset_index(drop=True)
        
        if len(df) < 5:
            return 0
        
        # Średnia wolumenu z 20 dni
        avg_volume = df['Volume'].tail(20).mean()
        
        if avg_volume == 0:
            return 0
        
        # Sprawdź ostatnie 5 dni roboczych
        max_rvol = 0
        for i in range(1, 6):
            if len(df) >= i:
                vol = df['Volume'].iloc[-i]
                rvol = vol / avg_volume
                if rvol > max_rvol:
                    max_rvol = rvol
        
        # Dla testu - pokaż w konsoli (opcjonalnie)
        if max_rvol > 0:
            print(f"{ticker}: max RVOL = {max_rvol:.2f}")
        
        # Jeśli max RVOL > 3, zwróć go
        if max_rvol > PRESCAN_THRESHOLD:
            return max_rvol
        
        return 0
        
    except Exception as e:
        return 0

# ============================================
# FUNKCJE CACHE
# ============================================

def save_to_cache(data, tickers):
    """Zapisuje cache"""
    try:
        cache = {
            'timestamp': datetime.now(),
            'tickers': tickers,
            'data': data
        }
        
        json_str = json.dumps(cache, default=str)
        compressed = gzip.compress(json_str.encode())
        
        with open(CACHE_FILE, 'wb') as f:
            f.write(compressed)
        
        return True
    except:
        return False

def load_from_cache():
    """Wczytuje cache"""
    if not os.path.exists(CACHE_FILE):
        return None
    
    try:
        with open(CACHE_FILE, 'rb') as f:
            compressed = f.read()
        
        json_str = gzip.decompress(compressed).decode()
        cache = json.loads(json_str)
        cache['timestamp'] = datetime.fromisoformat(cache['timestamp'])
        return cache
    except:
        return None

def clear_cache():
    """Czyści cache"""
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        return True
    return False

# ============================================
# PRESCAN
# ============================================

def prescan_all_tickers(tickers):
    """
    TEST: Prescan szukający spółek z RVOL > 3
    """
    st.markdown('<div class="phase-indicator">🔍 TEST: Szukam spółek z RVOL > 3 w ostatnich 4 dniach</div>', 
                unsafe_allow_html=True)
    
    promising_tickers = []
    promising_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    found_counter = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(quick_rvol_check, t): t for t in tickers[:500]}  # Tylko 500 na test
        
        for i, future in enumerate(as_completed(futures)):
            ticker = futures[future]
            try:
                max_rvol = future.result(timeout=15)
                if max_rvol > 0:
                    found_counter += 1
                    promising_tickers.append(ticker)
                    promising_data.append({
                        'Ticker': ticker,
                        'Max RVOL': round(max_rvol, 2),
                        'Status': 'Znaleziony!'
                    })
            except:
                pass
            
            if i % 10 == 0:
                progress_bar.progress(i / min(500, len(tickers)))
                status_text.text(f"Prescan: {i}/500 | Znaleziono: {found_counter}")
    
    progress_bar.empty()
    status_text.empty()
    
    if promising_data:
        df_result = pd.DataFrame(promising_data)
        st.success(f"✅ Znaleziono {len(promising_data)} spółek z RVOL > 3!")
        st.dataframe(df_result.sort_values('Max RVOL', ascending=False))
    else:
        st.error("❌ Nie znaleziono żadnej spółki z RVOL > 3")
    
    return promising_tickers, promising_data

# ============================================
# GŁÓWNA FUNKCJA
# ============================================

def intelligent_scan(force_refresh=False):
    """TEST: tylko prescan"""
    
    with st.spinner("📡 Pobieranie listy spółek..."):
        all_tickers = get_live_nasdaq_tickers()
    
    promising_tickers, promising_data = prescan_all_tickers(all_tickers)
    
    return [], promising_data, all_tickers

# ============================================
# SIDEBAR
# ============================================

with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 TEST")
    
    st.markdown("---")
    st.markdown('<span class="free-badge">💰 TEST: RVOL > 3</span>', unsafe_allow_html=True)
    st.caption("Szukam spółek z RVOL > 3 w ostatnich 4 dniach")
    
    if st.button("🚀 TESTUJ", type="primary", use_container_width=True):
        with st.spinner("Testowanie..."):
            results, prescan, tickers = intelligent_scan(True)
        
        if prescan:
            st.success(f"Znaleziono {len(prescan)} spółek!")
        else:
            st.error("Nie znaleziono")

# ============================================
# STOPKA
# ============================================

st.markdown("---")
st.caption("Tryb testowy - szukam RVOL > 3")