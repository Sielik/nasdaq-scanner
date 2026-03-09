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
PRESCAN_THRESHOLD = 1.5  # Powrót do 1.5
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
        if st.button("🔍 TEST - AAPL"):
            with st.spinner("Sprawdzam AAPL..."):
                try:
                    ticker = stock.Ticker('AAPL')
                    df = ticker.nasdaq.hist_quotes_stock
                    
                    # NAPRAWA
                    for col in df.columns:
                        if col != 'Date':
                            df[col] = df[col].astype(str).str.replace('$', '', regex=False)
                            df[col] = df[col].str.replace(',', '', regex=False)
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    df = df.sort_values('Date', ascending=False).head(10)
                    
                    volumes = df['Volume'].values
                    avg_vol = np.mean(volumes[1:])
                    today_vol = volumes[0]
                    rvol = today_vol / avg_vol if avg_vol > 0 else 0
                    
                    st.success(f"✅ RVOL = {rvol:.2f}")
                    
                except Exception as e:
                    st.error(f"Błąd: {e}")
    
    with col2:
        if st.button("📊 TEST - MSFT"):
            with st.spinner("Sprawdzam MSFT..."):
                try:
                    ticker = stock.Ticker('MSFT')
                    df = ticker.nasdaq.hist_quotes_stock
                    
                    if df is not None:
                        st.write(f"MSFT: {len(df)} wierszy")
                    else:
                        st.error("Brak danych")
                except Exception as e:
                    st.error(f"Błąd: {e}")
    
    with col3:
        if st.button("🌐 TEST - połączenie"):
            try:
                ticker = stock.Ticker('AAPL')
                df = ticker.nasdaq.hist_quotes_stock
                if df is not None:
                    st.success(f"✅ Dane pobrane! Wiersze: {len(df)}")
                else:
                    st.error("❌ Brak danych")
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
# FUNKCJA POBIERANIA DANYCH
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
        
        # NAPRAWA FORMATOWANIA
        for col in df.columns:
            if col != 'Date':
                df[col] = df[col].astype(str).str.replace('$', '', regex=False)
                df[col] = df[col].str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna()
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
    FAZA 1: Szybkie sprawdzenie RVOL (próg 1.5)
    """
    try:
        df = get_stockhero_data(ticker)
        
        if df is None or len(df) < 10:
            return 0
        
        df['Date'] = pd.to_datetime(df['Date'])
        
        # ODFILTRUJ WEEKENDY
        df = df[df['Date'].dt.dayofweek < 5]
        df = df.sort_values('Date').reset_index(drop=True)
        
        if len(df) < 10:
            return 0
        
        # Pobierz wolumeny (ostatnie 10 DNI ROBOCZYCH)
        volumes = df['Volume'].values[-10:]
        avg_vol = np.mean(volumes[:-1])
        today_vol = volumes[-1]
        
        return today_vol / avg_vol if avg_vol > 0 else 0
        
    except Exception as e:
        return 0

# ============================================
# FAZA 1: PRESCAN
# ============================================

def prescan_all_tickers(tickers):
    """
    FAZA 1: Prescan wszystkich spółek (RVOL > 1.5)
    """
    st.markdown('<div class="phase-indicator">🔍 FAZA 1: Prescan (RVOL > 1.5)</div>', 
                unsafe_allow_html=True)
    
    promising_tickers = []
    promising_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    found_counter = 0
    
    # Skanuj wszystkie spółki
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(quick_rvol_check, t): t for t in tickers}
        
        for i, future in enumerate(as_completed(futures)):
            ticker = futures[future]
            try:
                rvol = future.result(timeout=15)
                if rvol > PRESCAN_THRESHOLD:
                    found_counter += 1
                    promising_tickers.append(ticker)
                    promising_data.append({
                        'Ticker': ticker,
                        'RVOL (prescan)': round(rvol, 2),
                        'Status': 'Do głębokiego skanowania'
                    })
            except:
                pass
            
            if i % 50 == 0:
                progress_bar.progress(i / len(tickers))
                status_text.text(f"Prescan: {i}/{len(tickers)} | Znaleziono: {found_counter}")
    
    progress_bar.empty()
    status_text.empty()
    
    if promising_data:
        st.success(f"✅ FAZA 1: Znaleziono {len(promising_data)} obiecujących spółek")
    else:
        st.warning("⚠️ Brak spółek w prescanie")
    
    return promising_tickers, promising_data

# ============================================
# FAZA 2: GŁĘBOKIE SKANOWANIE
# ============================================

def deep_scan_ticker(ticker):
    """
    FAZA 2: Głębokie skanowanie z pełnymi wskaźnikami
    """
    try:
        df = get_stockhero_data(ticker)
        
        if df is None or len(df) < 25:
            return None
        
        df['Date'] = pd.to_datetime(df['Date'])
        
        # ODFILTRUJ WEEKENDY
        df = df[df['Date'].dt.dayofweek < 5]
        df = df.sort_values('Date').reset_index(drop=True)
        
        if len(df) < 20:
            return None
        
        # RVOL
        avg_volume = df['Volume'].tail(20).mean()
        
        rvol_values = []
        for i in range(1, 6):
            if len(df) >= i:
                vol = df['Volume'].iloc[-i]
                rvol = vol / avg_volume if avg_volume > 0 else 0
                rvol_values.append(rvol)
        
        today_rvol = rvol_values[0] if rvol_values else 0
        last_4_rvol = rvol_values[1:5] if len(rvol_values) >= 5 else []
        days_over_2 = sum(1 for r in last_4_rvol if r > RVOL_THRESHOLD)
        
        rvol_ok = (today_rvol > RVOL_THRESHOLD) and (days_over_2 >= 2)
        
        # Jeśli nie spełnia RVOL, odrzuć
        if not rvol_ok:
            return None
        
        # OBV
        obv = [0]
        for i in range(1, len(df)):
            if df['Close'].iloc[i] > df['Close'].iloc[i-1]:
                obv.append(obv[-1] + df['Volume'].iloc[i])
            elif df['Close'].iloc[i] < df['Close'].iloc[i-1]:
                obv.append(obv[-1] - df['Volume'].iloc[i])
            else:
                obv.append(obv[-1])
        
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
        
        flow_ok = obv_ok and ad_ok and cmf_ok
        
        # Zmiany
        change_1d = ((df['Close'].iloc[-1] / df['Close'].iloc[-2] - 1) * 100) if len(df) >= 2 else 0
        change_5d = ((df['Close'].iloc[-1] / df['Close'].iloc[-5] - 1) * 100) if len(df) >= 5 else 0
        
        return {
            'Ticker': ticker,
            'Cena': round(df['Close'].iloc[-1], 2),
            'Wolumen': int(df['Volume'].iloc[-1]),
            'RVOL': round(today_rvol, 2),
            'Dni>2': days_over_2,
            'RVOL OK': '✅' if rvol_ok else '❌',
            'OBV': round(obv_slope, 2),
            'A/D': round(ad_slope, 2),
            'CMF': round(cmf, 3),
            'Flow OK': '✅' if flow_ok else '❌',
            'Zmiana 1d': round(change_1d, 2),
            'Zmiana 5d': round(change_5d, 2),
            'Data': datetime.now().strftime('%H:%M')
        }
        
    except Exception as e:
        return None

def deep_scan_promising(promising_tickers):
    """
    FAZA 2: Głębokie skanowanie obiecujących spółek
    """
    st.markdown('<div class="phase-indicator">🔬 FAZA 2: Głębokie skanowanie</div>', 
                unsafe_allow_html=True)
    
    if not promising_tickers:
        return []
    
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(deep_scan_ticker, t): t for t in promising_tickers}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                results.append(result)
            
            if i % 10 == 0:
                progress_bar.progress((i + 1) / len(promising_tickers))
                status_text.text(f"Skanowanie: {i+1}/{len(promising_tickers)} | Znaleziono: {len(results)}")
    
    progress_bar.empty()
    status_text.empty()
    
    return results

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
# GŁÓWNA FUNKCJA
# ============================================

def intelligent_scan(force_refresh=False):
    """
    GŁÓWNA FUNKCJA - dwuetapowe skanowanie
    """
    with st.spinner("📡 Pobieranie listy spółek..."):
        all_tickers = get_live_nasdaq_tickers()
    
    # Sprawdź cache
    if not force_refresh:
        cached = load_from_cache()
        if cached:
            cache_age = datetime.now() - cached['timestamp']
            if cache_age.total_seconds() / 3600 < CACHE_MAX_AGE_HOURS:
                st.info(f"📦 Używam cache sprzed {cache_age.seconds//60} minut")
                return cached['data'], [], all_tickers
    
    # FAZA 1: Prescan
    promising_tickers, promising_data = prescan_all_tickers(all_tickers)
    
    # FAZA 2: Głębokie skanowanie
    deep_results = deep_scan_promising(promising_tickers)
    
    # Zapisz do cache
    save_to_cache(deep_results, all_tickers)
    
    return deep_results, promising_data, all_tickers

# ============================================
# SIDEBAR
# ============================================

with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry")
    
    st.markdown("---")
    st.markdown('<span class="free-badge">💰 DARMOWE - StockHero</span>', unsafe_allow_html=True)
    
    st.subheader("📊 RVOL")
    use_rvol = st.checkbox("Filtruj RVOL >2 (2/4 dni)", value=True)
    
    st.subheader("💰 Przepływ")
    use_flow = st.checkbox("Filtruj OBV/A/D/CMF >0", value=True)
    
    st.subheader("💰 Cena")
    min_price = st.number_input("Min cena ($)", 0.0, 1000.0, 1.0, 0.5)
    max_price = st.number_input("Max cena ($)", 0.0, 10000.0, 500.0, 10.0)
    
    if st.button("🚀 SKANUJ", type="primary", use_container_width=True):
        with st.spinner("Skanowanie..."):
            deep_results, prescan_data, tickers = intelligent_scan(False)
        
        if deep_results or prescan_data:
            st.success("Skanowanie zakończone!")
        else:
            st.warning("Brak wyników")
    
    if st.button("🧹 Wyczyść cache"):
        if clear_cache():
            st.success("Cache wyczyszczony!")

# ============================================
# WYNIKI (wyświetlane po skanowaniu)
# ============================================

if 'deep_results' in locals() and deep_results:
    df_deep = pd.DataFrame(deep_results)
    
    # Filtry
    if min_price > 0:
        df_deep = df_deep[df_deep['Cena'] >= min_price]
    if max_price < 10000:
        df_deep = df_deep[df_deep['Cena'] <= max_price]
    if use_rvol:
        df_deep = df_deep[df_deep['RVOL OK'] == '✅']
    if use_flow:
        df_deep = df_deep[df_deep['Flow OK'] == '✅']
    
    st.subheader(f"📋 Wyniki ({len(df_deep)} spółek)")
    st.dataframe(df_deep.sort_values('RVOL', ascending=False))