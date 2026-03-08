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
    page_title="NASDAQ Intelligent Scanner",
    page_icon="🧠",
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
</style>
""", unsafe_allow_html=True)

# Stałe
CACHE_FILE = "nasdaq_intelligent_cache.gz"
RVOL_THRESHOLD = 2.0
PRESCAN_THRESHOLD = 1.5  # Niższy próg dla prescanu
MAX_WORKERS_PRESCAN = 30  # Więcej wątków dla prescanu
MAX_WORKERS_DEEP = 20
TIMEOUT_SECONDS = 3  # Krótszy timeout dla prescanu
CACHE_MAX_AGE_HOURS = 12

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
    
    status.error("❌ Błąd - lista awaryjna")
    return get_fallback_tickers()

@st.cache_data(ttl=86400, show_spinner=False)
def get_fallback_tickers():
    """Lista awaryjna"""
    return [
        "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "INTC", "TSLA", "NFLX",
        "AMZN", "PEP", "COST", "CSCO", "ADBE", "CRM", "ORCL", "IBM", "QCOM",
        "TXN", "AVGO", "AMAT", "MU", "NXPI", "KLAC", "LRCX", "ASML", "SNPS",
        "CDNS", "ADI", "MCHP", "ON", "SWKS", "QRVO", "MPWR", "INTU", "NOW",
        "PANW", "FTNT", "CRWD", "ZS", "OKTA", "DDOG", "MDB", "SNOW", "PLTR",
        "GILD", "REGN", "VRTX", "MRNA", "ILMN", "BNTX", "ALNY", "BIIB", "AMGN"
    ]

# ============================================
# FUNKCJE CACHE - NAPRAWIONE!
# ============================================

def save_to_cache(data, tickers):
    """Zapisuje cache"""
    try:
        # Ogranicz rozmiar
        if len(data) > 2500:
            data = sorted(data, key=lambda x: x.get('Wolumen', 0), reverse=True)[:2500]
        
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
    except Exception as e:
        st.sidebar.error(f"Błąd zapisu cache: {e}")
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
    except Exception as e:
        st.sidebar.warning(f"Błąd odczytu cache: {e}")
        return None

def clear_cache():
    """Czyści cache bez restartu!"""
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        return True
    return False

# ============================================
# FUNKCJE SKANOWANIA - DWUETAPOWE
# ============================================

def quick_rvol_check(ticker):
    """
    FAZA 1: Bardzo szybkie sprawdzenie RVOL
    Tylko 10 dni danych, prosty timeout
    """
    try:
        url = f"https://stooq.pl/q/d/l/?s={ticker.lower()}.us&i=d"
        df = pd.read_csv(url, nrows=10)
        
        if df.empty or len(df) < 5:
            return 0
        
        # Uproszczone RVOL
        volumes = df['Wolumen'].values
        avg_vol = np.mean(volumes)
        today_vol = volumes[-1]
        
        return today_vol / avg_vol if avg_vol > 0 else 0
    except:
        return 0

def prescan_all_tickers(tickers):
    """
    FAZA 1: Prescan wszystkich spółek
    Znajduje obiecujące tickery (RVOL > PRESCAN_THRESHOLD)
    """
    st.markdown('<div class="phase-indicator">🔍 FAZA 1: Prescan wszystkich spółek</div>', 
                unsafe_allow_html=True)
    
    promising = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PRESCAN) as executor:
        futures = {executor.submit(quick_rvol_check, t): t for t in tickers}
        
        for i, future in enumerate(as_completed(futures)):
            ticker = futures[future]
            try:
                rvol = future.result(timeout=2)
                if rvol > PRESCAN_THRESHOLD:
                    promising.append(ticker)
            except:
                pass
            
            if i % 50 == 0:
                progress_bar.progress(i / len(tickers))
                status_text.text(f"Prescan: {i}/{len(tickers)} | Znaleziono: {len(promising)}")
    
    progress_bar.empty()
    status_text.empty()
    
    st.success(f"✅ FAZA 1: Znaleziono {len(promising)} obiecujących spółek")
    return promising

def deep_scan_ticker(ticker):
    """
    FAZA 2: Głębokie skanowanie pojedynczej spółki
    Pełne dane, wszystkie wskaźniki
    """
    try:
        url = f"https://stooq.pl/q/d/l/?s={ticker.lower()}.us&i=d"
        df = pd.read_csv(url)
        
        if df.empty or len(df) < 25:
            return None
        
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df = df.sort_values('Date').reset_index(drop=True)
        
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
        
        if not rvol_ok:  # Jeśli nie spełnia RVOL, nie licz reszty
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
            'OBV': '📈' if obv_ok else '📉',
            'A/D': '📈' if ad_ok else '📉',
            'CMF': round(cmf, 3),
            'Flow OK': '✅' if (obv_ok and ad_ok and cmf_ok) else '❌',
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
    
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DEEP) as executor:
        futures = {executor.submit(deep_scan_ticker, t): t for t in promising_tickers}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                results.append(result)
            
            if i % 5 == 0:
                progress_bar.progress((i + 1) / len(promising_tickers))
                status_text.text(f"Skanowanie: {i+1}/{len(promising_tickers)} | Znaleziono: {len(results)}")
    
    progress_bar.empty()
    status_text.empty()
    
    return results

def intelligent_scan(force_refresh=False):
    """
    GŁÓWNA FUNKCJA - dwuetapowe inteligentne skanowanie
    """
    # Krok 0: Lista spółek
    with st.spinner("📡 Pobieranie listy spółek..."):
        all_tickers = get_live_nasdaq_tickers()
    
    # Sprawdź cache
    if not force_refresh:
        cached = load_from_cache()
        if cached:
            cache_age = datetime.now() - cached['timestamp']
            if cache_age.total_seconds() / 3600 < CACHE_MAX_AGE_HOURS:
                st.info(f"📦 Używam cache sprzed {cache_age.seconds//60} minut")
                return cached['data'], all_tickers
    
    # FAZA 1: Prescan wszystkich spółek
    promising = prescan_all_tickers(all_tickers)
    
    if not promising:
        st.warning("❌ Nie znaleziono obiecujących spółek")
        return [], all_tickers
    
    # FAZA 2: Głębokie skanowanie
    results = deep_scan_promising(promising)
    
    # Zapisz do cache
    save_to_cache(results, all_tickers)
    
    return results, all_tickers

# ============================================
# INTERFEJS UŻYTKOWNIKA
# ============================================

st.markdown('<h1 class="main-header">🧠 NASDAQ Intelligent Scanner</h1>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry")
    
    st.markdown("---")
    
    # Informacje o trybie
    st.subheader("🧠 Tryb inteligentny")
    st.info("""
    **Dwuetapowe skanowanie:**
    1. Prescan 3500 spółek (30s)
    2. Głębokie skanowanie obiecujących (3-4 min)
    """)
    
    st.markdown("---")
    
    # Filtry
    st.subheader("📊 RVOL")
    use_rvol = st.checkbox("Filtruj RVOL >2 (2/4 dni)", value=True)
    
    st.subheader("💰 Przepływ")
    use_flow = st.checkbox("Filtruj OBV/A/D/CMF >0", value=True)
    
    st.subheader("💰 Cena")
    min_price = st.number_input("Min cena ($)", 0.0, 1000.0, 1.0, 0.5)
    max_price = st.number_input("Max cena ($)", 0.0, 10000.0, 500.0, 10.0)
    
    st.markdown("---")
    
    # Przyciski
    col1, col2 = st.columns(2)
    with col1:
        scan_btn = st.button("🧠 Skanuj inteligentnie", type="primary", use_container_width=True)
    with col2:
        refresh_btn = st.button("🔄 Świeże dane", use_container_width=True)
    
    st.markdown("---")
    
    # Konserwacja - NAPRAWIONE!
    st.subheader("🧹 Konserwacja")
    
    # Informacja o cache
    if os.path.exists(CACHE_FILE):
        size_kb = os.path.getsize(CACHE_FILE) / 1024
        st.caption(f"📦 Cache: {size_kb:.1f} KB")
    
    # Przycisk czyszczenia BEZ RESTARTU!
    if st.button("🧹 Wyczyść cache", use_container_width=True):
        if clear_cache():
            st.success("✅ Cache wyczyszczony! Następne skanowanie będzie świeże.")
            time.sleep(1)
        else:
            st.info("ℹ️ Cache był już pusty")
            time.sleep(1)

# Główna logika
if scan_btn or refresh_btn:
    force_refresh = refresh_btn
    
    if force_refresh:
        clear_cache()  # Wymuś świeże dane
    
    with st.spinner("Inicjowanie inteligentnego skanowania..."):
        results, all_tickers = intelligent_scan(force_refresh)
    
    if results:
        df = pd.DataFrame(results)
        
        # Filtry
        if min_price > 0:
            df = df[df['Cena'] >= min_price]
        if max_price < 10000:
            df = df[df['Cena'] <= max_price]
        if use_rvol:
            df = df[df['RVOL OK'] == '✅']
        if use_flow:
            df = df[df['Flow OK'] == '✅']
        
        # Statystyki
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(all_tickers)}</div>
                <div>Spółek na NASDAQ</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(df)}</div>
                <div>Po filtrach</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            rvol_count = len(df[df['RVOL OK'] == '✅'])
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{rvol_count}</div>
                <div>Z RVOL >2</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            flow_count = len(df[df['Flow OK'] == '✅'])
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{flow_count}</div>
                <div>Z przepływem >0</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Wyniki
        st.subheader("📋 Wyniki skanowania")
        
        df = df.sort_values('RVOL', ascending=False)
        
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Cena": st.column_config.NumberColumn(format="$%.2f"),
                "Wolumen": st.column_config.NumberColumn(format="%d"),
                "CMF": st.column_config.NumberColumn(format="%.3f"),
                "Zmiana 1d": st.column_config.NumberColumn(format="%.1f%%"),
                "Zmiana 5d": st.column_config.NumberColumn(format="%.1f%%")
            }
        )
        
        # Eksport
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 Pobierz CSV",
            csv,
            f"nasdaq_intelligent_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            "text/csv"
        )
        
        # Top 10
        with st.expander("🏆 Top 10 spółek", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Najwyższy RVOL**")
                st.dataframe(
                    df[['Ticker', 'RVOL', 'CMF', 'Cena']].head(10),
                    use_container_width=True,
                    hide_index=True
                )
            with col2:
                st.write("**Najwyższy CMF**")
                st.dataframe(
                    df.sort_values('CMF', ascending=False)[['Ticker', 'CMF', 'RVOL', 'Cena']].head(10),
                    use_container_width=True,
                    hide_index=True
                )
        
    else:
        st.warning("Nie znaleziono spółek spełniających kryteria. Spróbuj rozszerzyć filtry.")

# Instrukcja
with st.expander("ℹ️ Jak działa inteligentne skanowanie?", expanded=False):
    st.markdown("""
    ### 🧠 Dwuetapowe skanowanie:
    
    **FAZA 1: Prescan (30 sekund)**
    - Szybkie sprawdzenie wszystkich 3500 spółek
    - Tylko RVOL, uproszczone obliczenia
    - Próg: RVOL > 1.5
    
    **FAZA 2: Głębokie skanowanie (3-4 minuty)**
    - Tylko obiecujące spółki z fazy 1
    - Pełne obliczenia (RVOL, OBV, A/D, CMF)
    - Dokładne wskaźniki techniczne
    
    **Korzyści:**
    - ✅ Sprawdza WSZYSTKIE spółki
    - ✅ Szybciej niż pełne skanowanie
    - ✅ Nie pomija małych spółek z potencjałem
    
    **Konserwacja:**
    - Przycisk "Wyczyść cache" usuwa dane BEZ restartu
    - Następne skanowanie pobierze świeże dane
    """)