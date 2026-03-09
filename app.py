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
PRESCAN_THRESHOLD = 1.5
MAX_WORKERS = 20  # StockHero nie ma limitów, możemy skanować równolegle!
TIMEOUT_SECONDS = 10
CACHE_MAX_AGE_HOURS = 12

# ============================================
# IMPORT STOCKHERO - DZIAŁA BEZ KLUCZY API!
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
    st.error("❌ StockHero nie jest zainstalowane! Dodaj 'StockHero' do requirements.txt")
    st.stop()

with st.expander("🔧 TESTY", expanded=True):
    st.markdown('<span class="free-badge">💰 DARMOWE - BEZ KLUCZY API</span>', unsafe_allow_html=True)
    
    if st.button("🔍 TEST - RVOL dla AAPL"):
        with st.spinner("Sprawdzam AAPL..."):
            try:
                ticker = stock.Ticker('AAPL')
                df = ticker.nasdaq.hist_quotes_stock
                
                if df is not None and len(df) > 10:
                    df = df.sort_values('Date', ascending=False).head(10)
                    volumes = df['Volume'].values
                    avg_vol = np.mean(volumes[1:])
                    today_vol = volumes[0]
                    rvol = today_vol / avg_vol if avg_vol > 0 else 0
                    
                    st.success(f"✅ RVOL dla AAPL = {rvol:.2f}")
                    st.write(f"Średni wolumen (9d): {avg_vol:.0f}")
                    st.write(f"Dzisiejszy wolumen: {today_vol:.0f}")
                    st.dataframe(df.head())
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
        status.warning("⚠️ NASDAQ Trader niedostępny, używam backupu...")
    
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
# FUNKCJA POBIERANIA DANYCH Z STOCKHERO
# ============================================

def get_stockhero_data(ticker):
    """
    Pobiera dane giełdowe z StockHero
    StockHero: darmowe, bez limitów, bez kluczy API!
    """
    try:
        ticker_obj = stock.Ticker(ticker)
        df = ticker_obj.nasdaq.hist_quotes_stock
        
        if df is None or len(df) < 25:
            return None
        
        # StockHero zwraca kolumny: Date, Open, High, Low, Close, Volume
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
        
        # Pobierz ostatnie 5 dni (dzisiaj + 4 poprzednie)
        last_5_volumes = df['Volume'].values[-5:]
        
        # Średnia z 20 dni do obliczenia RVOL
        avg_volume = df['Volume'].tail(20).mean()
        
        if avg_volume == 0:
            return 0
        
        # Sprawdź każdy z ostatnich 5 dni
        max_rvol = 0
        for i, vol in enumerate(last_5_volumes):
            rvol = vol / avg_volume
            if rvol > max_rvol:
                max_rvol = rvol
        
        # Jeśli w ostatnich 5 dniach był RVOL > 3, to obiecująca
        if max_rvol > 3:
            return max_rvol  # Zwracamy najwyższy RVOL
        
        return 0
        
    except Exception as e:
        return 0


# ============================================
# FUNKCJE CACHE
# ============================================

def save_to_cache(data, tickers):
    """Zapisuje cache"""
    try:
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
    """Czyści cache bez restartu"""
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        return True
    return False

# ============================================
# FAZA 1: PRESCAN
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
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(quick_rvol_check, t): t for t in tickers}
        
        for i, future in enumerate(as_completed(futures)):
            ticker = futures[future]
            try:
                max_rvol = future.result(timeout=15)
                if max_rvol > 0:
                    promising_tickers.append(ticker)
                    promising_data.append({
                        'Ticker': ticker,
                        'Max RVOL (4d)': round(max_rvol, 2),
                        'Status': 'TEST - ma RVOL > 3'
                    })
            except:
                pass
            
            if i % 50 == 0:
                progress_bar.progress(i / len(tickers))
                status_text.text(f"Prescan: {i}/{len(tickers)} | Znaleziono: {len(promising_tickers)}")
    
    progress_bar.empty()
    status_text.empty()
    
    if promising_data:
        st.success(f"✅ TEST: Znaleziono {len(promising_data)} spółek z RVOL > 3!")
        
        # Pokaż pierwsze 20 znalezionych
        df_test = pd.DataFrame(promising_data[:20])
        st.dataframe(df_test)
    else:
        st.error("❌ TEST: Nie znaleziono żadnej spółki z RVOL > 3")
    
    return promising_tickers, promising_data

# ============================================
# FAZA 2: GŁĘBOKIE SKANOWANIE
# ============================================

def deep_scan_ticker(ticker):
    """
    FAZA 2: Głębokie skanowanie pojedynczej spółki
    Z pominięciem weekendów - tylko dni robocze!
    """
    try:
        df = get_stockhero_data(ticker)
        
        if df is None or len(df) < 25:
            return None
        
        df = df.sort_values('Date').reset_index(drop=True)
        
        # Konwersja kolumny Date na datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # ODFILTRUJ WEEKENDY (sobota=5, niedziela=6)
        df = df[df['Date'].dt.dayofweek < 5]  # Tylko pon-pt
        df = df.reset_index(drop=True)
        
        if len(df) < 20:  # Potrzebujemy minimum 20 dni roboczych
            return None
        
        # RVOL
        avg_volume = df['Volume'].tail(20).mean()
        
        # Sprawdź RVOL dla ostatnich 5 DNI ROBOCZYCH
        rvol_values = []
        for i in range(1, 6):
            if len(df) >= i:
                vol = df['Volume'].iloc[-i]
                rvol = vol / avg_volume if avg_volume > 0 else 0
                rvol_values.append(rvol)
        
        today_rvol = rvol_values[0] if rvol_values else 0
        
        # Sprawdź ostatnie 4 DNI ROBOCZE (bez dzisiaj)
        last_4_rvol = rvol_values[1:5] if len(rvol_values) >= 5 else []
        days_over_2 = sum(1 for r in last_4_rvol if r > RVOL_THRESHOLD)
        
        rvol_ok = (today_rvol > RVOL_THRESHOLD) and (days_over_2 >= 2)
        
        # Jeśli nie spełnia RVOL, nie licz reszty
        if not rvol_ok:
            return None
        
        # Reszta kodu bez zmian (OBV, A/D, CMF)...
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
        
        # Flow OK - wszystkie 3 > 0
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
    st.markdown('<div class="phase-indicator">🔬 FAZA 2: Głębokie skanowanie - <span class="free-badge">💰 DARMOWE API</span></div>', 
                unsafe_allow_html=True)
    
    if not promising_tickers:
        st.warning("Brak spółek do głębokiego skanowania")
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

def intelligent_scan(force_refresh=False):
    """
    GŁÓWNA FUNKCJA - dwuetapowe inteligentne skanowanie z StockHero
    """
    # Lista spółek
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
    #deep_results = deep_scan_promising(promising_tickers)
    deep_results = []
    # Zapisz do cache
    #save_to_cache(deep_results, all_tickers)
    
    return deep_results, promising_data, all_tickers

# ============================================
# SIDEBAR
# ============================================

with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry")
    
    st.markdown("---")
    
    # Informacje
    st.subheader("🧠 Informacje")
    st.markdown('<span class="free-badge">💰 DARMOWE API - BEZ LIMITÓW</span>', unsafe_allow_html=True)
    st.caption(f"Prescan: RVOL > {PRESCAN_THRESHOLD}")
    st.caption(f"Głębokie: RVOL > {RVOL_THRESHOLD} (2/4 dni) + OBV/A/D/CMF > 0")
    
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
        scan_btn = st.button("🧠 Skanuj", type="primary", use_container_width=True)
    with col2:
        refresh_btn = st.button("🔄 Świeże dane", use_container_width=True)
    
    st.markdown("---")
    
    # Konserwacja
    st.subheader("🧹 Konserwacja")
    
    if os.path.exists(CACHE_FILE):
        size_kb = os.path.getsize(CACHE_FILE) / 1024
        st.caption(f"📦 Cache: {size_kb:.1f} KB")
    
    if st.button("🧹 Wyczyść cache", use_container_width=True):
        if clear_cache():
            st.success("✅ Cache wyczyszczony! Następne skanowanie pobierze świeże dane.")
        else:
            st.info("ℹ️ Cache był już pusty")

# ============================================
# GŁÓWNA LOGIKA
# ============================================

if scan_btn or refresh_btn:
    force_refresh = refresh_btn
    
    if force_refresh:
        clear_cache()
    
    with st.spinner("Inicjowanie skanowania..."):
        deep_results, prescan_data, all_tickers = intelligent_scan(force_refresh)
    
    if prescan_data or deep_results:
        
        tab1, tab2 = st.tabs(["🔍 FAZA 1: Prescan (RVOL > 1.5)", "🔬 FAZA 2: Głębokie skanowanie"])
        
        with tab1:
            if prescan_data:
                df_prescan = pd.DataFrame(prescan_data)
                st.subheader(f"Znaleziono {len(df_prescan)} spółek z RVOL > 1.5")
                
                st.dataframe(
                    df_prescan.sort_values('RVOL (prescan)', ascending=False),
                    use_container_width=True,
                    hide_index=True
                )
                
                csv_prescan = df_prescan.to_csv(index=False)
                st.download_button(
                    "📥 Pobierz prescan CSV",
                    csv_prescan,
                    f"prescan_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    "text/csv"
                )
            else:
                st.info("Brak spółek w prescanie")
        
        with tab2:
            if deep_results:
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
                
                # Statystyki
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Przed filtrami", len(deep_results))
                with col2:
                    st.metric("Po filtrach", len(df_deep))
                with col3:
                    flow_count = len(df_deep[df_deep['Flow OK'] == '✅'])
                    st.metric("Z przepływem >0", flow_count)
                
                st.subheader("Wyniki głębokiego skanowania")
                
                st.dataframe(
                    df_deep.sort_values('RVOL', ascending=False),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Cena": st.column_config.NumberColumn(format="$%.2f"),
                        "Wolumen": st.column_config.NumberColumn(format="%d"),
                        "OBV": st.column_config.NumberColumn(format="%.2f"),
                        "A/D": st.column_config.NumberColumn(format="%.2f"),
                        "CMF": st.column_config.NumberColumn(format="%.3f"),
                        "Zmiana 1d": st.column_config.NumberColumn(format="%.1f%%"),
                        "Zmiana 5d": st.column_config.NumberColumn(format="%.1f%%")
                    }
                )
                
                csv_deep = df_deep.to_csv(index=False)
                st.download_button(
                    "📥 Pobierz głębokie CSV",
                    csv_deep,
                    f"deep_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    "text/csv"
                )
                
                # Top 10
                with st.expander("🏆 Top 10", expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Najwyższy RVOL**")
                        st.dataframe(
                            df_deep[['Ticker', 'RVOL', 'CMF', 'Cena']].head(10),
                            use_container_width=True,
                            hide_index=True
                        )
                    with col2:
                        st.write("**Najwyższy CMF**")
                        st.dataframe(
                            df_deep.sort_values('CMF', ascending=False)[['Ticker', 'CMF', 'RVOL', 'Cena']].head(10),
                            use_container_width=True,
                            hide_index=True
                        )
            else:
                st.info("Brak wyników w głębokim skanowaniu")
        
    else:
        st.warning("Nie znaleziono żadnych spółek")

# Instrukcja
with st.expander("ℹ️ Instrukcja", expanded=False):
    st.markdown("""
    ### 📊 NASDAQ StockHero Scanner
    
    **Jak działa:**
    - Używa StockHero – darmowej biblioteki Python
    - **Nie wymaga kluczy API ani rejestracji!**
    - Skanuje wszystkie spółki NASDAQ (~3500)
    
    **Fazy:**
    1. **Prescan** - szybkie sprawdzenie RVOL > 1.5
    2. **Głębokie** - RVOL > 2.0 (2/4 dni) + OBV/A/D/CMF > 0
    
    **Darmowe i bez limitów!** 🎉
    """)