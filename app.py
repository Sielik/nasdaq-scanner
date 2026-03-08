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
    page_title="NASDAQ Ultra Fast Scanner",
    page_icon="⚡",
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
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        color: #0c5460;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .stProgress > div > div > div > div {
        background-color: #00A3E0;
    }
</style>
""", unsafe_allow_html=True)

# Stałe
CACHE_FILE = "nasdaq_ultra_cache.gz"
RVOL_THRESHOLD = 2.0
MAX_WORKERS = 20  # 20 równoległych zapytań!
TIMEOUT_SECONDS = 5  # Maksymalnie 5 sekund na spółkę
CACHE_MAX_AGE_HOURS = 12  # Cache ważny 12 godzin
MAX_STOCKS_IN_CACHE = 2500  # Maksymalna liczba spółek w cache

# ============================================
# FUNKCJE POBIERANIA AKTUALNEJ LISTY SPÓŁEK
# ============================================

@st.cache_data(ttl=3600, show_spinner=False)
def get_live_nasdaq_tickers():
    """Pobiera AKTUALNĄ listę spółek NASDAQ"""
    
    status = st.sidebar.empty()
    status.info("📡 Pobieranie listy spółek...")
    
    # Źródło 1: NASDAQ Trader (oficjalne)
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
    
    # Źródło 2: GitHub (szybki backup)
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
    """Lista awaryjna 200+ spółek"""
    return [
        "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "INTC", "TSLA", "NFLX",
        "AMZN", "PEP", "COST", "CSCO", "ADBE", "CRM", "ORCL", "IBM", "QCOM",
        "TXN", "AVGO", "AMAT", "MU", "NXPI", "KLAC", "LRCX", "ASML", "SNPS",
        "CDNS", "ADI", "MCHP", "ON", "SWKS", "QRVO", "MPWR", "INTU", "NOW",
        "PANW", "FTNT", "CRWD", "ZS", "OKTA", "DDOG", "MDB", "SNOW", "PLTR",
        "GILD", "REGN", "VRTX", "MRNA", "ILMN", "BNTX", "ALNY", "BIIB", "AMGN",
        "SQ", "SOFI", "AFRM", "COIN", "HOOD", "ABNB", "RIVN", "PYPL", "BKNG",
        "SPOT", "UBER", "DASH", "ZM", "DOCU", "TWLO", "EA", "TTWO", "ROKU",
        "PINS", "SNAP", "NET", "RBLX", "U", "PATH", "AI", "PLTR", "SNOW"
    ]

# ============================================
# FUNKCJE CACHE - Z OGRANICZENIEM ROZMIARU
# ============================================

def save_ultra_cache(data, tickers):
    """Zapisuje cache z ograniczeniem rozmiaru - optymalizacja wydajności"""
    try:
        # Ogranicz liczbę przechowywanych spółek
        if len(data) > MAX_STOCKS_IN_CACHE:
            # Posortuj według wolumenu (ważniejsze spółki) i weź top N
            data = sorted(data, key=lambda x: x.get('Wolumen', 0), reverse=True)[:MAX_STOCKS_IN_CACHE]
        
        cache = {
            'timestamp': datetime.now(),
            'tickers': tickers,
            'data': data
        }
        
        # Kompresja dla szybkości
        json_str = json.dumps(cache, default=str)
        compressed = gzip.compress(json_str.encode())
        
        with open(CACHE_FILE, 'wb') as f:
            f.write(compressed)
        
        return True
    except Exception as e:
        st.sidebar.error(f"Błąd zapisu cache: {e}")
        return False

def load_ultra_cache():
    """Wczytuje skompresowany cache - optymalizacja wydajności"""
    if not os.path.exists(CACHE_FILE):
        return None
    
    try:
        with open(CACHE_FILE, 'rb') as f:
            compressed = f.read()
        
        json_str = gzip.decompress(compressed).decode()
        cache = json.loads(json_str)
        
        # Konwersja timestamp
        cache['timestamp'] = datetime.fromisoformat(cache['timestamp'])
        
        return cache
    except Exception as e:
        st.sidebar.warning(f"Błąd odczytu cache: {e}")
        return None

def clear_cache():
    """Czyści plik cache"""
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        return True
    return False

def get_cache_info():
    """Zwraca informacje o cache"""
    if not os.path.exists(CACHE_FILE):
        return None
    
    try:
        size_kb = os.path.getsize(CACHE_FILE) / 1024
        cache = load_ultra_cache()
        if cache:
            age = datetime.now() - cache['timestamp']
            return {
                'size': f"{size_kb:.1f} KB",
                'age': f"{age.seconds//3600}h {(age.seconds//60)%60}m",
                'stocks': len(cache.get('data', [])),
                'timestamp': cache['timestamp']
            }
    except:
        pass
    return None

# ============================================
# FUNKCJE POBIERANIA DANYCH - ZOPTYMALIZOWANE
# ============================================

def fetch_single_stock(ticker):
    """Pobiera pojedynczą spółkę z timeout'em"""
    try:
        url = f"https://stooq.pl/q/d/l/?s={ticker.lower()}.us&i=d"
        
        # Szybkie pobieranie z timeout'em
        response = requests.get(url, timeout=TIMEOUT_SECONDS)
        
        if response.status_code != 200:
            return None
        
        df = pd.read_csv(StringIO(response.text))
        
        if df.empty or len(df) < 25:
            return None
        
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Ticker'] = ticker
        
        return df
        
    except Exception as e:
        return None

def calculate_fast_indicators(df):
    """Szybkie obliczanie wskaźników - tylko potrzebne dane"""
    try:
        if df is None or len(df) < 25:
            return None
        
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
        
        # OBV (trend)
        obv_trend = 1 if df['Close'].iloc[-1] > df['Close'].iloc[-20] else -1
        
        # A/D (ostatni dzień)
        high, low, close = df['High'].iloc[-1], df['Low'].iloc[-1], df['Close'].iloc[-1]
        if high != low:
            clv = ((close - low) - (high - close)) / (high - low)
        else:
            clv = 0
        ad_trend = 1 if clv > 0 else -1
        
        # CMF (uproszczony)
        cmf = (df['Close'].iloc[-1] / df['Close'].iloc[-20] - 1) * 100 if len(df) >= 20 else 0
        
        flow_ok = (obv_trend > 0 and ad_trend > 0 and cmf > 0)
        
        # Zmiany
        change_1d = ((df['Close'].iloc[-1] / df['Close'].iloc[-2] - 1) * 100) if len(df) >= 2 else 0
        
        return {
            'Ticker': df['Ticker'].iloc[0],
            'Cena': round(df['Close'].iloc[-1], 2),
            'Wolumen': int(df['Volume'].iloc[-1]),
            'RVOL': round(today_rvol, 2),
            'Dni>2': days_over_2,
            'RVOL OK': '✅' if rvol_ok else '❌',
            'Flow OK': '✅' if flow_ok else '❌',
            'CMF': round(cmf, 2),
            'Zmiana 1d': round(change_1d, 2),
            'Data': datetime.now().strftime('%H:%M')
        }
        
    except Exception as e:
        return None

# ============================================
# GŁÓWNA FUNKCJA - SUPERSZYBKIE SKANOWANIE
# ============================================

def ultra_fast_scan(force_refresh=False):
    """
    Najszybsze możliwe skanowanie - równoległe + zoptymalizowany cache
    """
    
    # Krok 1: Lista spółek (zawsze świeża)
    with st.spinner("📡 Pobieranie listy spółek..."):
        current_tickers = get_live_nasdaq_tickers()
    
    # Krok 2: Sprawdź cache
    cached = load_ultra_cache()
    
    if cached and not force_refresh:
        cache_age = datetime.now() - cached['timestamp']
        cache_hours = cache_age.total_seconds() / 3600
        
        if cache_hours < CACHE_MAX_AGE_HOURS:
            cached_tickers = set(cached['tickers'])
            current_set = set(current_tickers)
            
            new_tickers = current_set - cached_tickers
            removed_tickers = cached_tickers - current_set
            
            # Użyj cache dla starych
            all_results = [d for d in cached['data'] 
                          if d['Ticker'] in cached_tickers - removed_tickers]
            
            # Skanuj tylko nowe (równolegle!)
            if new_tickers:
                new_list = list(new_tickers)
                st.warning(f"🆕 Skanowanie {len(new_list)} nowych spółek...")
                
                new_results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {
                        executor.submit(fetch_single_stock, t): t 
                        for t in new_list
                    }
                    
                    progress = st.progress(0)
                    for i, future in enumerate(as_completed(futures)):
                        df = future.result()
                        if df:
                            result = calculate_fast_indicators(df)
                            if result:
                                new_results.append(result)
                        progress.progress((i + 1) / len(new_list))
                    
                    progress.empty()
                
                all_results.extend(new_results)
                
                # Zapisz zaktualizowany cache
                save_ultra_cache(all_results, current_tickers)
            
            return all_results, current_tickers
    
    # Krok 3: Pierwsze skanowanie - RÓWNOLEGLE!
    st.warning(f"⚡ Pierwsze skanowanie {len(current_tickers)} spółek (zajmie ~3-4 minuty)...")
    
    results = []
    failed = []
    
    # Równoległe skanowanie
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_single_stock, t): t 
            for t in current_tickers
        }
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        completed = 0
        total = len(current_tickers)
        
        start_time = time.time()
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                df = future.result()
                if df:
                    result = calculate_fast_indicators(df)
                    if result:
                        results.append(result)
                else:
                    failed.append(ticker)
            except:
                failed.append(ticker)
            
            completed += 1
            
            # Aktualizuj co 50 spółek
            if completed % 50 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (total - completed) / rate if rate > 0 else 0
                
                progress_bar.progress(completed / total)
                status_text.text(
                    f"⚡ {completed}/{total} | "
                    f"Szybkość: {rate:.1f}/s | "
                    f"Pozostało: {int(remaining//60)}m {int(remaining%60)}s | "
                    f"Znaleziono: {len(results)}"
                )
        
        progress_bar.empty()
        status_text.empty()
    
    # Zapisz cache
    save_ultra_cache(results, current_tickers)
    
    return results, current_tickers

# ============================================
# INTERFEJS UŻYTKOWNIKA
# ============================================

st.markdown('<h1 class="main-header">⚡ NASDAQ Ultra Fast Scanner</h1>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry wyszukiwania")
    
    st.markdown("---")
    
    # Informacje o wydajności
    st.subheader("📊 Wydajność")
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"⚡ {MAX_WORKERS} wątków")
    with col2:
        st.info(f"⏱️ {TIMEOUT_SECONDS}s timeout")
    
    # Informacje o cache
    cache_info = get_cache_info()
    if cache_info:
        st.subheader("💾 Cache")
        st.text(f"Rozmiar: {cache_info['size']}")
        st.text(f"Wiek: {cache_info['age']}")
        st.text(f"Spółki: {cache_info['stocks']}")
    
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
        scan_btn = st.button("⚡ Skanuj", type="primary", use_container_width=True)
    with col2:
        refresh_btn = st.button("🔄 Nowe dane", use_container_width=True)
    
    # Przycisk czyszczenia cache
    st.markdown("---")
    st.subheader("🧹 Konserwacja")
    
    if st.button("🧹 Wyczyść cache i uruchom ponownie", use_container_width=True):
        if clear_cache():
            st.success("✅ Cache wyczyszczony! Następne skanowanie będzie świeże.")
            time.sleep(1)
            st.rerun()
        else:
            st.info("ℹ️ Cache był już pusty")
            time.sleep(1)
            st.rerun()

# Główna logika
if scan_btn or refresh_btn:
    force_refresh = refresh_btn
    
    with st.spinner("Inicjowanie superszybkiego skanowania..."):
        results, current_tickers = ultra_fast_scan(force_refresh)
    
    if results:
        df = pd.DataFrame(results)
        
        # Zastosuj filtry
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
                <div style="font-size: 2rem;">{len(current_tickers)}</div>
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
        
        # Sortuj według RVOL
        df = df.sort_values('RVOL', ascending=False)
        
        # Wyświetl tabelę
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Cena": st.column_config.NumberColumn(format="$%.2f"),
                "Wolumen": st.column_config.NumberColumn(format="%d"),
                "CMF": st.column_config.NumberColumn(format="%.1f"),
                "Zmiana 1d": st.column_config.NumberColumn(format="%.1f%%")
            }
        )
        
        # Eksport do CSV
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 Pobierz CSV",
            csv,
            f"nasdaq_ultra_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            "text/csv",
            use_container_width=True
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
with st.expander("ℹ️ Instrukcja obsługi", expanded=False):
    st.markdown("""
    ### ⚡ Superszybkie skanowanie NASDAQ
    
    **Jak to działa:**
    - 20 wątków równolegle (zamiast 1)
    - Timeout 5 sekund (nie czeka na martwe spółki)
    - Kompresowany cache (błyskawiczny odczyt)
    - Automatyczne czyszczenie starego cache
    
    **Filtry:**
    - **RVOL > 2** dzisiaj i w ≥2 z ostatnich 4 dni roboczych
    - **OBV, A/D, CMF > 0** (wszystkie trzy wskaźniki)
    - Filtry cenowe (min/max)
    
    **Czasy skanowania:**
    - Pierwsze skanowanie: ~3-4 minuty
    - Kolejne skanowania: ~30-60 sekund
    - Tylko nowe spółki: ~10-20 sekund
    
    **Konserwacja:**
    - Jeśli aplikacja zwolni, kliknij "Wyczyść cache"
    - Cache jest automatycznie ograniczany do {MAX_STOCKS_IN_CACHE} spółek
    - Dane w cache są ważne {CACHE_MAX_AGE_HOURS} godzin
    
    **Źródła danych:**
    - Lista spółek: NASDAQ Trader (oficjalne, na żywo)
    - Dane cenowe: stooq.pl (darmowe, bez limitów)
    """.format(MAX_STOCKS_IN_CACHE=MAX_STOCKS_IN_CACHE, CACHE_MAX_AGE_HOURS=CACHE_MAX_AGE_HOURS))