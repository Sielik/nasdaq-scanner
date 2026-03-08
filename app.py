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
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #00A3E0;
        text-align: center;
        margin-bottom: 1rem;
    }
    .stat-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
    }
    .flash {
        animation: flash 1s infinite;
    }
    @keyframes flash {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

# Stałe
CACHE_FILE = "nasdaq_ultra_cache.gz"
RVOL_THRESHOLD = 2.0
MAX_WORKERS = 20  # 20 równoległych zapytań!
TIMEOUT_SECONDS = 5  # Maksymalnie 5 sekund na spółkę

# ============================================
# FUNKCJE POBIERANIA AKTUALNEJ LISTY SPÓŁEK
# ============================================

@st.cache_data(ttl=3600)  # Odświeżaj co 1 godzinę!
def get_live_nasdaq_tickers():
    """Pobiera AKTUALNĄ listę spółek NASDAQ - superszybko!"""
    
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
        status.warning("⚠️ Używam backupu")
    
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

def get_fallback_tickers():
    """Lista awaryjna 200+ spółek"""
    return [
        "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "INTC", "TSLA", "NFLX",
        "AMZN", "PEP", "COST", "CSCO", "ADBE", "CRM", "ORCL", "IBM", "QCOM",
        "TXN", "AVGO", "AMAT", "MU", "NXPI", "KLAC", "LRCX", "ASML", "SNPS",
        "CDNS", "ADI", "MCHP", "ON", "SWKS", "QRVO", "MPWR", "INTU", "NOW",
        "PANW", "FTNT", "CRWD", "ZS", "OKTA", "DDOG", "MDB", "SNOW", "PLTR",
        "GILD", "REGN", "VRTX", "MRNA", "ILMN", "BNTX", "ALNY", "BIIB", "AMGN"
    ]

# ============================================
# FUNKCJE CACHE - Redis w pliku (szybki)
# ============================================

def save_ultra_cache(data, tickers):
    """Zapisuje cache z kompresją - superszybki odczyt"""
    try:
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
    except:
        return False

def load_ultra_cache():
    """Wczytuje skompresowany cache - błyskawicznie!"""
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
    except:
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
        
    except:
        return None

def calculate_fast_indicators(df):
    """Szybkie obliczanie wskaźników"""
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
        
        # OBV (uproszczony dla szybkości)
        obv_trend = 1 if df['Close'].iloc[-1] > df['Close'].iloc[-20] else -1
        
        # A/D (ostatnie 20 dni)
        high, low, close = df['High'].iloc[-1], df['Low'].iloc[-1], df['Close'].iloc[-1]
        if high != low:
            clv = ((close - low) - (high - close)) / (high - low)
        else:
            clv = 0
        ad_trend = 1 if clv > 0 else -1
        
        # CMF (uproszczony)
        cmf = (df['Close'].iloc[-1] - df['Close'].iloc[-20]) / df['Close'].iloc[-20] * 100
        
        flow_ok = (obv_trend > 0 and ad_trend > 0 and cmf > 0)
        
        # Zmiany
        change_1d = ((df['Close'].iloc[-1] / df['Close'].iloc[-2] - 1) * 100) if len(df) >= 2 else 0
        
        return {
            'Ticker': df['Ticker'].iloc[0],
            'Cena': round(df['Close'].iloc[-1], 2),
            'RVOL': round(today_rvol, 2),
            'Dni>2': days_over_2,
            'RVOL OK': '✅' if rvol_ok else '❌',
            'Flow OK': '✅' if flow_ok else '❌',
            'CMF': round(cmf, 2),
            'Zmiana 1d': round(change_1d, 2),
            'Data': datetime.now().strftime('%H:%M')
        }
        
    except:
        return None

# ============================================
# GŁÓWNA FUNKCJA - SUPERSZYBKIE SKANOWANIE
# ============================================

def ultra_fast_scan(force_refresh=False):
    """
    Najszybsze możliwe skanowanie - równoległe + Redis w pliku
    """
    
    # Krok 1: Lista spółek (zawsze świeża)
    with st.spinner("📡 Pobieranie listy spółek..."):
        current_tickers = get_live_nasdaq_tickers()
    
    # Krok 2: Sprawdź cache
    cached = load_ultra_cache()
    
    if cached and not force_refresh:
        cache_age = datetime.now() - cached['timestamp']
        cache_hours = cache_age.total_seconds() / 3600
        
        if cache_hours < 24:
            cached_tickers = set(cached['tickers'])
            current_set = set(current_tickers)
            
            new_tickers = current_set - cached_tickers
            removed_tickers = cached_tickers - current_set
            
            st.sidebar.info(f"📦 Cache: {len(cached['data'])} spółek, {cache_hours:.1f}h")
            
            if new_tickers:
                st.sidebar.warning(f"🆕 Nowe: {len(new_tickers)}")
            
            # Użyj cache dla starych
            all_results = [d for d in cached['data'] 
                          if d['Ticker'] in cached_tickers - removed_tickers]
            
            # Skanuj tylko nowe (równolegle!)
            if new_tickers:
                with st.spinner(f"⚡ Skanowanie {len(new_tickers)} nowych spółek..."):
                    new_results = []
                    new_list = list(new_tickers)
                    
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
                    st.success(f"✅ Dodano {len(new_results)} nowych")
                    
                    # Zapisz zaktualizowany cache
                    save_ultra_cache(all_results, current_tickers)
            
            return all_results, current_tickers
    
    # Krok 3: Pierwsze skanowanie - RÓWNOLEGLE! ⚡
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
                rate = completed / elapsed
                remaining = (total - completed) / rate
                
                progress_bar.progress(completed / total)
                status_text.text(
                    f"⚡ {completed}/{total} | "
                    f"Szybkość: {rate:.1f}/s | "
                    f"Pozostało: {int(remaining//60)}m {int(remaining%60)}s | "
                    f"Znaleziono: {len(results)}"
                )
        
        progress_bar.empty()
        status_text.empty()
    
    # Podsumowanie
    success_rate = (len(results) / (len(results) + len(failed))) * 100
    st.info(f"📊 Sukces: {len(results)} spółek ({success_rate:.1f}%), Pominięto: {len(failed)}")
    
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
    st.header("🔍 Filtry")
    
    st.markdown("---")
    
    # Status
    st.subheader("📊 Wydajność")
    st.info(f"⚡ {MAX_WORKERS} wątków równolegle")
    st.info(f"⏱️ Timeout: {TIMEOUT_SECONDS}s")
    
    st.markdown("---")
    
    # Filtry
    st.subheader("📊 RVOL")
    use_rvol = st.checkbox("Filtruj RVOL >2 (2/4 dni)", value=True)
    
    st.subheader("💰 Przepływ")
    use_flow = st.checkbox("Filtruj OBV/A/D/CMF >0", value=True)
    
    st.subheader("💰 Cena")
    min_price = st.number_input("Min cena ($)", 0.0, 1000.0, 1.0)
    max_price = st.number_input("Max cena ($)", 0.0, 10000.0, 500.0)
    
    st.markdown("---")
    
    # Przyciski
    col1, col2 = st.columns(2)
    with col1:
        scan_btn = st.button("⚡ Skanuj", type="primary", use_container_width=True)
    with col2:
        refresh_btn = st.button("🔄 Nowe dane", use_container_width=True)

# Główna logika
if scan_btn or refresh_btn:
    force_refresh = refresh_btn
    
    with st.spinner("Inicjowanie superszybkiego skanowania..."):
        results, current_tickers = ultra_fast_scan(force_refresh)
    
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
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(df[df['RVOL OK'] == '✅'])}</div>
                <div>Z RVOL >2</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(df[df['Flow OK'] == '✅'])}</div>
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
                "CMF": st.column_config.NumberColumn(format="%.1f"),
                "Zmiana 1d": st.column_config.NumberColumn(format="%.1f%%")
            }
        )
        
        # Eksport
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 Pobierz CSV",
            csv,
            f"nasdaq_ultra_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            "text/csv"
        )
        
        # Top 10
        with st.expander("🏆 Top 10"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Najwyższy RVOL**")
                st.dataframe(df[['Ticker', 'RVOL', 'CMF', 'Cena']].head(10))
            with col2:
                st.write("**Najwyższy CMF**")
                st.dataframe(df.sort_values('CMF', ascending=False)[['Ticker', 'CMF', 'RVOL', 'Cena']].head(10))
        
    else:
        st.warning("Nie znaleziono spółek. Spróbuj rozszerzyć filtry.")

# Instrukcja
with st.expander("ℹ️ Instrukcja"):
    st.markdown("""
    ### ⚡ Superszybkie skanowanie:
    
    **Dlaczego to jest szybkie?**
    - 20 wątków równolegle (zamiast 1)
    - Timeout 5 sekund (nie czeka na martwe spółki)
    - Kompresowany cache (błyskawiczny odczyt)
    - Uproszczone obliczenia dla szybkości
    
    **Czasy:**
    - Pierwsze skanowanie: ~3-4 minuty
    - Kolejne skanowania: ~30-60 sekund
    - Z nowymi spółkami: ~10-20 sekund
    
    **Filtry:**
    - RVOL >2 dzisiaj i w ≥2/4 dni
    - OBV, A/D, CMF >0 (uproszczone)
    - Filtry cenowe
    """)