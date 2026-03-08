import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import time
import os
import requests
import pickle
from io import StringIO

# Konfiguracja strony
st.set_page_config(
    page_title="NASDAQ Live Scanner",
    page_icon="📊",
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
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 10px;
    }
    .warning-box {
        background-color: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Stałe
PRICE_CACHE_FILE = "price_cache.pkl"
RVOL_THRESHOLD = 2.0

# ============================================
# FUNKCJE POBIERANIA AKTUALNEJ LISTY SPÓŁEK
# ============================================

@st.cache_data(ttl=3600)  # Odświeżaj co 1 godzinę!
def get_live_nasdaq_tickers():
    """
    Pobiera AKTUALNĄ listę spółek NASDAQ bezpośrednio z giełdy
    """
    st.sidebar.info("📡 Pobieranie aktualnej listy spółek NASDAQ...")
    
    # Źródło 1: NASDAQ Trader (oficjalne, aktualizowane codziennie)
    try:
        url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
        df = pd.read_csv(url, sep='|')
        
        # Filtruj tylko aktywne spółki NASDAQ
        stocks = df[
            (df['NASDAQ Symbol'].notna()) &  # Ma ticker
            (df['ETF'] == 'N') &  # To nie jest ETF
            (df['TEST ISSUE'] == 'N')  # To nie jest testowa
        ]
        
        tickers = stocks['NASDAQ Symbol'].tolist()
        tickers = [t.strip() for t in tickers if t.strip()]
        tickers = sorted(list(set(tickers)))
        
        st.sidebar.success(f"✅ Pobrano {len(tickers)} spółek z NASDAQ Trader")
        return tickers
        
    except Exception as e:
        st.sidebar.warning(f"⚠️ NASDAQ Trader niedostępny: {e}")
    
    # Źródło 2: GitHub (często aktualizowane)
    try:
        backup_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt"
        response = requests.get(backup_url)
        if response.status_code == 200:
            tickers = response.text.strip().split('\n')
            tickers = [t.strip().upper() for t in tickers if t.strip()]
            st.sidebar.success(f"✅ Pobrano {len(tickers)} spółek z GitHub")
            return tickers
    except:
        pass
    
    # Źródło 3: Lista awaryjna
    st.sidebar.warning("⚠️ Używam lokalnej listy awaryjnej - dane mogą być nieaktualne!")
    return get_fallback_tickers()

def get_fallback_tickers():
    """Lista awaryjna na wypadek całkowitego braku dostępu"""
    return [
        "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "INTC", "TSLA", "NFLX",
        "AMZN", "PEP", "COST", "CSCO", "ADBE", "CRM", "ORCL", "IBM", "QCOM",
        "TXN", "AVGO", "AMAT", "MU", "NXPI", "KLAC", "LRCX", "ASML", "SNPS",
        "CDNS", "ADI", "MCHP", "ON", "SWKS", "QRVO", "MPWR", "INTU", "NOW",
        "PANW", "FTNT", "CRWD", "ZS", "OKTA", "DDOG", "MDB", "SNOW", "PLTR",
        "GILD", "REGN", "VRTX", "MRNA", "ILMN", "BNTX", "ALNY", "BIIB", "AMGN",
        "SQ", "SOFI", "AFRM", "COIN", "HOOD", "ABNB", "RIVN", "PYPL", "BKNG",
        "SPOT", "UBER", "DASH", "ZM", "DOCU", "TWLO", "EA", "TTWO", "ROKU"
    ]

# ============================================
# FUNKCJE CACHE'OWANIA DANYCH CENOWYCH
# ============================================

def load_price_cache():
    """Wczytuje cache z danymi cenowymi"""
    if not os.path.exists(PRICE_CACHE_FILE):
        return None
    
    try:
        with open(PRICE_CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        
        # Sprawdź wiek cache
        cache_age = datetime.now() - cache['timestamp']
        if cache_age < timedelta(hours=24):  # Cache ważny 24h
            st.sidebar.info(f"📦 Cache cenowy: {len(cache['data'])} spółek, {cache_age.seconds//3600}h stary")
            return cache
        else:
            st.sidebar.warning(f"📦 Cache wygasł ({cache_age.seconds//3600}h temu)")
            return None
    except:
        return None

def save_price_cache(data, tickers_list):
    """Zapisuje dane cenowe do cache"""
    cache = {
        'timestamp': datetime.now(),
        'data': data,
        'tickers': tickers_list
    }
    with open(PRICE_CACHE_FILE, 'wb') as f:
        pickle.dump(cache, f)

# ============================================
# FUNKCJE POBIERANIA DANYCH ZE STOOQ.PL
# ============================================

def fetch_from_stooq(ticker):
    """Pobiera dane historyczne ze stooq.pl dla pojedynczej spółki"""
    try:
        url = f"https://stooq.pl/q/d/l/?s={ticker.lower()}.us&i=d"
        df = pd.read_csv(url)
        
        if df.empty or len(df) < 25:
            return None
        
        # Stooq.pl ma kolumny: Data, Otwarcie, Max, Min, Zamknięcie, Wolumen
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Ticker'] = ticker
        
        return df
        
    except Exception as e:
        return None

def calculate_indicators(df):
    """Oblicza wszystkie wskaźniki dla jednej spółki"""
    try:
        if df is None or len(df) < 25:
            return None
        
        df = df.sort_values('Date').reset_index(drop=True)
        
        # --- RVOL ---
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
        
        # --- OBV ---
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
        
        # --- A/D Line ---
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
        
        # --- CMF 20 ---
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
            cmf = sum(mfv) / volume_sum if volume_sum > 0 else 0
            return cmf
        
        cmf = calculate_cmf(df, 20)
        cmf_ok = cmf > 0
        
        # --- Inne wskaźniki ---
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs.iloc[-1])) if loss.iloc[-1] != 0 else 50
        
        change_1d = ((df['Close'].iloc[-1] / df['Close'].iloc[-2] - 1) * 100) if len(df) >= 2 else 0
        change_5d = ((df['Close'].iloc[-1] / df['Close'].iloc[-5] - 1) * 100) if len(df) >= 5 else 0
        
        return {
            'Ticker': df['Ticker'].iloc[0],
            'Cena': round(df['Close'].iloc[-1], 2),
            'Wolumen': int(df['Volume'].iloc[-1]),
            'RVOL': round(today_rvol, 2),
            'Dni>2': days_over_2,
            'RVOL OK': '✅' if rvol_ok else '❌',
            'OBV': '📈' if obv_ok else '📉',
            'A/D': '📈' if ad_ok else '📉',
            'CMF': round(cmf, 3),
            'Flow OK': '✅' if (obv_ok and ad_ok and cmf_ok) else '❌',
            'RSI': round(rsi, 1),
            'Zmiana 1d': round(change_1d, 2),
            'Zmiana 5d': round(change_5d, 2),
            'Data': datetime.now().strftime('%Y-%m-%d %H:%M')
        }
        
    except Exception as e:
        return None

# ============================================
# GŁÓWNA FUNKCJA SKANOWANIA
# ============================================

def scan_stocks(force_refresh=False):
    """
    Główna funkcja skanująca spółki
    """
    
    # KROK 1: Pobierz aktualną listę spółek (ZAWSZE ŚWIEŻA!)
    with st.spinner("📡 Pobieranie aktualnej listy spółek NASDAQ..."):
        current_tickers = get_live_nasdaq_tickers()
    
    st.info(f"📊 Znaleziono {len(current_tickers)} aktywnych spółek")
    
    # KROK 2: Sprawdź cache
    cached = load_price_cache()
    
    if cached and not force_refresh:
        cached_tickers = set(cached['tickers'])
        current_set = set(current_tickers)
        
        # Znajdź różnice
        new_tickers = current_set - cached_tickers
        removed_tickers = cached_tickers - current_set
        
        if new_tickers:
            st.warning(f"🆕 Znaleziono {len(new_tickers)} NOWYCH spółek!")
        
        if removed_tickers:
            st.info(f"📉 Usunięto {len(removed_tickers)} spółek (delisting)")
        
        # Użyj cache dla starych spółek
        all_results = []
        
        # Dodaj stare spółki z cache
        old_data = [d for d in cached['data'] if d['Ticker'] in cached_tickers - removed_tickers]
        all_results.extend(old_data)
        
        # Skanuj tylko nowe spółki
        if new_tickers:
            st.warning(f"🆕 Skanowanie {len(new_tickers)} nowych spółek...")
            progress_bar = st.progress(0)
            
            for i, ticker in enumerate(new_tickers):
                df = fetch_from_stooq(ticker)
                result = calculate_indicators(df)
                if result:
                    all_results.append(result)
                
                progress_bar.progress((i + 1) / len(new_tickers))
                time.sleep(0.3)
            
            progress_bar.empty()
            
            # Zapisz zaktualizowany cache
            save_price_cache(all_results, current_tickers)
            st.success(f"✅ Zaktualizowano cache - dodano {len(new_tickers)} nowych spółek")
        
        return all_results, current_tickers
    
    else:
        # KROK 3: Pierwsze skanowanie lub force_refresh - skanuj wszystko
        st.warning(f"⏳ Pierwsze skanowanie {len(current_tickers)} spółek (to zajmie ~15-20 minut)...")
        
        estimated = len(current_tickers) // 60  # ~60 spółek na minutę
        st.info(f"⏱️ Szacowany czas: około {estimated} minut")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        results = []
        start_time = time.time()
        
        for i, ticker in enumerate(current_tickers):
            status_text.text(f"Skanowanie {i+1}/{len(current_tickers)}: {ticker}")
            
            df = fetch_from_stooq(ticker)
            result = calculate_indicators(df)
            if result:
                results.append(result)
            
            # Aktualizuj progress
            progress_bar.progress((i + 1) / len(current_tickers))
            
            # Pokaż estymację czasu
            if i > 0 and i % 50 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                remaining = (len(current_tickers) - i) / rate
                st.caption(f"⏱️ Pozostało około {int(remaining//60)}m {int(remaining%60)}s")
            
            time.sleep(0.2)
        
        progress_bar.empty()
        status_text.empty()
        
        # Zapisz do cache
        save_price_cache(results, current_tickers)
        st.success(f"✅ Zakończono! Znaleziono {len(results)} spółek z danymi")
        
        return results, current_tickers

# ============================================
# INTERFEJS UŻYTKOWNIKA
# ============================================

st.markdown('<h1 class="main-header">📊 NASDAQ Live Stock Scanner</h1>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry")
    
    st.markdown("---")
    st.subheader("📋 Źródło danych")
    st.info("✅ Lista spółek: NASDAQ Trader (na żywo)")
    st.info("✅ Dane cenowe: stooq.pl + cache")
    
    st.markdown("---")
    
    # Filtry RVOL
    st.subheader("📊 RVOL")
    use_rvol = st.checkbox("Filtruj RVOL", value=True)
    if use_rvol:
        st.caption("Warunek: >2 dziś i ≥2/4 dni")
    
    # Filtry przepływu
    st.subheader("💰 OBV/A/D/CMF")
    use_flow = st.checkbox("Filtruj wskaźniki >0", value=True)
    
    # Filtry cenowe
    st.subheader("💰 Cena")
    min_price = st.number_input("Min cena ($)", 0.0, 1000.0, 1.0)
    max_price = st.number_input("Max cena ($)", 0.0, 10000.0, 500.0)
    
    st.markdown("---")
    
    # Przyciski
    col1, col2 = st.columns(2)
    with col1:
        scan_btn = st.button("🔍 Skanuj", type="primary", use_container_width=True)
    with col2:
        refresh_btn = st.button("🔄 Nowe dane", use_container_width=True)

# Główna logika
if scan_btn or refresh_btn:
    force_refresh = refresh_btn
    
    with st.spinner("Inicjowanie skanowania..."):
        results, current_tickers = scan_stocks(force_refresh)
    
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
            f"nasdaq_live_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            "text/csv"
        )
        
    else:
        st.warning("Nie znaleziono spółek. Spróbuj rozszerzyć filtry.")

# Instrukcja
with st.expander("ℹ️ Jak to działa?"):
    st.markdown("""
    ### 🔄 Aktualność danych:
    
    **Lista spółek:** Pobierana na żywo z NASDAQ Trader przy każdym skanowaniu
    - ✅ Zawsze aktualna
    - ✅ Nowe IPO widoczne następnego dnia
    - ✅ Usunięte spółki automatycznie znikają
    
    **Dane cenowe:** Cache'owane przez 24h
    - ✅ Pierwsze skanowanie: ~15-20 minut
    - ✅ Kolejne skanowania: błyskawiczne (sekundy)
    - ✅ Nowe spółki są automatycznie doskanowywane
    
    ### 📊 Filtry:
    - **RVOL > 2** dzisiaj i w ≥2 z ostatnich 4 dni
    - **OBV, A/D, CMF > 0** (wszystkie trzy)
    - Filtry cenowe (min/max)
    
    ### 💾 Cache:
    - Przechowuje dane przez 24h
    - Automatycznie aktualizuje nowe spółki
    - Możesz wymusić odświeżenie przyciskiem "Nowe dane"
    """)