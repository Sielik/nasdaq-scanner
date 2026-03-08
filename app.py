import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import time
import os
import requests
from io import StringIO

# Konfiguracja strony
st.set_page_config(
    page_title="NASDAQ Scanner - stooq.pl",
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
</style>
""", unsafe_allow_html=True)

# Stałe
CACHE_FILE = "nasdaq_cache.csv"
CACHE_EXPIRY_HOURS = 6  # Cache ważny 6 godzin
RVOL_THRESHOLD = 2.0

# Lista spółek NASDAQ (sprawdzona, działająca)
@st.cache_data(ttl=86400)
def get_nasdaq_tickers():
    """Zwraca listę aktywnych spółek NASDAQ"""
    return [
        "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMD", "TSLA", "NFLX",
        "AMZN", "PEP", "COST", "CSCO", "ADBE", "CRM", "INTC", "QCOM",
        "GILD", "REGN", "VRTX", "MRNA", "AMGN", "BIIB", "ILMN",
        "ADI", "MCHP", "ON", "SWKS", "MPWR", "INTU", "NOW", "PANW",
        "FTNT", "CRWD", "ZS", "OKTA", "SQ", "SOFI", "ABNB", "RIVN",
        "PYPL", "BKNG", "SPOT", "UBER", "DASH", "ZM", "DOCU", "TWLO",
        "EA", "TTWO", "ROKU", "PINS", "SNAP", "PLTR", "SNOW", "MDB"
    ]

def fetch_from_stooq(ticker):
    """Pobiera dane historyczne ze stooq.pl"""
    try:
        # stooq.pl format: https://stooq.pl/q/d/l/?s=aapl.us&i=d
        url = f"https://stooq.pl/q/d/l/?s={ticker.lower()}.us&i=d"
        
        df = pd.read_csv(url)
        if df.empty or len(df) < 25:  # Minimum 25 dni do obliczeń
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
        
        # Przewijamy dane (najnowsze na końcu)
        df = df.sort_values('Date').reset_index(drop=True)
        
        # --- RVOL ---
        avg_volume = df['Volume'].tail(20).mean()
        
        # RVOL dla ostatnich 5 dni
        rvol_values = []
        for i in range(1, 6):
            if len(df) >= i:
                vol = df['Volume'].iloc[-i]
                rvol = vol / avg_volume if avg_volume > 0 else 0
                rvol_values.append(rvol)
        
        # Warunek RVOL
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
        
        # Trend OBV (ostatnie 20 dni)
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
        
        # Trend A/D
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
        # RSI
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs.iloc[-1])) if loss.iloc[-1] != 0 else 50
        
        # Zmiana procentowa
        change_1d = ((df['Close'].iloc[-1] / df['Close'].iloc[-2] - 1) * 100) if len(df) >= 2 else 0
        change_5d = ((df['Close'].iloc[-1] / df['Close'].iloc[-5] - 1) * 100) if len(df) >= 5 else 0
        
        # Nazwa spółki (dla stooq.pl nie mamy, więc użyjemy tickera)
        name = f"{df['Ticker'].iloc[0]}"
        
        return {
            'Ticker': df['Ticker'].iloc[0],
            'Nazwa': name,
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

def load_from_cache(tickers):
    """Wczytuje dane z cache jeśli są świeże"""
    if not os.path.exists(CACHE_FILE):
        return None
    
    try:
        df = pd.read_csv(CACHE_FILE)
        
        # Sprawdź timestamp
        if 'cache_time' in df.columns:
            cache_time = pd.to_datetime(df['cache_time'].iloc[0])
            age = datetime.now() - cache_time
            
            if age < timedelta(hours=CACHE_EXPIRY_HOURS):
                st.info(f"📦 Używam danych z cache (sprzed {age.seconds//60} minut)")
                return df
    except:
        pass
    
    return None

def save_to_cache(results):
    """Zapisuje wyniki do cache"""
    df = pd.DataFrame(results)
    df['cache_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df.to_csv(CACHE_FILE, index=False)

def scan_all_stocks(tickers, force_refresh=False):
    """Główna funkcja skanowania"""
    
    # Sprawdź cache
    if not force_refresh:
        cached = load_from_cache(tickers)
        if cached is not None:
            return cached.to_dict('records')
    
    # Pobierz świeże dane
    st.warning("⏳ Pobieranie świeżych danych ze stooq.pl (to zajmie ~5-10 minut)...")
    
    all_results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"Skanowanie {i+1}/{len(tickers)}: {ticker}")
        
        # Pobierz dane ze stooq
        df = fetch_from_stooq(ticker)
        
        # Oblicz wskaźniki
        result = calculate_indicators(df)
        if result:
            all_results.append(result)
        
        # Aktualizuj progress
        progress_bar.progress((i + 1) / len(tickers))
        time.sleep(0.3)  # Grzecznościowe opóźnienie
    
    progress_bar.empty()
    status_text.empty()
    
    # Zapisz do cache
    if all_results:
        save_to_cache(all_results)
        st.success(f"✅ Pobrano {len(all_results)} spółek i zapisano w cache")
    
    return all_results

# Główny interfejs
st.markdown('<h1 class="main-header">📊 NASDAQ Scanner - dane ze stooq.pl</h1>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry")
    
    # Lista spółek
    tickers = get_nasdaq_tickers()
    st.info(f"📊 Skanuję {len(tickers)} spółek")
    
    st.markdown("---")
    
    # Filtry RVOL
    st.subheader("📊 RVOL")
    use_rvol = st.checkbox("Filtruj RVOL", value=True)
    if use_rvol:
        st.info("""
        **Warunek:**
        - RVOL > 2 dzisiaj
        - RVOL > 2 w ≥2 z ostatnich 4 dni
        """)
    
    st.markdown("---")
    
    # Filtry przepływu
    st.subheader("💰 OBV/A/D/CMF")
    use_flow = st.checkbox("Filtruj wskaźniki >0", value=True)
    
    st.markdown("---")
    
    # Filtry cenowe
    st.subheader("💰 Cena")
    min_price = st.number_input("Min cena ($)", 0.0, 1000.0, 1.0)
    max_price = st.number_input("Max cena ($)", 0.0, 10000.0, 500.0)
    
    st.markdown("---")
    
    # Opcje skanowania
    col1, col2 = st.columns(2)
    with col1:
        scan_btn = st.button("🔍 Skanuj", type="primary", use_container_width=True)
    with col2:
        refresh_btn = st.button("🔄 Odśwież dane", use_container_width=True)
    
    if refresh_btn:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            st.success("Cache wyczyszczony!")
            st.rerun()

# Główna logika
if scan_btn or refresh_btn:
    force_refresh = refresh_btn
    
    with st.spinner("Skanowanie..." if not force_refresh else "Pobieranie świeżych danych..."):
        results = scan_all_stocks(tickers, force_refresh)
    
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
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(df)}</div>
                <div>Znalezionych spółek</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(df[df['RVOL OK'] == '✅'])}</div>
                <div>Z RVOL >2</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 2rem;">{len(df[df['Flow OK'] == '✅'])}</div>
                <div>Z dobrym przepływem</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Wyniki
        st.subheader("📋 Wyniki skanowania")
        
        # Sortowanie
        df = df.sort_values('RVOL', ascending=False)
        
        # Wyświetl
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
            f"nasdaq_results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            "text/csv"
        )
        
        # Top 10
        st.subheader("🏆 Top 10 spółek")
        cols = st.columns(2)
        
        with cols[0]:
            st.markdown("**Najwyższy RVOL**")
            st.dataframe(
                df[['Ticker', 'RVOL', 'CMF', 'Cena']].head(10),
                use_container_width=True,
                hide_index=True
            )
        
        with cols[1]:
            st.markdown("**Najwyższy CMF**")
            st.dataframe(
                df.sort_values('CMF', ascending=False)[['Ticker', 'CMF', 'RVOL', 'Cena']].head(10),
                use_container_width=True,
                hide_index=True
            )
        
    else:
        st.warning("Nie znaleziono spółek. Spróbuj rozszerzyć filtry.")

# Instrukcja
with st.expander("ℹ️ Instrukcja"):
    st.markdown("""
    ### 📋 Jak używać:
    
    1. **Kliknij "Skanuj"** - pierwsze skanowanie pobierze dane ze stooq.pl (5-10 minut)
    2. **Kolejne skanowania** - będą błyskawiczne (dane z cache)
    3. **Kliknij "Odśwież dane"** - jeśli chcesz świeże dane z rynku
    
    ### 📊 Warunki:
    - **RVOL > 2 dzisiaj** i w ≥2 z ostatnich 4 dni
    - **OBV, A/D, CMF > 0** (trend wzrostowy)
    
    ### 💾 Cache:
    - Dane trzymane są przez 6 godzin
    - Plik `nasdaq_cache.csv` w folderze aplikacji
    """)