# app.py
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
from io import StringIO

# Konfiguracja strony
st.set_page_config(
    page_title="NASDAQ Full Scanner - Wszystkie spółki",
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
        margin: 0.5rem;
        text-align: center;
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
    .indicator-positive {
        color: #00C853;
        font-weight: bold;
    }
    .indicator-negative {
        color: #D32F2F;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# === FUNKCJE DO POBIERANIA PEŁNEJ LISTY NASDAQ ===

@st.cache_data(ttl=86400)  # Cache przez 24 godziny
def get_all_nasdaq_tickers():
    """
    Pobiera pełną listę wszystkich spółek notowanych na NASDAQ
    Źródło: Oficjalne dane NASDAQ Trader
    """
    tickers = []
    
    with st.spinner("📡 Pobieranie pełnej listy spółek NASDAQ..."):
        try:
            # Źródło 1: NASDAQ Trader (oficjalne)
            url1 = "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqtraded.txt"
            url2 = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
            
            # Próba 1: FTP
            try:
                df = pd.read_csv(url1, sep='|')
                st.success("✅ Pobrano listę z NASDAQ Trader (FTP)")
            except:
                # Próba 2: HTTP
                try:
                    df = pd.read_csv(url2, sep='|')
                    st.success("✅ Pobrano listę z NASDAQ Trader (HTTP)")
                except:
                    # Próba 3: Backup z GitHub
                    backup_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt"
                    response = requests.get(backup_url)
                    if response.status_code == 200:
                        tickers = response.text.strip().split('\n')
                        st.success(f"✅ Pobrano listę z backupu ({len(tickers)} spółek)")
                        return [t.strip().upper() for t in tickers if t.strip()]
                    else:
                        raise Exception("Wszystkie źródła zawiodły")
            
            # Filtruj dane z NASDAQ Trader
            if 'df' in locals():
                # Filtruj tylko akcje zwykłe (pomijamy ETFy, fundusze itp.)
                stocks = df[
                    (df['ETF'] == 'N') &  # Nie ETF
                    (df['TEST ISSUE'] == 'N') &  # Nie testowe
                    (df['FINANCIAL_STATUS'].notna())  # Aktywne spółki
                ]
                
                # Pobierz tickery
                tickers = stocks['NASDAQ Symbol'].tolist()
                
                # Usuń duplikaty i wartości NaN
                tickers = [t for t in tickers if str(t) != 'nan']
                tickers = list(set(tickers))
                
                st.success(f"✅ Znaleziono {len(tickers)} aktywnych spółek na NASDAQ")
        
        except Exception as e:
            st.warning(f"⚠️ Błąd pobierania pełnej listy: {str(e)}")
            st.info("📋 Używam listy zapasowej 500+ spółek")
            tickers = get_extended_ticker_list()
    
    return sorted(tickers)

def get_extended_ticker_list():
    """
    Rozszerzona lista zapasowa (~500 spółek) na wypadek problemów
    """
    # Połączenie różnych źródeł
    base_tickers = [
        # Technologia (200+)
        "AAPL", "MSFT", "GOOGL", "GOOG", "META", "NVDA", "AMD", "INTC",
        "CSCO", "ADBE", "CRM", "ORCL", "IBM", "QCOM", "TXN", "AVGO",
        "AMAT", "MU", "NXPI", "KLAC", "LRCX", "ASML", "SNPS", "CDNS",
        "ADI", "MCHP", "ON", "SWKS", "QRVO", "MPWR", "WOLF", "LSCC",
        "FLEX", "JBL", "KEYS", "TER", "ENTG", "CREE", "UCTT", "ACLS",
        "AMKR", "CBT", "COHU", "CYBE", "DIOD", "FORM", "IPGP", "KLIC",
        "MKSI", "PLAB", "RGTI", "SMTC", "UCTT", "VEON", "VECO", "WFRD",
        
        # Software i usługi IT (150+)
        "INTU", "NOW", "PANW", "FTNT", "CRWD", "ZS", "OKTA", "DDOG",
        "MDB", "SNOW", "PLTR", "NET", "RBLX", "U", "PATH", "AI",
        "DOCU", "TWLO", "ZM", "TEAM", "WDAY", "VEEV", "PAYC", "COUP",
        "ESTC", "FIVN", "MEDP", "PCTY", "QLYS", "TYL", "VRSN", "CHKP",
        "FFIV", "AKAM", "CDW", "EPAM", "GIB", "BR", "FISV", "GPN",
        "WEX", "JKHY", "ENV", "EVTC", "EXLS", "FICO", "FOUR", "GLOB",
        
        # E-commerce i konsumenckie (100+)
        "AMZN", "TSLA", "NFLX", "PEP", "COST", "CMCSA", "TMUS", "CHTR",
        "PYPL", "BKNG", "ABNB", "RIVN", "LCID", "EBAY", "JD", "BIDU",
        "PDD", "BABA", "MELI", "SE", "CPNG", "DASH", "UBER", "LYFT",
        "ETSY", "W", "CVNA", "CARG", "ACVA", "AN", "KMX", "MUSA",
        
        # Biotechnologia i farmacja (300+)
        "GILD", "REGN", "VRTX", "MRNA", "ILMN", "BNTX", "ALNY", "BIIB",
        "AMGN", "INCY", "IONS", "SGEN", "EXAS", "NBIX", "UTHR", "SRPT",
        "VTRS", "MYGN", "QDEL", "TECH", "BIO", "WST", "IDXX", "DHR",
        "LH", "DGX", "IQV", "CRL", "MEDP", "HZNP", "ALXN", "BMRN",
        "BGNE", "BEAM", "CRSP", "NTLA", "EDIT", "VERV", "ARWR", "IONS",
        
        # Finanse i fintech (150+)
        "SQ", "SOFI", "AFRM", "UPST", "COIN", "HOOD", "MELI", "DKNG",
        "LPLA", "FIBK", "WAFD", "CWBC", "PFG", "AFL", "ALL", "AMP",
        "AON", "AXP", "BAC", "BLK", "BRK-B", "C", "CMA", "COF",
        "CS", "DFS", "FITB", "GS", "HBAN", "HIG", "JPM", "KEY",
        "MET", "MKTX", "MS", "MTB", "NDAQ", "NTRS", "PNC", "RF",
        
        # Media i rozrywka (80+)
        "ROKU", "TTWO", "EA", "ZG", "Z", "ANGI", "IAC", "MTCH",
        "SPOT", "WBD", "PARA", "DIS", "FOX", "FOXA", "NWSA", "NWS",
        "LYV", "MSGS", "BATRA", "BATRK", "FWONA", "FWONK", "LSXMA",
        
        # Przemysł i transport (100+)
        "ODFL", "OLD", "PCAR", "PINC", "PKG", "PNR", "PODD", "POOL",
        "PPG", "PPL", "PRGO", "PRI", "PRU", "PSA", "PSX", "PVH",
        "QRVO", "RCL", "RE", "REG", "REGN", "RF", "RHI", "RJF",
        "RL", "RMD", "ROK", "ROL", "ROP", "ROST", "RSG", "RTX",
        
        # Energia (50+)
        "XOM", "CVX", "COP", "EOG", "SLB", "HAL", "BKR", "OXY",
        "VLO", "MPC", "PSX", "KMI", "WMB", "OKE", "TRGP", "LNG",
        
        # Dodatkowe małe spółki
        "AAOI", "AAON", "AAT", "AAWW", "ABCB", "ABCL", "ABCM", "ABEO",
        "ABGI", "ABIO", "ABMD", "ABNB", "ABOS", "ABR", "ABSI", "ABT",
        "ABTX", "ABUS", "ACAD", "ACAH", "ACB", "ACBA", "ACCD", "ACER",
        "ACET", "ACEV", "ACGL", "ACHV", "ACIU", "ACIW", "ACLS", "ACMR",
        "ACNB", "ACOR", "ACRS", "ACRX", "ACST", "ACT", "ACTG", "ACVA",
        "ACXP", "ADAG", "ADAP", "ADBE", "ADC", "ADCT", "ADD", "ADEA",
        "ADER", "ADES", "ADGI", "ADGM", "ADIA", "ADIL", "ADIV", "ADM",
        "ADMA", "ADMP", "ADMS", "ADN", "ADNT", "ADOC", "ADP", "ADPT",
        "ADRA", "ADRT", "ADSE", "ADSK", "ADSN", "ADTH", "ADTN", "ADTX",
        "ADUS", "ADV", "ADVM", "ADWN", "ADX", "ADXN", "AE", "AEAE"
    ]
    
    return sorted(list(set(base_tickers)))

def fetch_stock_data(ticker, filters):
    """
    Pobiera dane dla pojedynczej spółki z uwzględnieniem wszystkich filtrów
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Podstawowe dane
        current_price = info.get('regularMarketPrice', info.get('currentPrice', 0))
        
        # Filtry cenowe
        if current_price < filters['min_price'] or current_price > filters['max_price']:
            return None
        
        # Pobierz dane historyczne
        hist = stock.history(period="2mo")
        
        if hist.empty or len(hist) < 25:
            return None
        
        # --- OBLICZANIE OBV ---
        obv = [0]
        for i in range(1, len(hist)):
            if hist['Close'].iloc[i] > hist['Close'].iloc[i-1]:
                obv.append(obv[-1] + hist['Volume'].iloc[i])
            elif hist['Close'].iloc[i] < hist['Close'].iloc[i-1]:
                obv.append(obv[-1] - hist['Volume'].iloc[i])
            else:
                obv.append(obv[-1])
        
        hist['OBV'] = obv
        obv_slope = np.polyfit(range(min(20, len(hist))), hist['OBV'].tail(20), 1)[0]
        obv_positive = obv_slope > 0
        
        # --- OBLICZANIE A/D LINE ---
        def calculate_ad_line(data):
            ad_line = [0]
            for i in range(1, len(data)):
                if data['High'].iloc[i] != data['Low'].iloc[i]:
                    clv = ((data['Close'].iloc[i] - data['Low'].iloc[i]) - 
                           (data['High'].iloc[i] - data['Close'].iloc[i])) / \
                           (data['High'].iloc[i] - data['Low'].iloc[i])
                else:
                    clv = 0
                money_flow = clv * data['Volume'].iloc[i]
                ad_line.append(ad_line[-1] + money_flow)
            return ad_line
        
        hist['A/D'] = calculate_ad_line(hist)
        ad_slope = np.polyfit(range(min(20, len(hist))), hist['A/D'].tail(20), 1)[0]
        ad_positive = ad_slope > 0
        
        # --- OBLICZANIE CMF 20 ---
        def calculate_cmf(data, period=20):
            if len(data) < period:
                return 0
            
            mfv = []
            for i in range(-period, 0):
                if data['High'].iloc[i] != data['Low'].iloc[i]:
                    clv = ((data['Close'].iloc[i] - data['Low'].iloc[i]) - 
                           (data['High'].iloc[i] - data['Close'].iloc[i])) / \
                           (data['High'].iloc[i] - data['Low'].iloc[i])
                else:
                    clv = 0
                mfv.append(clv * data['Volume'].iloc[i])
            
            volume_sum = data['Volume'].iloc[-period:].sum()
            cmf = sum(mfv) / volume_sum if volume_sum > 0 else 0
            return cmf
        
        cmf_20 = calculate_cmf(hist, 20)
        cmf_positive = cmf_20 > 0
        
        # Sprawdź warunki wskaźników
        indicators_met = True
        if filters['use_indicators']:
            if not obv_positive or not ad_positive or not cmf_positive:
                indicators_met = False
        
        if filters['use_indicators'] and not indicators_met:
            return None
        
        # --- OBLICZANIE RVOL ---
        avg_volume_20d = hist['Volume'].tail(20).mean()
        
        rvol_values = []
        price_changes = []
        
        for i in range(1, 6):
            if len(hist) >= i:
                daily_volume = hist['Volume'].iloc[-i]
                daily_close = hist['Close'].iloc[-i]
                daily_open = hist['Open'].iloc[-i]
                
                if avg_volume_20d > 0:
                    rvol = daily_volume / avg_volume_20d
                    rvol_values.append(rvol)
                
                daily_change = ((daily_close / daily_open - 1) * 100)
                price_changes.append(daily_change)
        
        # Sprawdź RVOL
        current_rvol = rvol_values[0] if rvol_values else 0
        rvol_over_threshold = current_rvol > filters['rvol_threshold']
        days_over_2 = sum(1 for rvol in rvol_values[1:5] if rvol > filters['rvol_threshold'])
        rvol_condition_met = days_over_2 >= 2
        
        if filters['use_rvol']:
            if not rvol_over_threshold or not rvol_condition_met:
                return None
        
        # --- SPRAWDZANIE STABILNOŚCI CENY ---
        price_stability_met = True
        if filters['use_price_stability'] and len(price_changes) >= 5:
            last_4_changes = price_changes[1:5]
            for change in last_4_changes:
                if change < filters['max_drop'] or change > filters['max_rise']:
                    price_stability_met = False
                    break
        
        if filters['use_price_stability'] and not price_stability_met:
            return None
        
        # --- OBLICZ POZOSTAŁE WSKAŹNIKI ---
        # RSI
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs.iloc[-1])) if not loss.iloc[-1] == 0 else 50
        
        # SMA
        sma_20 = hist['Close'].rolling(window=20).mean().iloc[-1]
        sma_50 = hist['Close'].rolling(window=50).mean().iloc[-1] if len(hist) >= 50 else None
        
        # Zmiany
        change_1d = ((hist['Close'].iloc[-1] / hist['Close'].iloc[-2] - 1) * 100) if len(hist) >= 2 else 0
        change_5d = ((hist['Close'].iloc[-1] / hist['Close'].iloc[-5] - 1) * 100) if len(hist) >= 5 else 0
        
        # Zakres cen
        last_4_prices = hist['Close'].iloc[-5:-1] if len(hist) >= 5 else []
        min_price_4d = min(last_4_prices) if not last_4_prices.empty else current_price
        max_price_4d = max(last_4_prices) if not last_4_prices.empty else current_price
        range_4d = ((max_price_4d / min_price_4d - 1) * 100) if min_price_4d > 0 else 0
        
        # Nazwa spółki
        company_name = info.get('longName', info.get('shortName', 'Brak nazwy'))
        if len(company_name) > 40:
            company_name = company_name[:40] + "..."
        
        return {
            'Ticker': ticker,
            'Nazwa': company_name,
            'Sektor': info.get('sector', 'Nieznany'),
            'Cena ($)': round(current_price, 2),
            'Kapitalizacja (mld)': round(info.get('marketCap', 0) / 1e9, 2) if info.get('marketCap') else 0,
            
            # Wskaźniki przepływu
            'OBV': '📈' if obv_positive else '📉',
            'A/D': '📈' if ad_positive else '📉',
            'CMF': round(cmf_20, 3),
            'Flow OK': '✅' if indicators_met else '❌',
            
            # RVOL
            'RVOL': round(current_rvol, 2),
            'Dni>2': days_over_2,
            'RVOL OK': '✅' if rvol_condition_met else '❌',
            
            # Cena
            'Zmiana 1d': round(change_1d, 2),
            'Zakres 4d': round(range_4d, 2),
            'Cena OK': '✅' if price_stability_met else '❌',
            
            # Techniczne
            'RSI': round(rsi, 1),
            'SMA20': round(sma_20, 2) if sma_20 else 0,
            'Zmiana 5d': round(change_5d, 2),
            
            'Data': datetime.now().strftime('%H:%M')
        }
        
    except Exception as e:
        return None

def scan_stocks(tickers, filters, max_workers=15):
    """Skanuje spółki równolegle z optymalizacją"""
    results = []
    total = len(tickers)
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    time_text = st.empty()
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_stock_data, ticker, filters): ticker 
                  for ticker in tickers}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                results.append(result)
            
            # Aktualizuj co 25 spółek
            if i % 25 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                remaining = (total - (i + 1)) / rate if rate > 0 else 0
                
                progress_bar.progress((i + 1) / total)
                status_text.text(f"Skanowanie: {i+1}/{total} | Znaleziono: {len(results)}")
                time_text.text(f"⏱️ Szybkość: {rate:.1f}/s | Pozostało: {remaining:.0f}s")
    
    progress_bar.empty()
    status_text.empty()
    time_text.empty()
    
    return results

# Główny interfejs
st.markdown('<h1 class="main-header">📊 NASDAQ Full Scanner - Wszystkie spółki</h1>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/stock-exchange.png", width=80)
    st.header("🔍 Filtry wyszukiwania")
    
    # Wybór listy spółek
    st.subheader("📋 Lista spółek")
    ticker_option = st.radio(
        "Wybierz zakres:",
        ["🚀 Szybkie skanowanie (500+ spółek)", "🌐 Pełna NASDAQ (~3500 spółek)"]
    )
    
    if ticker_option == "🚀 Szybkie skanowanie (500+ spółek)":
        tickers = get_extended_ticker_list()
        st.success(f"✅ {len(tickers)} spółek (skanowanie ~2-3 minuty)")
    else:
        tickers = get_all_nasdaq_tickers()
        st.info(f"📊 {len(tickers)} spółek (skanowanie ~10-15 minut)")
    
    st.markdown("---")
    
    # Filtry wskaźników
    st.subheader("💰 Wskaźniki przepływu")
    use_indicators = st.checkbox("Filtruj OBV/A/D/CMF > 0", value=True)
    
    st.markdown("---")
    
    # Filtry RVOL
    st.subheader("📊 Relative Volume")
    use_rvol = st.checkbox("Filtruj RVOL", value=True)
    if use_rvol:
        rvol_threshold = st.slider("Próg RVOL", 1.0, 5.0, 2.0, 0.1)
    
    st.markdown("---")
    
    # Filtry stabilności
    st.subheader("📈 Stabilność ceny")
    use_price_stability = st.checkbox("Filtruj stabilność", value=True)
    if use_price_stability:
        col1, col2 = st.columns(2)
        with col1:
            max_drop = st.number_input("Max spadek (%)", -20.0, 0.0, -3.0, 0.5)
        with col2:
            max_rise = st.number_input("Max wzrost (%)", 0.0, 20.0, 7.0, 0.5)
    
    st.markdown("---")
    
    # Filtry cenowe i kapitalizacja
    st.subheader("💰 Filtry podstawowe")
    
    col1, col2 = st.columns(2)
    with col1:
        min_price = st.number_input("Min cena ($)", 0.0, 1000.0, 1.0, 0.5)
    with col2:
        max_price = st.number_input("Max cena ($)", 0.0, 10000.0, 500.0, 10.0)
    
    min_mcap = st.number_input("Min kapitalizacja (mld $)", 0.0, 1000.0, 0.0, 0.1)
    
    scan_button = st.button("🔍 Rozpocznij skanowanie", type="primary", use_container_width=True)

# Główna logika
if scan_button:
    filters = {
        'use_indicators': use_indicators,
        'use_rvol': use_rvol,
        'rvol_threshold': rvol_threshold if use_rvol else 2.0,
        'use_price_stability': use_price_stability,
        'max_drop': max_drop if use_price_stability else -100,
        'max_rise': max_rise if use_price_stability else 100,
        'min_price': min_price,
        'max_price': max_price,
        'min_mcap': min_mcap * 1e9
    }
    
    # Oszacowanie czasu
    if len(tickers) > 2000:
        st.warning(f"⚠️ Skanowanie {len(tickers)} spółek zajmie około 10-15 minut. Możesz zrobić sobie kawę ☕")
    
    with st.spinner("Rozpoczynam skanowanie..."):
        results = scan_stocks(tickers, filters)
    
    if results:
        df = pd.DataFrame(results)
        
        # Filtruj po kapitalizacji
        if min_mcap > 0:
            df = df[df['Kapitalizacja (mld)'] >= min_mcap]
        
        # Statystyki
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 1.8rem;">{len(df)}</div>
                <div>Znalezionych spółek</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            flow_ok = len(df[df['Flow OK'] == '✅'])
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 1.8rem;">{flow_ok}</div>
                <div>Z dobrym przepływem</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            rvol_ok = len(df[df['RVOL OK'] == '✅'])
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 1.8rem;">{rvol_ok}</div>
                <div>Z RVOL > {rvol_threshold}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            price_ok = len(df[df['Cena OK'] == '✅'])
            st.markdown(f"""
            <div class="stat-card">
                <div style="font-size: 1.8rem;">{price_ok}</div>
                <div>Stabilne cenowo</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Zakładki
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 Wszystkie wyniki",
            "💰 Przepływ pieniędzy",
            "📊 RVOL i cena",
            "🎯 Spełniające wszystkie"
        ])
        
        with tab1:
            st.dataframe(
                df.sort_values('RVOL', ascending=False),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Cena ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Kapitalizacja (mld)": st.column_config.NumberColumn(format="$%.2f mld"),
                    "CMF": st.column_config.NumberColumn(format="%.3f"),
                    "RSI": st.column_config.NumberColumn(format="%.1f"),
                    "Zmiana 1d": st.column_config.NumberColumn(format="%.1f%%"),
                    "Zmiana 5d": st.column_config.NumberColumn(format="%.1f%%")
                }
            )
            
            # Eksport
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Pobierz CSV",
                csv,
                f"nasdaq_full_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv"
            )
        
        with tab2:
            flow_stocks = df[df['Flow OK'] == '✅'].sort_values('CMF', ascending=False)
            if not flow_stocks.empty:
                st.success(f"✅ {len(flow_stocks)} spółek z OBV, A/D i CMF > 0")
                
                # Wykres CMF
                fig = px.bar(
                    flow_stocks.head(20),
                    x='Ticker',
                    y='CMF',
                    color='CMF',
                    color_continuous_scale='RdYlGn',
                    title="Top 20 spółek według CMF",
                    hover_data=['Nazwa', 'Cena ($)', 'RVOL', 'Kapitalizacja (mld)']
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(
                    flow_stocks[['Ticker', 'Nazwa', 'OBV', 'A/D', 'CMF', 'RVOL', 'Cena ($)']],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("Brak spółek z OBV, A/D i CMF > 0")
        
        with tab3:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏆 Najwyższy RVOL")
                rvol_top = df.nlargest(15, 'RVOL')[['Ticker', 'Nazwa', 'RVOL', 'Dni>2', 'Zmiana 1d']]
                st.dataframe(rvol_top, use_container_width=True, hide_index=True)
            
            with col2:
                st.subheader("📊 Najwęższy zakres cen")
                range_top = df.nsmallest(15, 'Zakres 4d')[['Ticker', 'Nazwa', 'Zakres 4d', 'Cena ($)', 'RSI']]
                st.dataframe(range_top, use_container_width=True, hide_index=True)
        
        with tab4:
            perfect_stocks = df[
                (df['Flow OK'] == '✅') & 
                (df['RVOL OK'] == '✅') & 
                (df['Cena OK'] == '✅')
            ].sort_values('CMF', ascending=False)
            
            if not perfect_stocks.empty:
                st.balloons()
                st.success(f"🎯 Znaleziono {len(perfect_stocks)} spółek spełniających WSZYSTKIE kryteria!")
                
                for _, row in perfect_stocks.iterrows():
                    with st.container():
                        cols = st.columns([1, 2, 1, 1, 1, 1])
                        
                        with cols[0]:
                            st.markdown(f"**{row['Ticker']}**")
                        with cols[1]:
                            st.markdown(row['Nazwa'][:25])
                        with cols[2]:
                            st.markdown(f"${row['Cena ($)']}")
                        with cols[3]:
                            st.markdown(f"CMF: {row['CMF']}")
                        with cols[4]:
                            st.markdown(f"RVOL: {row['RVOL']}x")
                        with cols[5]:
                            change_color = "green" if row['Zmiana 1d'] > 0 else "red"
                            st.markdown(f"<span style='color:{change_color}'>{row['Zmiana 1d']}%</span>", 
                                      unsafe_allow_html=True)
                        
                        st.markdown("---")
            else:
                st.warning("Brak spółek spełniających wszystkie kryteria")
        
    else:
        st.warning("Nie znaleziono spółek spełniających kryteria. Spróbuj rozszerzyć filtry.")

# Instrukcja
with st.expander("ℹ️ Instrukcja i informacje"):
    st.markdown("""
    ### 📋 Jak używać:
    
    1. **Wybierz zakres** - szybkie skanowanie (500+) lub pełna NASDAQ (3500+)
    2. **Ustaw filtry** w panelu bocznym
    3. **Kliknij "Rozpocznij skanowanie"**
    4. **Przeglądaj wyniki** w zakładkach
    
    ### ⏱️ Czas skanowania:
    - **Szybkie (500+ spółek)** - około 2-3 minut
    - **Pełne (3500+ spółek)** - około 10-15 minut
    
    ### 📊 Interpretacja wskaźników:
    
    **💰 Wskaźniki przepływu (>0 = bycze):**
    - **OBV** - On-Balance Volume (trend rosnący)
    - **A/D** - Accumulation/Distribution (trend rosnący)  
    - **CMF** - Chaikin Money Flow (>0 = napływ kapitału)
    
    **📊 RVOL:**
    - > 2.0 = silne zainteresowanie instytucjonalne
    - > 5.0 = ekstremalne (może być wyczerpanie)
    
    **📈 Stabilność ceny:**
    - Żaden dzień nie spadł więcej niż -3%
    - Żaden dzień nie wzrósł więcej niż +7%
    - Szukamy spokojnej akumulacji
    
    ### 🌐 Źródła danych:
    - Lista spółek: NASDAQ Trader (oficjalne dane)
    - Dane finansowe: Yahoo Finance API
    - Aktualizacja: na żywo
    """)