import requests
import re
import pandas as pd
from io import StringIO
import time
from datetime import datetime
import os

def get_encparam(code, session):
    url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    }
    try:
        res = session.get(url, headers=headers, timeout=10)
        match = re.search(r"encparam: '([^']+)'", res.text)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None

def get_financial_data(code, name, session):
    encparam = get_encparam(code, session)
    if not encparam:
        return None
    
    url_ajax = "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx"
    params = {
        'cmp_cd': code,
        'fin_typ': '0', # Consolidated
        'freq_typ': 'Y', # Annual
        'encparam': encparam
    }
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
    }
    
    try:
        res = session.get(url_ajax, params=params, headers=headers, timeout=10)
        if res.status_code != 200:
            return None
        
        dfs = pd.read_html(StringIO(res.text))
        if len(dfs) < 2:
            return None
            
        df = dfs[1]
        
        # Simplify Columns (Handle nested columns)
        new_columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                val = col[1]
                date_match = re.search(r'\d{4}/\d{2}', val)
                if date_match:
                    new_columns.append(date_match.group(0)) # e.g., 2026/12
                else:
                    new_columns.append(col[0])
            else:
                new_columns.append(col)
        
        df.columns = new_columns
        df.set_index(df.columns[0], inplace=True)
        return df

    except Exception:
        return None

def get_tickers_from_naver():
    tickers = []
    # KOSPI (0) & KOSDAQ (1)
    markets = [(0, 40), (1, 40)] 
    
    print("Crawling tickers & prices from Naver Ranking...")
    session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for sosok, max_page in markets:
        market_name = "KOSPI" if sosok == 0 else "KOSDAQ"
        print(f"Scanning {market_name}...")
        
        for page in range(1, max_page + 1):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = session.get(url, headers=headers)
                # Use pandas to parse the huge table
                dfs = pd.read_html(StringIO(res.text))
                
                if len(dfs) < 2: 
                    # Usually the main table is the second one (index 1), but let's check
                    # Sometimes Naver layout changes, but reliable enough
                    continue
                
                df = dfs[1]
                # Filter rows using 'N' column (which is rank number)
                # Valid rows have a numeric 'N'
                df = df[pd.to_numeric(df['N'], errors='coerce').notnull()]
                
                if df.empty:
                    break
                
                # Extract Code. Note: pd.read_html doesn't give us the link (Code).
                # We still need regex or valid mapping for Code.
                # Actually, scraping codes via regex is robust, but mapping them to this DF is tricky if mismatched.
                # Let's use the regex to get (Code, Name) in order, and assume the DF order matches.
                # Since we iterate pages, the order should match EXACTLY.
                
                pattern = r'<a href="/item/main.naver\?code=(\d{6})" class="tltle">([^<]+)</a>'
                matches = re.findall(pattern, res.text)
                
                if not matches or len(matches) != len(df):
                    # Fallback or strict check. 
                    # If mismatch, maybe just trust regex for Code/Name and ignore price? No, we need Price.
                    # Usually matches are robust.
                    pass
                
                # Let's iterate matches and the DF rows
                # df['종목명'] should match name roughly
                
                # Actually, simpler approach:
                # Just loop through matches (Code, Name) and define a map.
                # But we need Price.
                # Let's zip them.
                
                # "현재가" column exists?
                if '현재가' not in df.columns:
                    continue
                    
                prices = df['현재가'].tolist()
                
                # matches might contain duplicates if Naver html is weird, but usually distinct on rankings.
                # Min length to zip
                limit = min(len(matches), len(prices))
                
                for i in range(limit):
                    code, name = matches[i]
                    price = prices[i]
                    tickers.append({'Code': code, 'Name': name, 'CurrentPrice': price})
                
                time.sleep(0.05)
                
            except Exception as e:
                print(f"  Page {page} error: {e}")
                
    return pd.DataFrame(tickers).drop_duplicates(subset=['Code'])

def main():
    # 1. Get Tickers
    krx_df = get_tickers_from_naver()
    print(f"Total Companies Found: {len(krx_df)}")

    # 2. Fetch Financials
    all_data = []
    session = requests.Session()
    
    # We want these specific metrics
    target_metrics = ['EPS', 'PER', 'BPS', 'PBR', 'DPS', '배당수익률', '발행주식수']
    # Mapping might be needed if exact names differ in table, but usually they contain these strings
    
    records = []

    print("Fetching financial data...")
    count = 0
    total = len(krx_df)
    
    for _, row in krx_df.iterrows():
        code = row['Code']
        name = row['Name']
        count += 1
        
        if count % 10 == 0:
            print(f"[{count}/{total}] Processing {name}...", end='\r')

        try:
            df = get_financial_data(code, name, session)
            if df is None:
                continue

            # Process DF into Long Format for our targets
            # Rows are Metrics, Cols are Dates (YYYY/MM)
            
            for date_col in df.columns:
                # We are interested in 2020 through 2026 (including Estimates)
                # date_col format: "2020/12", "2026/12(E)", etc.
                year_match = re.search(r'202[0-6]', date_col)
                if not year_match:
                    continue
                
                year = year_match.group(0) # 2020, 2021, ... 2026
                
                for metric in target_metrics:
                    # Find row that contains this metric name
                    # Naver Table index examples: "EPS(원)", "PER(배)"
                    found_val = None
                    for idx in df.index:
                        if metric == 'DPS' and '현금DPS' in str(idx):
                             found_val = df.loc[idx, date_col]
                             break
                        elif metric in str(idx):
                            found_val = df.loc[idx, date_col]
                            break
                    
                    if found_val is not None:
                         records.append({
                             'Code': code,
                             'Name': name,
                             'Year': year,
                             'Metric': metric,
                             'Value': found_val
                         })
            
            time.sleep(0.05)

            if 'CurrentPrice' in row:
                records.append({
                     'Code': code,
                     'Name': name,
                     'Year': 'Current',
                     'Metric': 'CurrentPrice',
                     'Value': row['CurrentPrice']
                })
            # --- NEW: Fetch Historical Prices (Jan 10th of each year) ---
            # URL: https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=2200&requestType=0
            # Returns XML with daily OHLCV
            try:
                chart_url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=2200&requestType=0"
                c_res = session.get(chart_url, timeout=5)
                
                # Simple parsing without lxml for speed if possible, but we have lxml installed
                # <item data="20200110|59500|60400|59500|59500|40243"/> (Date|Open|High|Low|Close|Vol)
                
                # We need prices for: Jan 10 of 2020, 2021, 2022, 2023, 2024, 2025, 2026
                # Since Jan 10 might be holiday, we take nearest date <= Jan 10.
                
                raw_xml = c_res.text
                items = re.findall(r'item data=\"([^\"]+)\"', raw_xml)
                
                # Create a simple dict: Date(YYYYMMDD) -> ClosePrice
                price_map = {}
                sorted_dates = []
                
                for item in items:
                    parts = item.split('|')
                    if len(parts) >= 5:
                        d = parts[0]
                        c = int(parts[4])
                        price_map[d] = c
                        sorted_dates.append(d)
                
                sorted_dates.sort() # Ensure sorted
                
                today_md = datetime.now().strftime("%m%d") # 0110
                
                target_years = range(2020, 2027)
                for y in target_years:
                    target_date_val = int(f"{y}{today_md}") # e.g. 20200110
                    
                    # Find nearest date <= target_date_val
                    # Since sorted_dates is sorted, we can iterate or bisect.
                    # Simple linear search backwards is fine for small list.
                    
                    found_price = None
                    found_date = None
                    
                    # Optimization: Filter list to those <= target
                    candidates = [d for d in sorted_dates if int(d) <= target_date_val]
                    if candidates:
                        found_date = candidates[-1] # closest one
                        found_price = price_map[found_date]
                    
                    if found_price is not None:
                         records.append({
                             'Code': code,
                             'Name': name,
                             'Year': str(y),
                             'Metric': 'FixedDatePrice', # Special metric name
                             'Value': found_price
                         })
            except Exception as e:
                # print(f"Chart Error {name}: {e}")
                pass

        except Exception as e:
            # print(f"Error {name}: {e}")
            pass

    print("\nSaving raw data...")
    raw_df = pd.DataFrame(records)
    raw_df.to_csv("stock_data_raw.csv", index=False, encoding='utf-8-sig')
    print("Done. Saved to stock_data_raw.csv")

if __name__ == "__main__":
    main()
