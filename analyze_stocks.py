import pandas as pd
import numpy as np
import os
import datetime
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# CONFIG
TARGET_EMAIL = "chiwon7.kim@gmail.com"
TARGET_FOLDER_NAME = "Invest"
CAGR_THRESHOLD = 0.12 # 12%

def get_credentials(json_keyfile):
    if not os.path.exists(json_keyfile):
        return None
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    return Credentials.from_service_account_file(json_keyfile, scopes=scopes)

def find_folder_id(service, folder_name):
    try:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        results = service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            return None
        return items[0]['id'] # Return first match
    except Exception as e:
        print(f"Error searching folder: {e}")
        return None

def calculate_cagr(start_val, end_val, years):
    try:
        if start_val <= 0 or end_val <= 0:
            return 0  # CAGR undefined for negative/zero start
        return (end_val / start_val) ** (1 / years) - 1
    except:
        return 0

def analyze():
    print("Loading raw data...")
    if not os.path.exists("stock_data_raw.csv"):
        print("stock_data_raw.csv not found. Run fetch_data.py first.")
        return

    df = pd.read_csv("stock_data_raw.csv", dtype={'Code': str, 'Year': str})
    
    # 1. Pivot
    df_pivot = df.pivot_table(index=['Code', 'Name', 'Metric'], columns='Year', values='Value', aggfunc='first')
    df_pivot.reset_index(inplace=True)

    # Clean data
    cols = df_pivot.columns.difference(['Code', 'Name', 'Metric'])
    for col in cols:
        df_pivot[col] = df_pivot[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')
    
    print("Processing companies...")
    analyzed_rows = []
    
    grouped = df_pivot.groupby(['Code', 'Name'])
    
    for (code, name), group in grouped:
        group = group.set_index('Metric')
        years = [str(y) for y in range(2020, 2027)]
        for y in years:
            if y not in group.columns:
                group[y] = np.nan
        
        # Calculate PRICE row
        # Correction: User wants PRICE for EACH year to be the price on "Jan 10" of that year.
        # We now have 'FixedDatePrice' metric from fetch_data.py covering 2020-2026.
        
        price_vals = pd.Series(index=years, dtype='float64')

        # 1. Try to fill from 'FixedDatePrice' (Primary Source)
        if 'FixedDatePrice' in group.index:
             for y in years:
                 if y in group.columns:
                     val = group.loc['FixedDatePrice', y]
                     if pd.notna(val):
                         price_vals[y] = val
        
        # 2. Fallback to CurrentPrice for 2026 if missing (e.g. recent IPO)
        if pd.isna(price_vals['2026']) and 'CurrentPrice' in group.index and 'Current' in group.columns:
             cp = group.loc['CurrentPrice', 'Current']
             if pd.notna(cp):
                 price_vals['2026'] = cp

        # 3. Fallback to BPS*PBR for VERY old gaps if needed (Secondary)
        if 'BPS' in group.index and 'PBR' in group.index:
             bps_row = group.loc['BPS', years]
             pbr_row = group.loc['PBR', years]
             calc_price = bps_row * pbr_row
             # Only fill NaN
             price_vals = price_vals.combine_first(calc_price)

        # 4. RECALCULATE PER / PBR based on this new Fixed Price
        # Because Naver's raw "PER" is based on Year-End Price.
        # User wants "Jan 10 Price" / "Annual EPS".
        
        if 'EPS' in group.index:
             eps_row = group.loc['EPS', years]
             # Avoid div by zero
             new_per = price_vals / eps_row
             # Update PER row in group
             # We should only update where valid? Or overwrite? 
             # Let's overwrite to be consistent with the "Price" displayed.
             if 'PER' not in group.index:
                 group.loc['PER'] = np.nan
             
             # Align indexes
             common_idx = new_per.index.intersection(group.columns)
             # group.loc['PER', common_idx] = new_per[common_idx] --> This might fail if types differ
             # Safer:
             for y in common_idx:
                 if pd.notna(new_per[y]) and abs(new_per[y]) != np.inf:
                     group.loc['PER', y] = new_per[y]

        if 'BPS' in group.index:
             bps_row = group.loc['BPS', years]
             new_pbr = price_vals / bps_row
             if 'PBR' not in group.index:
                 group.loc['PBR'] = np.nan
             
             common_idx = new_pbr.index.intersection(group.columns)
             for y in common_idx:
                 if pd.notna(new_pbr[y]) and abs(new_pbr[y]) != np.inf:
                     group.loc['PBR', y] = new_pbr[y]

        price_row = pd.Series(index=group.columns, dtype='object')
        price_row[years] = price_vals
        price_row['Code'] = code
        price_row['Name'] = name
        price_row.name = 'PRICE'
        group = pd.concat([group, price_row.to_frame().T])
        group.index.name = 'Metric'

        # --- ANALYSIS ---
        # 1. Undervalued: 2026 PER < 8, PBR < 0.8
        per_2026 = group.loc['PER', '2026'] if 'PER' in group.index else np.nan
        pbr_2026 = group.loc['PBR', '2026'] if 'PBR' in group.index else np.nan
        
        is_undervalued = False
        if pd.notna(per_2026) and pd.notna(pbr_2026):
            if per_2026 < 8 and pbr_2026 < 0.8:
                is_undervalued = True

        # 2. High Growth: EPS CAGR (2021 -> 2026) > Threshold (12%)
        eps_2021 = group.loc['EPS', '2021'] if 'EPS' in group.index else np.nan
        eps_2026 = group.loc['EPS', '2026'] if 'EPS' in group.index else np.nan
        
        cagr = 0
        is_high_growth = False
        if pd.notna(eps_2021) and pd.notna(eps_2026):
            cagr = calculate_cagr(eps_2021, eps_2026, 5) 
            if cagr > CAGR_THRESHOLD:
                is_high_growth = True

        # 3. Market Cap / Attributes
        market_cap_2026 = 0
        if '발행주식수' in group.index and 'PRICE' in group.index:
             shares = group.loc['발행주식수', years].ffill().iloc[-1]
             if pd.isna(shares): shares = 0
             price_2026 = group.loc['PRICE', '2026'] 
             if pd.notna(price_2026):
                 market_cap_2026 = shares * price_2026
        
        is_large = "중소형"
        if market_cap_2026 > 10_000_000_000_000: 
             is_large = "대기업"
        elif market_cap_2026 > 5_000_000_000_000:
             is_large = "대기업"

        # --- FILL ROWS ---
        target_rows = ['BPS', 'DPS', 'EPS', 'PBR', 'PER', 'PRICE']
        for metric in target_rows:
            if metric not in group.index:
                continue
            row_data = group.loc[metric].to_dict()
            row_data['Name'] = name
            row_data['Code'] = code
            row_data['지표'] = metric
            row_data['대기업 유무'] = is_large
            row_data['PBR 기준'] = pbr_2026
            row_data['PER 기준'] = per_2026
            row_data['EPS_CAGR'] = f"{cagr:.1%}" if cagr != 0 else "-"
            row_data['저평가'] = "저평가" if is_undervalued else "-"
            row_data['고성장'] = "고성장" if is_high_growth else "-"
            analyzed_rows.append(row_data)

    final_df = pd.DataFrame(analyzed_rows)
    
    # Reorder
    year_cols = [str(y) for y in range(2020, 2027)]
    base_cols = ['Name', '지표']
    analysis_cols = ['대기업 유무', 'PBR 기준', 'PER 기준', 'EPS_CAGR', '저평가', '고성장']
    final_cols = base_cols + year_cols + analysis_cols
    final_cols = [c for c in final_cols if c in final_df.columns]
    final_df = final_df[final_cols]
    
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    filename = f"Stock_Analysis_{today_str}.xlsx"
    
    print(f"Saving to {filename}...")
    final_df.to_excel(filename, index=False)
    
    # --- GOOGLE SHEETS UPLOAD ---
    creds = get_credentials("service_account.json")
    if creds:
        print("Authenticating with Google...")
        gc = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        
        # We will use a fixed name for the online report to make manual creation easier if needed
        sheet_title = f"Stock_Analysis_Result" 
        
        # 1. Search for Folder "Invest"
        print(f"Searching for folder '{TARGET_FOLDER_NAME}'...")
        folder_id = find_folder_id(drive_service, TARGET_FOLDER_NAME)
        
        if not folder_id:
             print(f"Folder '{TARGET_FOLDER_NAME}' not found. Please create it and share with service account.")
             return

        # 2. Try to Open Existing or Create
        sh = None
        try:
            # Check if file exists in folder using Drive API (safer than gc.open which searches root)
            query = f"name='{sheet_title}' and '{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
            res = drive_service.files().list(q=query).execute()
            files = res.get('files', [])
            
            if files:
                print(f"Found existing sheet '{sheet_title}'. Updating...")
                sh = gc.open_by_key(files[0]['id'])
            else:
                print(f"Creating new sheet '{sheet_title}' in folder...")
                try:
                    sh = gc.create(sheet_title, folder_id=folder_id)
                    # 3. Share with User (Only needed if we created it)
                    print(f"Sharing with {TARGET_EMAIL}...")
                    sh.share(TARGET_EMAIL, perm_type='user', role='writer')
                except gspread.exceptions.APIError as e:
                    if "quota" in str(e).lower():
                        print("\n" + "="*60)
                        print("ERROR: Service Account Storage Quota Exceeded (Limit 0).")
                        print("The Service Account cannot CREATE files because it has no storage.")
                        print(f"SOLUTION: Please manually create an empty Google Sheet named '{sheet_title}'")
                        print(f"          inside the '{TARGET_FOLDER_NAME}' folder.")
                        print("          Then run this script again. The script can EDIT files you own.")
                        print("="*60 + "\n")
                        return
                    else:
                        raise e
            
            # 4. Update Content
            print("Uploading data...")
            ws = sh.sheet1
            ws.clear()
            cleaned_df = final_df.fillna('')
            ws.update([cleaned_df.columns.values.tolist()] + cleaned_df.values.tolist())
            print(f"Success! Link: {sh.url}")
            
        except Exception as e:
            print(f"Google Drive Error: {e}")
    else:
        print("Skipping Google Sheets upload (no credentials).")

if __name__ == "__main__":
    analyze()
