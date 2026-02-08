import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Logistics Master Data Gen", layout="wide")
st.title("📦 Generátor Master Dat: Detail Zakázky")
st.markdown("Generuje data pro Power BI se zaměřením na průběh zakázky a detaily materiálů.")

# --- 1. FUNKCE: OČIŠTĚNÍ ČASU (PAUZY) ---
def calculate_clean_pick_duration(df_pick):
    # Seřadit: User -> Čas
    df_pick = df_pick.sort_values(by=['User', 'PickTimestamp'])
    df_pick['Prev_Time'] = df_pick.groupby('User')['PickTimestamp'].shift(1)
    df_pick['Diff_Min'] = (df_pick['PickTimestamp'] - df_pick['Prev_Time']).dt.total_seconds() / 60
    
    # Definice pauz dle zadání
    pauzy = [
        ("08:15", "08:40"), ("11:00", "11:40"), ("12:45", "13:10"),
        ("16:15", "16:40"), ("18:30", "19:10"), ("20:30", "20:55")
    ]
    
    def ocistit(row):
        # Logika pro nulování pauz a nočních přechodů
        if pd.isna(row['Prev_Time']): return 0
        if row['PickTimestamp'].date() != row['Prev_Time'].date(): return 0
        val = row['Diff_Min']
        if val > 240: return 0 # Pojistka proti extrémům
        
        t = row['PickTimestamp'].time()
        for start, end in pauzy:
            s = pd.to_datetime(start).time()
            e = pd.to_datetime(end).time()
            if s <= t <= e:
                return 0
        return val

    df_pick['Cista_Prodleva'] = df_pick.apply(ocistit, axis=1)
    return df_pick

# --- HLAVNÍ LOGIKA ---
def process_files(file_ship, file_pick, file_pack):
    
    # --- A. NAČTENÍ DAT ---
    try:
        df_ship = pd.read_csv(file_ship, dtype={'Delivery': str})
        df_pick = pd.read_csv(file_pick, dtype={'Delivery': str})
        df_pack = pd.read_csv(file_pack, dtype={'Generated delivery': str})
    except:
        df_ship = pd.read_excel(file_ship, dtype={'Delivery': str})
        df_pick = pd.read_excel(file_pick, dtype={'Delivery': str})
        df_pack = pd.read_excel(file_pack, dtype={'Generated delivery': str})

    # --- B. PICKING DATA (Detailní agregace) ---
    # Timestamp
    if 'Confirmation date' in df_pick.columns and 'Confirmation time' in df_pick.columns:
        df_pick['PickTimestamp'] = pd.to_datetime(
            df_pick['Confirmation date'].astype(str) + ' ' + df_pick['Confirmation time'].astype(str),
            errors='coerce'
        )
    
    # Čistý čas práce
    df_pick = calculate_clean_pick_duration(df_pick)
    
    # Množství (Target Qty)
    if 'Source target qty' in df_pick.columns:
        qty_col = 'Source target qty'
    else:
        qty_col = 'Dest.target quantity' # Fallback
        
    # AGREGACE ZA ZAKÁZKU
    pick_agg = df_pick.groupby('Delivery').agg({
        'PickTimestamp': ['min', 'max'], # Start a Konec
        'Cista_Prodleva': 'sum',         # Člověkominuty celkem
        'User': 'nunique',               # Kolik lidí na tom dělalo (Handover Count)
        'Material': ['nunique', 'count'],# Počet druhů materiálu, Počet řádků
        qty_col: 'sum',                  # Celkový počet kusů
        'Source Storage Bin': 'nunique'  # Z kolika pozic se bralo
    }).reset_index()
    
    # Zploštění sloupců
    pick_agg.columns = [
        'Delivery', 'Pick_Start', 'Pick_End', 'Labor_Time_Min', 
        'Unique_Pickers', 'Unique_Materials', 'Total_Pick_Lines', 
        'Total_Pieces', 'Unique_Bins'
    ]
    
    # Doba trvání procesu pickování (Wall Clock Time)
    pick_agg['Process_Pick_Duration_Min'] = (pick_agg['Pick_End'] - pick_agg['Pick_Start']).dt.total_seconds() / 60

    # --- C. PACKING DATA ---
    # Timestamps
    df_pack['Label_Created_Time'] = pd.to_datetime(
        df_pack['Created On'].astype(str) + ' ' + df_pack['Time'].astype(str), errors='coerce'
    )
    df_pack['Shipment_Added_Time'] = pd.to_datetime(
        df_pack['Changed On'].astype(str) + ' ' + df_pack['Time of change'].astype(str), errors='coerce'
    )
    
    # Získání hlavního obalového materiálu (nejčastější)
    def get_mode(x):
        m = pd.Series.mode(x)
        return m.values[0] if not m.empty else np.nan

    pack_agg = df_pack.groupby('Generated delivery').agg({
        'Label_Created_Time': 'min',    # Start Balení
        'Shipment_Added_Time': 'max',   # Konec Balení / Shipment
        'Handling Unit': 'nunique',     # Počet krabic
        'Packaging materials': get_mode # Typ obalu (např. CARTON-02)
    }).reset_index()
    
    pack_agg.rename(columns={'Generated delivery': 'Delivery', 'Packaging materials': 'Main_Packaging_Type'}, inplace=True)

    # --- D. SHIPPING DATA (Master) ---
    date_cols = ['Creation date delivery', 'Loading Date', 'Pland Gds Mvmnt Date']
    for c in date_cols:
        if c in df_ship.columns:
            df_ship[c] = pd.to_datetime(df_ship[c], errors='coerce')

    # --- E. SLOUČENÍ ---
    df_final = pd.merge(df_ship, pick_agg, on='Delivery', how='left')
    df_final = pd.merge(df_final, pack_agg, on='Delivery', how='left')

    # --- F. VÝPOČTY KPI & STATUSŮ ---
    
    # 1. Lead Times (Průběžné doby)
    # T1: Reakce (Vznik -> Začátek Picku)
    df_final['Duration_Reaction_Hrs'] = (df_final['Pick_Start'] - df_final['Creation date delivery']).dt.total_seconds() / 3600
    
    # T2: Pickování (Už máme Process_Pick_Duration_Min)
    
    # T3: Čekání na balení (Konec Picku -> Label)
    df_final['Duration_Wait_Pack_Hrs'] = (df_final['Label_Created_Time'] - df_final['Pick_End']).dt.total_seconds() / 3600
    
    # T4: Balení & Expedice (Label -> Shipment/Loading)
    # Použijeme Loading Date pokud chybí Shipment Time
    df_final['End_Process_Time'] = df_final['Shipment_Added_Time'].fillna(df_final['Loading Date'])
    df_final['Duration_Pack_Ship_Hrs'] = (df_final['End_Process_Time'] - df_final['Label_Created_Time']).dt.total_seconds() / 3600

    # 2. Včasnost (OTP)
    def check_otp(row):
        if pd.isna(row['Loading Date']) or pd.isna(row['Pland Gds Mvmnt Date']): return "N/A"
        if row['Loading Date'].date() <= row['Pland Gds Mvmnt Date'].date(): return "Včas"
        return "ZPOŽDĚNÍ"
    
    df_final['OTP_Status'] = df_final.apply(check_otp, axis=1)

    # 3. Kategorizace Zpoždění (Kde to stálo?)
    def analyze_delay(row):
        if row['OTP_Status'] != "ZPOŽDĚNÍ": return "OK"
        # Kde byl největší prostoj?
        times = {
            "Reakce Skladu": row['Duration_Reaction_Hrs'] if pd.notna(row['Duration_Reaction_Hrs']) else 0,
            "Pickování": (row['Process_Pick_Duration_Min']/60) if pd.notna(row['Process_Pick_Duration_Min']) else 0,
            "Čekání na Balení": row['Duration_Wait_Pack_Hrs'] if pd.notna(row['Duration_Wait_Pack_Hrs']) else 0,
            "Balení/Expedice": row['Duration_Pack_Ship_Hrs'] if pd.notna(row['Duration_Pack_Ship_Hrs']) else 0
        }
        return max(times, key=times.get) # Vrátí název fáze s největším číslem

    df_final['Main_Delay_Reason'] = df_final.apply(analyze_delay, axis=1)

    # 4. Celková doba (Creation -> Loading)
    df_final['Total_Lead_Time_Hrs'] = (df_final['Loading Date'] - df_final['Creation date delivery']).dt.total_seconds() / 3600

    return df_final

# --- UI ---
st.markdown("### 1. Nahraj soubory")
col1, col2, col3 = st.columns(3)
f_ship = col1.file_uploader("SHIPPING_ALL", type=['csv', 'xlsx'])
f_pick = col2.file_uploader("PICK_ALL", type=['csv', 'xlsx'])
f_pack = col3.file_uploader("PACKING_ALL", type=['csv', 'xlsx'])

if f_ship and f_pick and f_pack:
    if st.button("🚀 Spustit Analýzu Zakázek"):
        with st.spinner("Spojuji světy Shipping, Picking a Packing..."):
            df_result = process_files(f_ship, f_pick, f_pack)
            
            st.success(f"Zpracováno {len(df_result)} zakázek!")
            
            # Ukázka
            cols_show = ['Delivery', 'order type', 'OTP_Status', 'Total_Pieces', 'Unique_Materials', 'Main_Packaging_Type', 'Main_Delay_Reason']
            st.dataframe(df_result[cols_show].head(10))
            
            # Export
            out_file = "DETAIL_ZAKAZEK_POWERBI.xlsx"
            df_result.to_excel(out_file, index=False)
            with open(out_file, "rb") as f:
                st.download_button("📥 Stáhnout Data pro Power BI", f, file_name=out_file)
