import streamlit as st
import pandas as pd
import numpy as np
import time

# Nastavení stránky
st.set_page_config(page_title="Logistics Master Data PRO", layout="wide", page_icon="🏭")

# --- CSS STYLING (Aby to vypadalo profesionálně) ---
st.markdown("""
    <style>
    .stProgress > div > div > div > div {
        background-color: #00CC00;
    }
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🏭 Generátor Master Dat: Detail Zakázky (PRO Verze)")
st.markdown("Generuje kompletní dataset pro Power BI s ukazatelem průběhu.")

# --- 1. FUNKCE: ČIŠTĚNÍ ID (KLÍČOVÁ OPRAVA) ---
def clean_id(series):
    """
    Vyčistí ID zakázky: Převede na text, smaže mezery, odstraní nuly na začátku.
    Příklad: '00123 ' -> '123'
    """
    return series.astype(str).str.strip().str.lstrip('0')

# --- 2. FUNKCE: VÝPOČET ČISTÉHO ČASU (PAUZY) ---
def calculate_clean_pick_duration(df_pick):
    # Seřadit: User -> Čas
    df_pick = df_pick.sort_values(by=['User', 'PickTimestamp'])
    df_pick['Prev_Time'] = df_pick.groupby('User')['PickTimestamp'].shift(1)
    df_pick['Diff_Min'] = (df_pick['PickTimestamp'] - df_pick['Prev_Time']).dt.total_seconds() / 60
    
    # Definice pauz
    pauzy = [
        ("08:15", "08:40"), ("11:00", "11:40"), ("12:45", "13:10"),
        ("16:15", "16:40"), ("18:30", "19:10"), ("20:30", "20:55")
    ]
    
    def ocistit(row):
        if pd.isna(row['Prev_Time']): return 0
        if row['PickTimestamp'].date() != row['Prev_Time'].date(): return 0
        val = row['Diff_Min']
        if val > 240: return 0 
        
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
    
    # Progress Bar Inicializace
    my_bar = st.progress(0)
    status_text = st.empty()
    
    # KROK 1: NAČÍTÁNÍ (0-20%)
    status_text.text("📂 Krok 1/5: Načítám soubory...")
    try:
        # Shipping
        if file_ship.name.endswith('.csv'):
            df_ship = pd.read_csv(file_ship, dtype=str) # Načítáme vše jako text pro jistotu
        else:
            df_ship = pd.read_excel(file_ship, dtype=str)
            
        # Picking
        if file_pick.name.endswith('.csv'):
            df_pick = pd.read_csv(file_pick) # Tady potřebujeme typy pro výpočty, ale ID opravíme
        else:
            df_pick = pd.read_excel(file_pick)
            
        # Packing
        if file_pack.name.endswith('.csv'):
            df_pack = pd.read_csv(file_pack)
        else:
            df_pack = pd.read_excel(file_pack)
            
    except Exception as e:
        st.error(f"Chyba při načítání souborů: {e}")
        return None

    my_bar.progress(20)
    time.sleep(0.5)

    # KROK 2: ČIŠTĚNÍ KLÍČŮ (20-40%)
    status_text.text("🧹 Krok 2/5: Čistím ID zakázek pro správné párování...")
    
    # Aplikujeme čištění ID na všechny tabulky
    if 'Delivery' in df_ship.columns:
        df_ship['Delivery_Key'] = clean_id(df_ship['Delivery'])
    else:
        st.error("Chybí sloupec 'Delivery' v Shipping souboru!")
        return None

    if 'Delivery' in df_pick.columns:
        df_pick['Delivery_Key'] = clean_id(df_pick['Delivery'])
    else:
        st.error("Chybí sloupec 'Delivery' v Picking souboru!")
        return None

    if 'Generated delivery' in df_pack.columns:
        df_pack['Delivery_Key'] = clean_id(df_pack['Generated delivery'])
    else:
        st.error("Chybí sloupec 'Generated delivery' v Packing souboru!")
        return None

    my_bar.progress(40)
    
    # KROK 3: ZPRACOVÁNÍ PICKINGU (40-60%)
    status_text.text("⏱️ Krok 3/5: Počítám časy pickování a pauzy...")
    
    # Timestamp
    if 'Confirmation date' in df_pick.columns and 'Confirmation time' in df_pick.columns:
        df_pick['PickTimestamp'] = pd.to_datetime(
            df_pick['Confirmation date'].astype(str) + ' ' + df_pick['Confirmation time'].astype(str),
            errors='coerce'
        )
    
    # Výpočty
    df_pick = calculate_clean_pick_duration(df_pick)
    
    # Agregace
    if 'Source target qty' in df_pick.columns:
        qty_col = 'Source target qty'
    else:
        qty_col = 'Dest.target quantity' 

    pick_agg = df_pick.groupby('Delivery_Key').agg({
        'PickTimestamp': ['min', 'max'],
        'Cista_Prodleva': 'sum',
        'User': 'nunique',
        'Material': ['nunique', 'count'],
        qty_col: 'sum',
        'Source Storage Bin': 'nunique'
    }).reset_index()
    
    pick_agg.columns = [
        'Delivery_Key', 'Pick_Start', 'Pick_End', 'Labor_Time_Min', 
        'Unique_Pickers', 'Unique_Materials', 'Total_Pick_Lines', 
        'Total_Pieces', 'Unique_Bins'
    ]
    pick_agg['Process_Pick_Duration_Min'] = (pick_agg['Pick_End'] - pick_agg['Pick_Start']).dt.total_seconds() / 60
    
    my_bar.progress(60)

    # KROK 4: ZPRACOVÁNÍ PACKINGU (60-80%)
    status_text.text("📦 Krok 4/5: Analyzuji obaly a expedici...")
    
    df_pack['Label_Created_Time'] = pd.to_datetime(
        df_pack['Created On'].astype(str) + ' ' + df_pack['Time'].astype(str), errors='coerce'
    )
    df_pack['Shipment_Added_Time'] = pd.to_datetime(
        df_pack['Changed On'].astype(str) + ' ' + df_pack['Time of change'].astype(str), errors='coerce'
    )
    
    def get_mode(x):
        m = pd.Series.mode(x)
        return m.values[0] if not m.empty else np.nan

    pack_agg = df_pack.groupby('Delivery_Key').agg({
        'Label_Created_Time': 'min',
        'Shipment_Added_Time': 'max',
        'Handling Unit': 'nunique',
        'Packaging materials': get_mode
    }).reset_index()
    
    pack_agg.rename(columns={'Packaging materials': 'Main_Packaging_Type'}, inplace=True)
    
    my_bar.progress(80)

    # KROK 5: FINÁLNÍ SPOJENÍ (80-100%)
    status_text.text("🔗 Krok 5/5: Spojuji vše do Master Reportu...")
    
    # Master data z Shipping
    date_cols = ['Creation date delivery', 'Loading Date', 'Pland Gds Mvmnt Date']
    for c in date_cols:
        if c in df_ship.columns:
            df_ship[c] = pd.to_datetime(df_ship[c], errors='coerce')

    # MERGE (Left Join na Delivery_Key)
    df_final = pd.merge(df_ship, pick_agg, on='Delivery_Key', how='left')
    df_final = pd.merge(df_final, pack_agg, on='Delivery_Key', how='left')

    # Doplnění KPI
    df_final['Duration_Reaction_Hrs'] = (df_final['Pick_Start'] - df_final['Creation date delivery']).dt.total_seconds() / 3600
    df_final['Duration_Wait_Pack_Hrs'] = (df_final['Label_Created_Time'] - df_final['Pick_End']).dt.total_seconds() / 3600
    df_final['End_Process_Time'] = df_final['Shipment_Added_Time'].fillna(df_final['Loading Date'])
    df_final['Duration_Pack_Ship_Hrs'] = (df_final['End_Process_Time'] - df_final['Label_Created_Time']).dt.total_seconds() / 3600

    def check_otp(row):
        if pd.isna(row['Loading Date']) or pd.isna(row['Pland Gds Mvmnt Date']): return "N/A"
        if row['Loading Date'].date() <= row['Pland Gds Mvmnt Date'].date(): return "Včas"
        return "ZPOŽDĚNÍ"
    
    df_final['OTP_Status'] = df_final.apply(check_otp, axis=1)

    def analyze_delay(row):
        if row['OTP_Status'] != "ZPOŽDĚNÍ": return "OK"
        times = {
            "Reakce Skladu": row['Duration_Reaction_Hrs'] if pd.notna(row['Duration_Reaction_Hrs']) else 0,
            "Pickování": (row['Process_Pick_Duration_Min']/60) if pd.notna(row['Process_Pick_Duration_Min']) else 0,
            "Čekání na Balení": row['Duration_Wait_Pack_Hrs'] if pd.notna(row['Duration_Wait_Pack_Hrs']) else 0,
            "Balení/Expedice": row['Duration_Pack_Ship_Hrs'] if pd.notna(row['Duration_Pack_Ship_Hrs']) else 0
        }
        return max(times, key=times.get)

    df_final['Main_Delay_Reason'] = df_final.apply(analyze_delay, axis=1)
    
    # Odstraníme pomocný klíč
    df_final.drop(columns=['Delivery_Key'], inplace=True)
    
    my_bar.progress(100)
    status_text.success("✅ HOTOVO! Data jsou připravena ke stažení.")
    
    return df_final

# --- UI LOGIKA ---
st.markdown("### 1. Nahraj vstupní soubory")
col1, col2, col3 = st.columns(3)
f_ship = col1.file_uploader("SHIPPING_ALL", type=['csv', 'xlsx'])
f_pick = col2.file_uploader("PICK_ALL", type=['csv', 'xlsx'])
f_pack = col3.file_uploader("PACKING_ALL", type=['csv', 'xlsx'])

if f_ship and f_pick and f_pack:
    if st.button("🚀 Spustit Analýzu Zakázek", use_container_width=True):
        
        df_result = process_files(f_ship, f_pick, f_pack)
        
        if df_result is not None:
            st.markdown("---")
            st.subheader("📊 Náhled Výsledku (Prvních 50 řádků)")
            
            # Zobrazíme klíčové sloupce
            cols_show = [col for col in ['Delivery', 'OTP_Status', 'Total_Pieces', 'Unique_Materials', 'Main_Packaging_Type', 'Main_Delay_Reason'] if col in df_result.columns]
            st.dataframe(df_result[cols_show].head(50))
            
            st.markdown("### 📥 STÁHNOUT KOMPLETNÍ DATA")
            st.info("Klikni na tlačítko níže pro stažení celého souboru pro Power BI.")
            
            # Export do Excelu
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False, sheet_name='MasterData')
            
            st.download_button(
                label="📥 STÁHNOUT MASTER EXCEL (.xlsx)",
                data=buffer,
                file_name="DETAIL_ZAKAZEK_POWERBI_FINAL.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key='download-excel'
            )
