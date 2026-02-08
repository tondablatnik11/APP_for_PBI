import streamlit as st
import pandas as pd
import numpy as np
import io

# --- NASTAVENÍ APLIKACE ---
st.set_page_config(page_title="Logistics Master Data Integrator", layout="wide", page_icon="🏭")

# CSS pro profesionální vzhled
st.markdown("""
    <style>
    .stProgress > div > div > div > div { background-color: #00CC00; }
    .big-font { font-size:20px !important; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

st.title("🏭 Logistics Master Data Integrator")
st.markdown("**Vstup:** 3 soubory (Shipping, Picking, Packing) | **Výstup:** Kompletní dataset pro Power BI")

# --- POMOCNÉ FUNKCE ---

def clean_id(series):
    """Vyčistí ID zakázky (odstraní nuly na začátku, mezery)."""
    return series.astype(str).str.strip().str.lstrip('0')

def find_column(df, candidates):
    """Najde existující sloupec z listu kandidátů."""
    for col in candidates:
        if col in df.columns: return col
        for df_col in df.columns:
            if df_col.lower() == col.lower(): return df_col
    return None

def calculate_clean_pick_duration(df_pick):
    """
    Pokročilý výpočet čistého času pickování s odečtením pauz.
    """
    if 'PickTimestamp' not in df_pick.columns or 'User' not in df_pick.columns:
        return df_pick

    # Seřadit: User -> Čas
    df_pick = df_pick.sort_values(by=['User', 'PickTimestamp'])
    df_pick['Prev_Time'] = df_pick.groupby('User')['PickTimestamp'].shift(1)
    
    # Rozdíl v minutách
    df_pick['Diff_Min'] = (df_pick['PickTimestamp'] - df_pick['Prev_Time']).dt.total_seconds() / 60
    
    # Definice pauz
    pauzy = [
        ("08:15", "08:40"), ("11:00", "11:40"), ("12:45", "13:10"),
        ("16:15", "16:40"), ("18:30", "19:10"), ("20:30", "20:55")
    ]
    
    def ocistit(row):
        if pd.isna(row['Prev_Time']): return 0
        # Pokud je to jiný den, nepočítat
        if row['PickTimestamp'].date() != row['Prev_Time'].date(): return 0
        
        val = row['Diff_Min']
        # Pokud je mezera větší než 4 hodiny, je to chyba nebo nová směna
        if val > 240: return 0 
        
        # Kontrola pauz (zjednodušená - pokud čas spadá do pauzy)
        t = row['PickTimestamp'].time()
        for start, end in pauzy:
            s = pd.to_datetime(start).time()
            e = pd.to_datetime(end).time()
            if s <= t <= e:
                return 0
        return val

    df_pick['Cista_Prodleva'] = df_pick.apply(ocistit, axis=1)
    return df_pick

# --- HLAVNÍ LOGIKA ZPRACOVÁNÍ ---

def process_three_files(file_ship, file_pick, file_pack):
    my_bar = st.progress(0)
    status_text = st.empty()

    try:
        # --- 1. NAČTENÍ SOUBORŮ ---
        status_text.text("📂 Krok 1/4: Načítám data...")
        
        # Helper pro načtení
        def load_file(f):
            if f.name.endswith('.csv'): return pd.read_csv(f, dtype=str)
            return pd.read_excel(f, dtype=str)

        df_ship = load_file(file_ship)
        df_pick = load_file(file_pick)
        df_pack = load_file(file_pack)
        
        my_bar.progress(25)

        # --- 2. ČIŠTĚNÍ ID (PRO SPOJENÍ) ---
        status_text.text("🧹 Krok 2/4: Čistím ID zakázek a páruji...")
        
        col_ship_id = find_column(df_ship, ['Delivery', 'Zakázka', 'Shipment'])
        col_pick_id = find_column(df_pick, ['Delivery', 'Zakázka'])
        col_pack_id = find_column(df_pack, ['Generated delivery', 'Delivery'])

        if not all([col_ship_id, col_pick_id, col_pack_id]):
            st.error(f"Chybí klíčové sloupce ID! (Našel: Ship={col_ship_id}, Pick={col_pick_id}, Pack={col_pack_id})")
            return None

        df_ship['KEY'] = clean_id(df_ship[col_ship_id])
        df_pick['KEY'] = clean_id(df_pick[col_pick_id])
        df_pack['KEY'] = clean_id(df_pack[col_pack_id])

        my_bar.progress(50)

        # --- 3. AGREGACE A VÝPOČTY (PICKING & PACKING) ---
        status_text.text("⚙️ Krok 3/4: Počítám metriky (Kusy, Materiály, Časy)...")

        # >>> ZPRACOVÁNÍ PICKINGU <<<
        # Převod sloupců na čísla/data
        col_pick_qty = find_column(df_pick, ['Source target qty', 'Množství', 'Qty', 'Pieces'])
        col_pick_mat = find_column(df_pick, ['Material', 'Materiál'])
        
        # Vytvoření Timestampu pro pickování
        if 'Confirmation date' in df_pick.columns and 'Confirmation time' in df_pick.columns:
            df_pick['PickTimestamp'] = pd.to_datetime(
                df_pick['Confirmation date'].astype(str) + ' ' + df_pick['Confirmation time'].astype(str),
                errors='coerce'
            )
            # Aplikace logiky čistého času
            if 'Source target qty' in df_pick.columns: # jen pokud máme sloupec qty
                df_pick[col_pick_qty] = pd.to_numeric(df_pick[col_pick_qty], errors='coerce').fillna(0)
            
            # Pokud máme User sloupec, spočítáme čistý čas
            if 'User' in df_pick.columns:
                df_pick = calculate_clean_pick_duration(df_pick)

        # Agregace Pickingu
        agg_rules_pick = {
            'PickTimestamp': ['min', 'max'], # Start a Konec pickování
            col_pick_mat: 'nunique',         # Počet unikátních materiálů
            col_pick_qty: 'sum',             # Celkem kusů
        }
        if 'Cista_Prodleva' in df_pick.columns:
            agg_rules_pick['Cista_Prodleva'] = 'sum' # Čistý čas práce (suma minut)

        df_pick_agg = df_pick.groupby('KEY').agg(agg_rules_pick).reset_index()
        
        # Přejmenování sloupců (flatten multi-index)
        df_pick_agg.columns = ['KEY', 'Pick_Start', 'Pick_End', 'Unique_Materials', 'Total_Pieces'] + \
                              (['Labor_Time_Min'] if 'Cista_Prodleva' in df_pick.columns else [])

        # >>> ZPRACOVÁNÍ PACKINGU <<<
        col_pack_mat = find_column(df_pack, ['Packaging materials', 'Packaging', 'Balení'])
        
        # Získání hlavního typu balení (nejčastější hodnota)
        def get_mode(x):
            return x.mode()[0] if not x.mode().empty else ""

        df_pack_agg = df_pack.groupby('KEY').agg({
            col_pack_mat: get_mode
        }).reset_index()
        df_pack_agg.rename(columns={col_pack_mat: 'Main_Packaging_Type'}, inplace=True)

        my_bar.progress(75)

        # --- 4. FINÁLNÍ SPOJENÍ A KPI ---
        status_text.text("🔗 Krok 4/4: Kompletuji Master Data...")

        # Left Join: Shipping (Hlavní) <- Picking <- Packing
        df_final = pd.merge(df_ship, df_pick_agg, on='KEY', how='left')
        df_final = pd.merge(df_final, df_pack_agg, on='KEY', how='left')

        # Doplnění nul tam, kde nebylo pickování (např. 0 kusů)
        df_final['Total_Pieces'] = df_final['Total_Pieces'].fillna(0)
        df_final['Unique_Materials'] = df_final['Unique_Materials'].fillna(0)

        # Konverze dat z Shipping
        col_loading = find_column(df_final, ['Loading Date', 'Datum nakládky'])
        col_planned = find_column(df_final, ['Pland Gds Mvmnt Date', 'Plánovaný GI', 'Planned GI'])
        
        if col_loading: df_final[col_loading] = pd.to_datetime(df_final[col_loading], errors='coerce')
        if col_planned: df_final[col_planned] = pd.to_datetime(df_final[col_planned], errors='coerce')

        # Výpočet OTP Statusu
        def get_otp(row):
            if pd.isna(row.get(col_loading)) or pd.isna(row.get(col_planned)): return "N/A"
            return "Včas" if row[col_loading] <= row[col_planned] else "Zpoždění"

        if col_loading and col_planned:
            df_final['OTP_Status'] = df_final.apply(get_otp, axis=1)
        else:
            df_final['OTP_Status'] = "N/A (Chybí data)"

        # Výpočet Delay Reason (zjednodušený)
        def get_reason(row):
            if row['OTP_Status'] != "Zpoždění": return "OK"
            # Zde je prostor pro logiku: pokud Pick_End > Planned -> "Pozdní Pick", atd.
            if pd.notna(row.get('Pick_End')) and pd.notna(row.get(col_planned)):
                if row['Pick_End'] > row[col_planned]: return "Zpoždění ve skladu (Pick)"
            return "Jiné zpoždění"
            
        df_final['Main_Delay_Reason'] = df_final.apply(get_reason, axis=1)

        # Úklid
        df_final.drop(columns=['KEY'], inplace=True)
        
        my_bar.progress(100)
        status_text.success("✅ Hotovo!")
        
        return df_final

    except Exception as e:
        st.error(f"Kritická chyba při zpracování: {e}")
        return None

# --- UI STRÁNKA ---

st.markdown("### 1. Nahraj vstupní soubory")
col1, col2, col3 = st.columns(3)
f_ship = col1.file_uploader("📂 SHIPPING (Zakázky)", type=['csv', 'xlsx'])
f_pick = col2.file_uploader("📂 PICKING (Položky/Scany)", type=['csv', 'xlsx'])
f_pack = col3.file_uploader("📂 PACKING (Balení)", type=['csv', 'xlsx'])

if f_ship and f_pick and f_pack:
    if st.button("🚀 Spustit Integraci a Analýzu", type="primary"):
        
        df_result = process_three_files(f_ship, f_pick, f_pack)
        
        if df_result is not None:
            st.markdown("---")
            st.subheader("📊 Náhled výsledku (Prvních 50 řádků)")
            
            # Výběr důležitých sloupců pro náhled (pokud existují)
            priority_cols = ['Delivery', 'OTP_Status', 'Total_Pieces', 'Unique_Materials', 'Main_Packaging_Type', 'Main_Delay_Reason']
            available_cols = [c for c in priority_cols if c in df_result.columns]
            
            st.dataframe(df_result[available_cols].head(50), use_container_width=True)
            
            # --- EXPORT PRO POWER BI ---
            st.markdown("### 📤 Export pro Power BI")
            st.write("Tento soubor obsahuje spojená data se všemi detaily.")
            
            # CSV Export (nejlepší pro PBI)
            csv_data = df_result.to_csv(index=False, sep=',', encoding='utf-8')
            
            col_d1, col_d2 = st.columns(2)
            
            col_d1.download_button(
                label="Stáhnout CSV pro Power BI",
                data=csv_data,
                file_name="MasterData_PowerBI.csv",
                mime="text/csv"
            )
            
            # Excel Export (alternativa)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_result.to_excel(writer, index=False, sheet_name='Data')
            
            col_d2.download_button(
                label="Stáhnout Excel (.xlsx)",
                data=buffer,
                file_name="MasterData_PowerBI.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
