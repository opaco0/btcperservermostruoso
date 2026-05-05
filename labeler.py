import pandas as pd
import numpy as np
import os
import glob

def process_triple_barrier(csv_path, output_path, tp_mult=1.0, sl_mult=0.8, time_window_min=10):
    print(f"Caricamento dataset: {csv_path}")
    if not os.path.exists(csv_path):
        print(f"Errore: Il file {csv_path} non esiste.")
        return

    df = pd.read_csv(csv_path)
    df = df.dropna().sort_values('timestamp').reset_index(drop=True)
    
    # Bug 3 FIX: Gestione robusta del nome colonna prezzo
    price_col = 'price' if 'price' in df.columns else 'current_price'
    prices = df[price_col].values
    atrs = df['atr'].values
    timestamps = df['timestamp'].values
    labels = np.full(len(df), 1)
    
    # Bug 1 FIX: Rilevamento automatico Secondi vs Millisecondi
    if timestamps[0] > 1e11:
        time_window = time_window_min * 60 * 1000  # Il CSV usa i Millisecondi
    else:
        time_window = time_window_min * 60         # Il CSV usa i Secondi (Unix)
        
    print(f"Calcolo etichette con Tripla Barriera (TP={tp_mult}x ATR, SL={sl_mult}x ATR, Timeout={time_window_min}m)...")
    
    for i in range(len(df)):
        start_price = prices[i]
        current_atr = atrs[i]
        start_ts = timestamps[i]
        
        if current_atr <= 0:
            continue
            
        barrier_up = start_price + (current_atr * tp_mult)
        barrier_down = start_price - (current_atr * sl_mult)
        barrier_time = start_ts + time_window  # FIX applicato
        
        for j in range(i + 1, len(df)):
            if timestamps[j] > barrier_time:
                break 
                
            future_price = prices[j]
            
            if future_price >= barrier_up:
                labels[i] = 2
                break
            elif future_price <= barrier_down:
                labels[i] = 0
                break

    df['label'] = labels
    counts = df['label'].value_counts()
    tot = len(df)
    
    print("\n--- RISULTATI LABELING ---")
    print(f"Totale righe elaborate: {tot}")
    print(f"SHORT  (0): {counts.get(0, 0):>6} ({counts.get(0, 0)/tot*100:.1f}%)")
    print(f"NEUTRO (1): {counts.get(1, 0):>6} ({counts.get(1, 0)/tot*100:.1f}%)")
    print(f"LONG   (2): {counts.get(2, 0):>6} ({counts.get(2, 0)/tot*100:.1f}%)")
    
    df.to_csv(output_path, index=False)
    print(f"\nDataset etichettato e salvato in: {output_path}")

if __name__ == "__main__":
    list_of_files = glob.glob('ml_data_*.csv')
    if not list_of_files:
        print("Nessun dataset trovato.")
    else:
        latest_file = max(list_of_files, key=os.path.getctime)
        process_triple_barrier(latest_file, "dataset_ready_for_training.csv", tp_mult=1.0, sl_mult=0.8, time_window_min=10)