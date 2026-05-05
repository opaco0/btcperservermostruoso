import pandas as pd
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report, f1_score, confusion_matrix
from sklearn.utils.class_weight import compute_sample_weight
import numpy as np
import json
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, auc
import os

DATA_FILE = "dataset_ready_for_training.csv" 
MODEL_FILE = "trading_model.json"

def main():
    print(f"Caricamento dataset: {DATA_FILE}...")
    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print(f"Errore: {DATA_FILE} non trovato.")
        return

    target_col = 'label' if 'label' in df.columns else 'target'
    df = df.dropna(subset=[target_col])
    
    if 'timestamp' in df.columns:
        df = df.sort_values(by='timestamp').reset_index(drop=True)

    exclude_cols = [target_col, 'ts', 'timestamp', 'price', 'current_price'] 
    features = [c for c in df.columns if c not in exclude_cols]
    
    X = df[features]
    y = df[target_col].astype(int)

    if -1 in y.values:
        y = y.map({-1: 0, 0: 1, 1: 2})
    
    tscv = TimeSeriesSplit(n_splits=5)
    
    params = {
        'objective': 'multi:softprob',
        'num_class': 3,
        'eval_metric': 'mlogloss',
        'max_depth': 4,
        'learning_rate': 0.05,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'min_child_weight': 5,
        'reg_lambda': 1.5,
        'reg_alpha': 0.5,
        'seed': 42
    }

    best_model = None
    best_f1 = -1.0
    best_fold = 0
    
    # Bug 2 FIX: Aggiunta variabile per le probabilità allineate
    best_y_test, best_preds, best_preds_prob = None, None, None

    for fold, (train_index, test_index) in enumerate(tscv.split(X), 1):
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        
        sample_weights = compute_sample_weight(class_weight='balanced', y=y_train)
        dtrain = xgb.DMatrix(X_train, label=y_train, weight=sample_weights)
        dtest = xgb.DMatrix(X_test, label=y_test)
        
        model = xgb.train(
            params, dtrain, num_boost_round=300,
            evals=[(dtrain, 'train'), (dtest, 'test')],
            early_stopping_rounds=20, verbose_eval=False 
        )
        
        preds_prob = model.predict(dtest)
        preds = np.argmax(preds_prob, axis=1)
            
        f1_operativo = f1_score(y_test, preds, labels=[0, 2], average='macro', zero_division=0)
        print(f"Fold {fold}: F1 Operativo = {f1_operativo:.4f}")
        
        if f1_operativo > best_f1:
            best_f1 = f1_operativo
            best_model = model
            best_fold = fold
            best_y_test = y_test
            best_preds = preds
            best_preds_prob = preds_prob # <--- FIX APPLICATO QUI
            
    print(f"\n🏆 SCELTA DEL MODELLO FINALE: Fold {best_fold}")
    
    print("\n--- CALIBRAZIONE SOGLIA DI CONFIDENZA ---")
    best_thresh = 0.60
    best_thresh_score = 0.0
    
    for thresh in np.arange(0.50, 0.90, 0.05):
        custom_preds = []
        # Bug 2 FIX: Iteriamo sulle probabilità del FOLD MIGLIORE, non dell'ultimo
        for row in best_preds_prob: 
            p_short, p_neutro, p_long = row[0], row[1], row[2]
            if p_long > thresh and p_long > p_short:
                custom_preds.append(2)
            elif p_short > thresh and p_short > p_long:
                custom_preds.append(0)
            else:
                custom_preds.append(1)
                
        f1_op = f1_score(best_y_test, custom_preds, labels=[0, 2], average='macro', zero_division=0)
        if f1_op > best_thresh_score:
            best_thresh_score = f1_op
            best_thresh = round(thresh, 2)

    print(f"🎯 Soglia Ottimale calcolata: {best_thresh * 100}% (F1: {best_thresh_score:.4f})")
    
    print("\n--- ANALISI PRECISION-RECALL SULLE CLASSI OPERATIVE ---")
    if best_preds_prob is not None:
        try:
            plt.figure(figsize=(10, 6))
            
            # --- 1. Curva PR per la classe SHORT (Classe 0) ---
            y_test_short = (best_y_test == 0).astype(int)
            prob_short = best_preds_prob[:, 0]
            precision_s, recall_s, thresholds_s = precision_recall_curve(y_test_short, prob_short)
            auc_short = auc(recall_s, precision_s)
            plt.plot(recall_s, precision_s, label=f'SHORT (Classe 0) - AUC: {auc_short:.3f}', color='red', alpha=0.7)
            
            # -> NOVITÀ: Troviamo e disegniamo la Soglia Ottimale per lo SHORT
            # (thresholds_s ha un elemento in meno di precision_s, quindi ignoriamo l'ultimo punto)
            if len(thresholds_s) > 0:
                idx_thresh_s = np.argmin(np.abs(thresholds_s - best_thresh))
                plt.plot(recall_s[idx_thresh_s], precision_s[idx_thresh_s], marker='*', markersize=15, 
                         color='darkred', markeredgecolor='black', label=f'Tua Soglia SHORT ({best_thresh*100}%)')

            # --- 2. Curva PR per la classe LONG (Classe 2) ---
            y_test_long = (best_y_test == 2).astype(int)
            prob_long = best_preds_prob[:, 2]
            precision_l, recall_l, thresholds_l = precision_recall_curve(y_test_long, prob_long)
            auc_long = auc(recall_l, precision_l)
            plt.plot(recall_l, precision_l, label=f'LONG (Classe 2) - AUC: {auc_long:.3f}', color='green', alpha=0.7)
            
            # -> NOVITÀ: Troviamo e disegniamo la Soglia Ottimale per il LONG
            if len(thresholds_l) > 0:
                idx_thresh_l = np.argmin(np.abs(thresholds_l - best_thresh))
                plt.plot(recall_l[idx_thresh_l], precision_l[idx_thresh_l], marker='*', markersize=15, 
                         color='lime', markeredgecolor='black', label=f'Tua Soglia LONG ({best_thresh*100}%)')
            
            # --- Personalizzazione del grafico ---
            plt.title('Curva Precision-Recall con Soglia Operativa')
            plt.xlabel('Recall (Opportunità Catturate)')
            plt.ylabel('Precision (Win Rate Stimato)')
            plt.legend(loc='lower left')
            plt.grid(True, linestyle='--', alpha=0.7)
            
            plot_filename = "precision_recall_curve.png"
            plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
            print(f"📊 Grafico Precision-Recall salvato come '{plot_filename}'")
            
            
        except Exception as e:
             print(f"Errore durante la generazione della curva PR: {e}")

    with open("ai_config.json", "w") as f:
        json.dump({"confidence_threshold": float(best_thresh)}, f)


    print("\n--- SIMULAZIONE FINANZIARIA (PROFITTI E PERDITE) ---")
    if best_preds_prob is not None:
        # 1. Imposta qui il valore medio in $ dei tuoi trade
        # Basato sui moltiplicatori del labeler (TP 1.0, SL 0.8) e un ATR stimato
        AVG_TP = 100.0  # Quanto guadagni in media per trade vincente
        AVG_SL = 80.0   # Quanto perdi in media per trade perdente

        # 2. Ricreiamo le previsioni finali usando la TUA SOGLIA OTTIMALE
        final_preds = []
        for row in best_preds_prob:
            p_short, p_neutro, p_long = row[0], row[1], row[2]
            if p_long > best_thresh and p_long > p_short:
                final_preds.append(2)
            elif p_short > best_thresh and p_short > p_long:
                final_preds.append(0)
            else:
                final_preds.append(1)
        
        final_preds = np.array(final_preds)
        y_test_vals = best_y_test.values if hasattr(best_y_test, 'values') else best_y_test

        # 3. Contiamo le Vittorie (True Positives) e le Sconfitte (False Positives)
        # LONG
        long_wins = np.sum((final_preds == 2) & (y_test_vals == 2))
        long_losses = np.sum((final_preds == 2) & (y_test_vals != 2))
        
        # SHORT
        short_wins = np.sum((final_preds == 0) & (y_test_vals == 0))
        short_losses = np.sum((final_preds == 0) & (y_test_vals != 0))

        # 4. Calcoli Finanziari
        tot_wins = long_wins + short_wins
        tot_losses = long_losses + short_losses
        tot_trades = tot_wins + tot_losses

        gross_profit = tot_wins * AVG_TP
        gross_loss = tot_losses * AVG_SL
        net_profit = gross_profit - gross_loss

        win_rate = (tot_wins / tot_trades * 100) if tot_trades > 0 else 0

        # 5. Stampa del Report P&L
        print(f"Soglia Applicata: {best_thresh*100}%")
        print(f"Totale Trade Eseguiti: {tot_trades}")
        print(f"Win Rate Reale: {win_rate:.1f}% ({tot_wins} Vinte, {tot_losses} Perse)")
        print(f"------------------------------------")
        print(f"Profitti Lordi (Gross Profit): + {gross_profit:.2f} $")
        print(f"Perdite Lorde  (Gross Loss):   - {gross_loss:.2f} $")
        print(f"------------------------------------")
        
        if net_profit > 0:
            print(f"P&L NETTO (Profitto Stimato):  + {net_profit:.2f} $ 🟢")
        else:
            print(f"P&L NETTO (Perdita Stimata):   - {abs(net_profit):.2f} $ 🔴")
        print(f"------------------------------------\n")

    if best_model:
        best_model.save_model(MODEL_FILE)

if __name__ == "__main__":
    main()