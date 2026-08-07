"""
NHL Win Prediction - Logistic Regression Pipeline
===================================================
Predicts Win (0/1) from team shot-quality metrics.

Workflow:
1. Data cleaning (handles undefined HDGF_PCT '0/0' games)
2. Correlation screening
3. Full 5-variable logistic regression + multicollinearity (VIF) diagnostics
4. Backward tuning -> final model
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report

RAW_PATH = "final_log_regression_game_data.csv"  # place in same folder
CANDIDATE_PREDICTORS = ["SF_PCT", "xGF_PCT", "CF_PCT", "FF_PCT", "HDGF_PCT"]
FINAL_PREDICTORS = ["xGF_PCT", "HDGF_PCT"]  # result of tuning, see report

def load_and_clean(path=RAW_PATH):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df["HDGF_PCT"] = pd.to_numeric(df["HDGF_PCT"], errors="coerce")  # '-' -> NaN (0/0 games)
    df = df.dropna(subset=CANDIDATE_PREDICTORS + ["Win"]).copy()
    return df

def fit_model(df, predictors=FINAL_PREDICTORS, test_size=0.2, seed=42):
    X, y = df[predictors], df["Win"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y
    )
    scaler = StandardScaler().fit(X_train)
    X_train_s, X_test_s = scaler.transform(X_train), scaler.transform(X_test)

    model = LogisticRegression().fit(X_train_s, y_train)
    y_pred = model.predict(X_test_s)
    y_proba = model.predict_proba(X_test_s)[:, 1]

    print(f"Predictors: {predictors}")
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(f"ROC-AUC:  {roc_auc_score(y_test, y_proba):.4f}")
    print(classification_report(y_test, y_pred))
    return model, scaler

def predict_win_probability(model, scaler, xgf_pct, hdgf_pct):
    """Given a team's xGF% and HDGF% for a game, return P(Win)."""
    X = scaler.transform([[xgf_pct, hdgf_pct]])
    return model.predict_proba(X)[0, 1]

if __name__ == "__main__":
    df = load_and_clean()
    model, scaler = fit_model(df)
    # example
    p = predict_win_probability(model, scaler, xgf_pct=58, hdgf_pct=65)
    print(f"\nExample: team with xGF%=58, HDGF%=65 -> P(Win) = {p:.3f}")
