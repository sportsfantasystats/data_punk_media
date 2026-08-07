# NHL Win Prediction: Logistic Regression Analysis

**Dataset:** 9,700 team-game records (2018-19 season), 8,728 used after cleaning
**Target:** `Win` (0/1) — balanced 50/50
**Candidate predictors:** SF_PCT, xGF_PCT, CF_PCT, FF_PCT, HDGF_PCT

---

## 1. Data Cleaning

`HDGF_PCT` contained 972 rows marked `'-'` (games with zero high-danger chances for
either team — mathematically undefined %). These rows were dropped rather than
imputed, since a 0/0 game carries no real signal about shooting performance.

## 2. Correlation Screening (Step 1)

Point-biserial correlation of each raw predictor against `Win`:

| Predictor | r (correlation w/ Win) | p-value | Significant? |
|---|---|---|---|
| HDGF_PCT | **0.483** | <0.001 | Yes |
| xGF_PCT | 0.198 | <0.001 | Yes |
| SF_PCT | 0.120 | <0.001 | Yes |
| FF_PCT | 0.075 | <0.001 | Yes |
| CF_PCT | 0.001 | 0.89 | **No** |

**Key finding:** `CF_PCT` (raw shot-attempt share) has essentially zero
relationship with winning on its own — a first red flag. `HDGF_PCT` (share of
high-danger goals) is by far the strongest single predictor, which makes hockey
sense: scoring the majority of high-danger goals is close to a proxy for
scoring more goals overall.

A predictor inter-correlation check also showed **severe multicollinearity**:
SF_PCT, CF_PCT, and FF_PCT are all shot-attempt-based metrics correlated with
each other at r = 0.83–0.93.

## 3. Full Model (Step 2): All 5 Predictors

A logistic regression with all 5 predictors (standardized) achieved:
- **Accuracy: 72.2%**
- **ROC-AUC: 0.780**

But the coefficient for `CF_PCT` flipped to **negative** (-0.82) despite having
zero raw correlation with winning — a classic multicollinearity artifact:
because CF_PCT overlaps so heavily with SF_PCT and FF_PCT, the model
"steals" explanatory credit in a statistically unstable way. Variance Inflation
Factors confirmed this: FF_PCT = 14.4, CF_PCT = 7.3, SF_PCT = 6.0 (all above
the common rule-of-thumb threshold of 5).

## 4. Tuning (Step 3): Backward Elimination

| Model | Predictors | Accuracy | ROC-AUC | Max VIF |
|---|---|---|---|---|
| Full | SF, xGF, CF, FF, HDGF | 0.722 | 0.780 | 14.4 |
| Drop CF_PCT | SF, xGF, FF, HDGF | 0.733 | 0.779 | 7.3 |
| Drop CF & FF | SF, xGF, HDGF | 0.725 | 0.775 | 2.6 |
| **Final** | **xGF, HDGF** | **0.725** | **0.775** | **1.1** |
| HDGF only | HDGF | 0.725 | 0.768 | — |

Removing the redundant shot-attempt variables costs virtually **no predictive
performance** (accuracy/AUC stay within ~1 point of the full model) while
eliminating the instability. In every reduced model that still included
`SF_PCT` alongside `xGF_PCT`, `SF_PCT`'s coefficient became statistically
insignificant (p > 0.8) — it was riding on collinearity with the other
variables, not contributing independent signal.

### Final Selected Model: `xGF_PCT` + `HDGF_PCT`

```
Win ~ xGF_PCT + HDGF_PCT   (standardized inputs)

                coef      std err     z        P>|z|
const         -0.0008     0.028     -0.03      0.977
xGF_PCT        0.209      0.029      7.21     <0.001
HDGF_PCT       1.108      0.031     35.29     <0.001
```

- **Test Accuracy: 72.5%**
- **Test ROC-AUC: 0.775**
- Both coefficients positive, stable, and highly significant (p < 0.001)
- No multicollinearity concern (VIF ≈ 1.1)

**Interpretation:** `HDGF_PCT` is by far the dominant driver of win probability
— a team's share of high-danger goals matters roughly 5x as much (in
standardized terms) as its expected-goals share. `xGF_PCT` adds meaningful,
independent predictive value on top of that. The three shot-attempt-share
metrics (SF_PCT, CF_PCT, FF_PCT) are redundant with each other and add no
reliable independent signal once xGF_PCT and HDGF_PCT are in the model.

## 5. Visuals Included
1. `1_correlation_barchart.png` — raw correlation of each predictor with Win
2. `2_boxplots_by_outcome.png` — distribution of each predictor, Win vs Loss
3. `3_multicollinearity_heatmap.png` — inter-predictor correlation matrix
4. `4_full_model_coefficients.png` — full model's unstable coefficients
5. `5_full_model_roc.png` / `6_full_model_confusion.png` — full model performance
6. `7_tuning_comparison.png` — accuracy/AUC across all tested variable sets
7. `8_final_roc_comparison.png` — full model vs. tuned model ROC overlay
8. `9_final_model_summary.png` — final coefficients + confusion matrix

## 6. Recommendation
Deploy the **2-variable model (xGF_PCT + HDGF_PCT)**. It matches the full
model's performance, is far more statistically stable, and is easier to
communicate to coaches/analysts: *"win probability tracks high-danger goal
share first, expected-goal share second — shot-attempt volume metrics (Corsi/
Fenwick share) don't add anything once quality-adjusted metrics are in the
model."*
