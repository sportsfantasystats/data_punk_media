# =============================================================================
# DATA PUNK MEDIA — DATA STORYTELLING LIFECYCLE WORKSHOP
# Stage 2: Feature Engineering & Predictive Modeling
#
# Use Case : NHL History — Finding Early Teams with the Most Potential
# Dataset  : raw_clean_nhl_history_data.csv (1,809 team-seasons, 1917–2024)
# Target   : PTS_PCT (points percentage — a team's actual season performance)
# Key Idea : Compute Pythagorean Win Expectation (PWE), measure the gap
#            between potential and actual performance, then build regression
#            models to predict future PTS_PCT from team characteristics.
#
# Algorithms compared:
#   1. Linear Regression (OLS)       — our interpretable baseline
#   2. Ridge Regression              — handles multicollinearity with L2 penalty
#   3. Lasso Regression              — L1 penalty for automatic feature selection
#   4. Elastic Net                   — blends Ridge + Lasso (best of both)
#   5. Random Forest                 — captures non-linear interactions
#   6. Gradient Boosting (GBM)       — sequential ensemble, often top performer
#   7. Support Vector Regression     — effective in high-dimensional spaces
#
# Workshop structure:
#   Section 0 : Setup & package loading
#   Section 1 : Data loading & initial inspection
#   Section 2 : Data cleaning & filtering
#   Section 3 : Feature engineering  (PWE, PTS_PCT_DELTA, era flags)
#   Section 4 : Feature selection & train/test split
#   Section 5 : Model training       (7 algorithms)
#   Section 6 : Model evaluation & comparison
#   Section 7 : Story output         (Top 5 early teams with most potential)
# =============================================================================


# =============================================================================
# SECTION 0: SETUP & PACKAGE LOADING
# =============================================================================

# ---- Install packages if you don't already have them ----
# Uncomment and run once, then re-comment before the workshop.
# install.packages(c(
#   "tidyverse",   # data wrangling + ggplot2 visualisation
#   "caret",       # unified train/test + model comparison framework
#   "glmnet",      # Ridge, Lasso, Elastic Net
#   "randomForest",# Random Forest
#   "gbm",         # Gradient Boosting Machine
#   "e1071",       # Support Vector Regression
#   "scales",      # axis formatting helpers
#   "ggrepel"      # non-overlapping chart labels
# ))

library(tidyverse)
library(caret)
library(glmnet)
library(randomForest)
library(gbm)
library(e1071)
library(scales)
library(ggrepel)

# Reproducibility — set a seed so every participant gets identical results
set.seed(42)


# =============================================================================
# SECTION 1: DATA LOADING & INITIAL INSPECTION
# =============================================================================

# ---- Load the dataset ----
# Adjust this path to wherever you saved the CSV on your machine.
raw <- read.csv("raw_clean_nhl_history_data.csv", stringsAsFactors = FALSE)

# ---- Quick look at what we're working with ----
glimpse(raw)          # column types and first values
summary(raw)          # distributions and NA counts

cat("\n=== Dataset dimensions ===\n")
cat("Rows (team-seasons):", nrow(raw), "\n")
cat("Columns            :", ncol(raw), "\n")

cat("\n=== Seasons covered ===\n")
cat("Earliest:", min(raw$SEASON), "\n")
cat("Latest  :", max(raw$SEASON), "\n")
cat("Unique seasons:", length(unique(raw$SEASON)), "\n")

cat("\n=== TIME_PERIOD breakdown ===\n")
# TIME_PERIOD is a pre-assigned era label in the source data (1–5):
#   1 = 1917–1942  (Original Six era, incomplete stats)
#   2 = 1942–1970  (Post-war era, power play data starts)
#   3 = 1970–1980  (WHA era, expansion chaos)
#   4 = 1980–2000  (Modern era, full special teams stats)
#   5 = 2000–2024  (Analytics era, shot data complete)
print(table(raw$TIME_PERIOD))


# =============================================================================
# SECTION 2: DATA CLEANING & FILTERING
# =============================================================================
# Workshop discussion: what rows should we exclude and why?
# Key decisions:
#   - Drop WHA seasons: GF/GA are all zero — we can't compute PWE
#   - Drop the 1-game WHA Finland record: not a real season
#   - Keep all NHL seasons even if some columns are zero/NA (we'll impute)

# ---- Identify and remove WHA & invalid rows ----
cat("\n=== Rows with zero GF (these are WHA seasons with missing goal data) ===\n")
raw %>%
  filter(GF == 0) %>%
  select(SEASON, TEAM, GP, GF, GA) %>%
  print(n = 10)

# Remove rows where GF or GA are zero (PWE is undefined without goals)
# Also remove the single-game WHA Finland anomaly (PTS_PCT = 0, GP = 1)
nhl <- raw %>%
  filter(
    GF  > 0,       # must have goal data to compute PWE
    GA  > 0,       # must have goal data to compute PWE
    GP  > 5        # exclude micro-season stubs (e.g., the 1-game Finland row)
  )

cat("\nRows retained after cleaning:", nrow(nhl), "\n")
cat("Rows removed               :", nrow(raw) - nrow(nhl), "\n")

# ---- Convert TIME_PERIOD to a factor ----
# This tells R to treat it as a categorical variable (not a continuous number)
nhl$TIME_PERIOD <- factor(nhl$TIME_PERIOD, levels = 1:5,
                          labels = c("1917-1942", "1942-1970",
                                     "1970-1980", "1980-2000", "2000-2024"))


# =============================================================================
# SECTION 3: FEATURE ENGINEERING
# =============================================================================
# This is the most important section for our story.
# We create three new variables that will drive everything downstream.

# ---- 3.1 Pythagorean Win Expectation (PWE) ----
# Formula: PWE = GF^2 / (GF^2 + GA^2)
#
# Originally from baseball (Bill James), adapted for hockey.
# Interpretation: given how many goals a team scored and allowed,
# what winning percentage *should* they have had?
# A team with PWE > actual PTS_PCT was "unlucky" or underperforming.
# A team with PWE < actual PTS_PCT was "lucky" or overperforming.

nhl <- nhl %>%
  mutate(
    PWE = GF^2 / (GF^2 + GA^2)
  )

cat("\n=== PWE summary ===\n")
summary(nhl$PWE)

# Quick sanity check — PWE should be strongly correlated with PTS_PCT
cat("\nCorrelation between PWE and PTS_PCT:",
    round(cor(nhl$PWE, nhl$PTS_PCT, use = "complete.obs"), 3), "\n")

# ---- 3.2 PTS_PCT Delta (the "gap" variable — our story hook) ----
# Delta = PWE - PTS_PCT
# Positive delta → team underperformed relative to their goal-scoring ability
#                  ("they deserved more wins than they got")
# Negative delta → team overperformed relative to their goal-scoring ability

nhl <- nhl %>%
  mutate(
    PTS_PCT_DELTA = PWE - PTS_PCT
  )

cat("\n=== PTS_PCT_DELTA summary (PWE minus actual PTS_PCT) ===\n")
summary(nhl$PTS_PCT_DELTA)

# ---- 3.3 Per-game rates from raw totals (era-normalised efficiency) ----
# Raw counts (GF, GA, PP goals) vary by season length — using per-game rates
# makes early short seasons comparable to modern 82-game seasons.
# Note: GF_PER_GAME and GA_PER_GAME already exist in the source data,
# but we recompute them here for transparency and verification.

nhl <- nhl %>%
  mutate(
    GF_PG_CALC  = GF / GP,     # goals for per game  (recalculated)
    GA_PG_CALC  = GA / GP,     # goals against per game (recalculated)
    # Goal differential per game — compact efficiency signal
    GOAL_DIFF_PG = (GF - GA) / GP
  )

# Verify our recalculation matches the source column
cat("\nRecalculated vs. source GF_PER_GAME agree:",
    all(round(nhl$GF_PG_CALC, 3) == round(nhl$GF_PER_GAME, 3), na.rm = TRUE), "\n")

# ---- 3.4 Visualise the PWE delta distribution ----
# Workshop: is the distribution symmetric? What does a big positive delta mean?

ggplot(nhl, aes(x = PTS_PCT_DELTA)) +
  geom_histogram(binwidth = 0.02, fill = "#1D9E75", colour = "white", alpha = 0.85) +
  geom_vline(xintercept = 0, colour = "#D85A30", linewidth = 0.8, linetype = "dashed") +
  labs(
    title    = "Distribution of PWE Delta (Potential minus Actual Performance)",
    subtitle = "Positive = team underperformed their goal-scoring talent | Negative = overperformed",
    x        = "PWE − PTS_PCT",
    y        = "Number of team-seasons",
    caption  = "Source: NHL historical team stats (1917–2024)"
  ) +
  theme_minimal(base_size = 13)

# ---- 3.5 Which early teams had the largest positive delta? ----
# Preview of our eventual story output (we'll formalise this in Section 7)

cat("\n=== Top 10 single seasons by PTS_PCT_DELTA (most potential unrealised) ===\n")
nhl %>%
  filter(TIME_PERIOD == "1917-1942") %>%   # focus on the early era
  arrange(desc(PTS_PCT_DELTA)) %>%
  select(SEASON, TEAM, GP, GF, GA, PTS_PCT, PWE, PTS_PCT_DELTA) %>%
  mutate(across(c(PTS_PCT, PWE, PTS_PCT_DELTA), ~ round(.x, 3))) %>%
  head(10) %>%
  print()


# =============================================================================
# SECTION 4: FEATURE SELECTION & TRAIN / TEST SPLIT
# =============================================================================

# ---- 4.1 Choose features for modeling ----
# Target variable: PTS_PCT (points percentage for that season)
#
# Feature selection philosophy:
#   INCLUDE: variables available across all (or nearly all) seasons, that
#            capture team quality without DIRECTLY encoding the target.
#   EXCLUDE: W, L, PTS  — these are algebraically derived FROM PTS_PCT;
#            including them would be data leakage (the model would just
#            back-calculate the target).
#   EXCLUDE: SHOTS, SHOT_PCT, SAVE_PCT, AVG_AGE — only available post-1959;
#            keeping them would drop ~363 rows and bias us toward modern teams.
#   EXCLUDE: OL, SOW, SOL — only exist in the shootout era (post-2005).
#   INCLUDE with care: PP, PPO, PPA, PPOA, SHG, SHA — sparse in early eras
#            but non-zero values are real and informative; we'll impute zeros.

feature_cols <- c(
  # Core efficiency — the heart of our story
  "PWE",              # Pythagorean Win Expectation (our engineered feature)
  "PTS_PCT_DELTA",    # PWE minus actual PTS_PCT (potential gap)
  "GOAL_DIFF_PG",     # goal differential per game (compact quality signal)
  "GF_PER_GAME",      # goals scored per game
  "GA_PER_GAME",      # goals allowed per game

  # Team strength context
  "SRS",              # Simple Rating System: avg margin of victory adjusted for SOS
  "SOS",              # Strength of Schedule

  # Special teams — available for ~94% of seasons, impute rest with 0
  "PP",               # power play goals scored
  "PPA",              # power play goals allowed
  "SHG",              # shorthanded goals scored
  "SHA",              # shorthanded goals allowed
  "PIM_PER_GAME",     # penalties in minutes per game
  "OPIM_PER_GAME",    # opponent penalties per game (proxy for opponent discipline)

  # Era label — important! captures rule changes and league context
  "TIME_PERIOD"       # factor: 1917-1942, 1942-1970, etc.
)

target_col <- "PTS_PCT"

# ---- 4.2 Build the modelling dataframe ----
model_df <- nhl %>%
  select(all_of(c(feature_cols, target_col, "SEASON", "TEAM"))) %>%
  # Impute remaining NAs/zeros with column median
  # (zeros in PP etc. for early seasons are truly zero, not missing)
  mutate(across(where(is.numeric), ~ ifelse(is.na(.x), median(.x, na.rm = TRUE), .x)))

cat("\n=== Modelling dataframe ===\n")
cat("Rows:", nrow(model_df), "\n")
cat("Columns:", ncol(model_df), "\n")
cat("Any remaining NAs:", anyNA(model_df), "\n")

# ---- 4.3 Train / Test split ----
# We split 75% training / 25% test.
# IMPORTANT: we use createDataPartition() from caret, which stratifies the
# split on the target variable — this ensures both splits have a similar
# distribution of PTS_PCT values (not all great teams in train, poor in test).

train_index <- createDataPartition(
  model_df[[target_col]],
  p     = 0.75,   # 75% training
  list  = FALSE,
  times = 1
)

train_df <- model_df[ train_index, ]
test_df  <- model_df[-train_index, ]

cat("\n=== Train / Test split ===\n")
cat("Training rows:", nrow(train_df), "\n")
cat("Test rows    :", nrow(test_df),  "\n")

# Verify the target distribution looks similar in both splits
cat("\nTrain PTS_PCT — mean:", round(mean(train_df$PTS_PCT), 3),
    " | sd:", round(sd(train_df$PTS_PCT), 3), "\n")
cat("Test  PTS_PCT — mean:", round(mean(test_df$PTS_PCT),  3),
    " | sd:", round(sd(test_df$PTS_PCT),  3), "\n")

# ---- 4.4 Prepare X and y matrices ----
# Most algorithms need a numeric-only X matrix; we one-hot encode TIME_PERIOD.
# createDataPartition keeps the factor intact, but glmnet/SVR need a matrix.

# Formula without SEASON and TEAM (identifier columns, not features)
model_formula <- as.formula(paste(target_col, "~",
                                  paste(feature_cols, collapse = " + ")))

# X matrices — model.matrix() drops the intercept and dummy-encodes factors
X_train <- model.matrix(model_formula, data = train_df)[, -1]  # remove intercept
X_test  <- model.matrix(model_formula, data = test_df)[, -1]
y_train <- train_df[[target_col]]
y_test  <- test_df[[target_col]]

cat("\nX_train dimensions:", dim(X_train), "\n")
cat("Feature names:\n")
print(colnames(X_train))


# =============================================================================
# SECTION 5: MODEL TRAINING
# =============================================================================
# We define a shared cross-validation control so all models are evaluated
# consistently. 10-fold CV on the training set ensures we're not overfitting
# to a lucky train/test split.

cv_control <- trainControl(
  method  = "cv",
  number  = 10,       # 10-fold cross-validation
  verboseIter = FALSE # set TRUE if you want fold-by-fold progress
)

# We'll collect results in a list as we go
model_results <- list()

# Helper function: evaluate a fitted model on the held-out test set
evaluate_model <- function(model_name, predictions, actuals) {
  residuals <- actuals - predictions
  rmse <- sqrt(mean(residuals^2))
  mae  <- mean(abs(residuals))
  ss_res <- sum(residuals^2)
  ss_tot <- sum((actuals - mean(actuals))^2)
  r2   <- 1 - ss_res / ss_tot
  tibble(
    Model = model_name,
    RMSE  = round(rmse, 5),
    MAE   = round(mae,  5),
    R2    = round(r2,   4)
  )
}


# -----------------------------------------------------------------------
# MODEL 1: Ordinary Least Squares Linear Regression
# -----------------------------------------------------------------------
# Our interpretable baseline. No regularisation — every feature is used
# as-is. Good for understanding the direction and magnitude of each effect.
# Weakness: sensitive to multicollinearity (e.g. GF_PER_GAME and PWE overlap).

cat("\n--- Training Model 1: Linear Regression (OLS) ---\n")

m_ols <- train(
  model_formula,
  data      = train_df,
  method    = "lm",
  trControl = cv_control
)

pred_ols <- predict(m_ols, newdata = test_df)
model_results[["Linear Regression"]] <- evaluate_model("Linear Regression", pred_ols, y_test)

cat("OLS 10-fold CV RMSE:", round(min(m_ols$results$RMSE), 5), "\n")
cat("OLS coefficient summary:\n")
print(summary(m_ols$finalModel)$coefficients)


# -----------------------------------------------------------------------
# MODEL 2: Ridge Regression (L2 regularisation)
# -----------------------------------------------------------------------
# Ridge shrinks all coefficients toward zero by adding a penalty term
# proportional to the squared sum of coefficients (lambda * sum(beta^2)).
# Keeps all features but reduces their influence proportionally.
# Best when many features each contribute a little (as here).
# Lambda is tuned automatically via cross-validation on alpha=0.

cat("\n--- Training Model 2: Ridge Regression ---\n")

m_ridge <- train(
  x         = X_train,
  y         = y_train,
  method    = "glmnet",
  trControl = cv_control,
  tuneGrid  = expand.grid(
    alpha  = 0,                              # alpha=0 → Ridge
    lambda = 10^seq(-4, 1, length.out = 50) # search log-scale grid
  )
)

best_lambda_ridge <- m_ridge$bestTune$lambda
cat("Best lambda (Ridge):", round(best_lambda_ridge, 6), "\n")

pred_ridge <- predict(m_ridge, newx = X_test)
model_results[["Ridge Regression"]] <- evaluate_model("Ridge Regression", pred_ridge, y_test)


# -----------------------------------------------------------------------
# MODEL 3: Lasso Regression (L1 regularisation)
# -----------------------------------------------------------------------
# Lasso adds a penalty proportional to the absolute sum of coefficients
# (lambda * sum(|beta|)). Unlike Ridge, Lasso can shrink coefficients
# all the way to zero — performing automatic feature selection.
# Useful for identifying the *minimum* set of features that explain PTS_PCT.

cat("\n--- Training Model 3: Lasso Regression ---\n")

m_lasso <- train(
  x         = X_train,
  y         = y_train,
  method    = "glmnet",
  trControl = cv_control,
  tuneGrid  = expand.grid(
    alpha  = 1,                              # alpha=1 → Lasso
    lambda = 10^seq(-4, 1, length.out = 50)
  )
)

best_lambda_lasso <- m_lasso$bestTune$lambda
cat("Best lambda (Lasso):", round(best_lambda_lasso, 6), "\n")

# Workshop insight: which features did Lasso zero out?
lasso_coefs <- coef(m_lasso$finalModel, s = best_lambda_lasso)
cat("Lasso non-zero coefficients:\n")
print(lasso_coefs[lasso_coefs[,1] != 0, , drop = FALSE])

pred_lasso <- predict(m_lasso, newx = X_test)
model_results[["Lasso Regression"]] <- evaluate_model("Lasso Regression", pred_lasso, y_test)


# -----------------------------------------------------------------------
# MODEL 4: Elastic Net (blend of Ridge + Lasso)
# -----------------------------------------------------------------------
# Elastic Net tunes BOTH alpha (mix ratio) and lambda (penalty strength).
# alpha = 0 → pure Ridge | alpha = 1 → pure Lasso | 0 < alpha < 1 → blend
# Often outperforms either Ridge or Lasso alone when features are correlated.

cat("\n--- Training Model 4: Elastic Net ---\n")

m_enet <- train(
  x         = X_train,
  y         = y_train,
  method    = "glmnet",
  trControl = cv_control,
  tuneGrid  = expand.grid(
    alpha  = seq(0, 1, by = 0.1),            # search alpha mix ratio
    lambda = 10^seq(-4, 1, length.out = 30)  # and penalty strength
  )
)

cat("Best alpha (Elastic Net):", m_enet$bestTune$alpha, "\n")
cat("Best lambda (Elastic Net):", round(m_enet$bestTune$lambda, 6), "\n")

pred_enet <- predict(m_enet, newx = X_test)
model_results[["Elastic Net"]] <- evaluate_model("Elastic Net", pred_enet, y_test)


# -----------------------------------------------------------------------
# MODEL 5: Random Forest
# -----------------------------------------------------------------------
# An ensemble of decision trees, each trained on a bootstrap sample of rows
# and a random subset of features. Predictions are averaged across all trees.
# Captures non-linear relationships and feature interactions automatically.
# mtry = number of features randomly considered at each split (tuned via CV).
# ntree = 500 is a practical default; more trees → more stable but slower.

cat("\n--- Training Model 5: Random Forest ---\n")

m_rf <- train(
  model_formula,
  data      = train_df,
  method    = "rf",
  trControl = cv_control,
  tuneGrid  = expand.grid(mtry = c(2, 4, 6, 8, 10)),  # tune sqrt(p) neighborhood
  ntree     = 500
)

cat("Best mtry (Random Forest):", m_rf$bestTune$mtry, "\n")

pred_rf <- predict(m_rf, newdata = test_df)
model_results[["Random Forest"]] <- evaluate_model("Random Forest", pred_rf, y_test)

# Feature importance — great talking point in the workshop
cat("Random Forest feature importance (top 10):\n")
rf_imp <- varImp(m_rf)$importance
rf_imp <- rf_imp %>%
  arrange(desc(Overall)) %>%
  head(10)
print(rf_imp)


# -----------------------------------------------------------------------
# MODEL 6: Gradient Boosting Machine (GBM)
# -----------------------------------------------------------------------
# GBM builds trees sequentially — each new tree corrects the residual
# errors of the previous ensemble. Often the top performer on structured
# tabular data. Key hyperparameters:
#   n.trees         = number of boosting iterations
#   interaction.depth = max tree depth (1 = stumps, higher = more interaction)
#   shrinkage       = learning rate (smaller → slower but more robust)
#   n.minobsinnode  = minimum leaf size (guards against overfitting)

cat("\n--- Training Model 6: Gradient Boosting Machine (GBM) ---\n")

gbm_grid <- expand.grid(
  n.trees           = c(100, 200, 300),
  interaction.depth = c(1, 3, 5),
  shrinkage         = c(0.01, 0.1),
  n.minobsinnode    = c(5, 10)
)

m_gbm <- train(
  model_formula,
  data      = train_df,
  method    = "gbm",
  trControl = cv_control,
  tuneGrid  = gbm_grid,
  verbose   = FALSE   # suppress iteration-level output
)

cat("Best GBM parameters:\n")
print(m_gbm$bestTune)

pred_gbm <- predict(m_gbm, newdata = test_df)
model_results[["Gradient Boosting"]] <- evaluate_model("Gradient Boosting", pred_gbm, y_test)


# -----------------------------------------------------------------------
# MODEL 7: Support Vector Regression (SVR)
# -----------------------------------------------------------------------
# SVR finds a hyperplane that fits the training data within a margin of
# tolerance (epsilon). Points outside the margin incur a penalty (C).
# With a radial basis function (RBF) kernel, SVR can capture non-linear
# patterns without explicitly engineering polynomial features.
# Hyperparameters: C (cost/penalty), sigma (RBF bandwidth), epsilon (tube width).

cat("\n--- Training Model 7: Support Vector Regression (SVR) ---\n")

svr_grid <- expand.grid(
  C     = c(0.1, 1, 10),
  sigma = c(0.01, 0.05, 0.1),  # RBF kernel width
  # Note: caret's svmRadial uses sigma, not gamma
)

m_svr <- train(
  x         = X_train,
  y         = y_train,
  method    = "svmRadial",
  trControl = cv_control,
  tuneGrid  = svr_grid,
  preProcess = c("center", "scale")  # SVR requires feature scaling
)

cat("Best SVR parameters:\n")
print(m_svr$bestTune)

pred_svr <- predict(m_svr, newdata = X_test)
model_results[["SVR"]] <- evaluate_model("SVR", pred_svr, y_test)


# =============================================================================
# SECTION 6: MODEL EVALUATION & COMPARISON
# =============================================================================

# ---- 6.1 Combine all results into one comparison table ----
results_df <- bind_rows(model_results) %>%
  arrange(RMSE)   # rank by test-set RMSE (lower = better)

cat("\n")
cat("==========================================================\n")
cat("          MODEL COMPARISON — Test Set Performance          \n")
cat("==========================================================\n")
cat("Metric definitions:\n")
cat("  RMSE : Root Mean Squared Error — lower is better\n")
cat("         Penalises large errors more than small ones.\n")
cat("         In our context: avg error in points-percentage units.\n")
cat("  MAE  : Mean Absolute Error — lower is better\n")
cat("         Average magnitude of prediction error.\n")
cat("  R²   : Coefficient of Determination — higher is better\n")
cat("         Proportion of variance in PTS_PCT explained by model.\n")
cat("         1.0 = perfect | 0.0 = no better than predicting the mean.\n\n")
print(results_df, n = Inf)

best_model_name <- results_df$Model[1]
cat("\n🏆 Best model by RMSE:", best_model_name, "\n")

# ---- 6.2 Visual comparison: RMSE bar chart ----
ggplot(results_df, aes(x = reorder(Model, -RMSE), y = RMSE, fill = RMSE)) +
  geom_col(width = 0.65) +
  geom_text(aes(label = round(RMSE, 5)), hjust = -0.1, size = 3.5) +
  coord_flip() +
  scale_fill_gradient(low = "#1D9E75", high = "#D85A30", guide = "none") +
  labs(
    title    = "Model Comparison — Test Set RMSE",
    subtitle = "Lower RMSE = better predictive accuracy for PTS_PCT",
    x        = NULL,
    y        = "Root Mean Squared Error (test set)",
    caption  = "All models trained with 10-fold CV on 75% of data; evaluated on held-out 25%"
  ) +
  theme_minimal(base_size = 13) +
  expand_limits(y = max(results_df$RMSE) * 1.15)

# ---- 6.3 Visual comparison: R² bar chart ----
ggplot(results_df, aes(x = reorder(Model, R2), y = R2, fill = R2)) +
  geom_col(width = 0.65) +
  geom_text(aes(label = round(R2, 4)), hjust = -0.1, size = 3.5) +
  coord_flip() +
  scale_fill_gradient(low = "#D85A30", high = "#1D9E75", guide = "none") +
  labs(
    title    = "Model Comparison — Test Set R²",
    subtitle = "Higher R² = more variance in PTS_PCT explained by the model",
    x        = NULL,
    y        = "R² (test set)",
    caption  = "All models trained with 10-fold CV on 75% of data; evaluated on held-out 25%"
  ) +
  theme_minimal(base_size = 13) +
  expand_limits(y = min(1.0, max(results_df$R2) * 1.12))

# ---- 6.4 Actual vs Predicted scatter — best model ----
# Run this block for the winning algorithm (adjust pred_* as needed)
best_preds <- switch(best_model_name,
  "Linear Regression" = pred_ols,
  "Ridge Regression"  = pred_ridge,
  "Lasso Regression"  = pred_lasso,
  "Elastic Net"       = pred_enet,
  "Random Forest"     = pred_rf,
  "Gradient Boosting" = pred_gbm,
  "SVR"               = pred_svr
)

scatter_df <- tibble(
  Actual    = y_test,
  Predicted = best_preds,
  SEASON    = test_df$SEASON,
  TEAM      = test_df$TEAM
)

ggplot(scatter_df, aes(x = Actual, y = Predicted)) +
  geom_point(alpha = 0.4, colour = "#1D9E75", size = 2) +
  geom_abline(slope = 1, intercept = 0, colour = "#D85A30", linewidth = 0.8) +
  geom_text_repel(
    data  = scatter_df %>% filter(abs(Actual - Predicted) > 0.08),
    aes(label = paste0(TEAM, " (", SEASON, ")")),
    size  = 2.8, max.overlaps = 10
  ) +
  labs(
    title    = paste("Actual vs Predicted PTS_PCT —", best_model_name),
    subtitle = "Points near the red diagonal = accurate predictions | Outliers labelled",
    x        = "Actual PTS_PCT",
    y        = "Predicted PTS_PCT",
    caption  = "Labelled points: absolute prediction error > 0.08"
  ) +
  theme_minimal(base_size = 13)


# =============================================================================
# SECTION 7: STORY OUTPUT — TOP 5 EARLY TEAMS WITH THE MOST POTENTIAL
# =============================================================================
# This is where modeling meets narrative.
# We focus on TIME_PERIOD 1 (1917–1942) and TIME_PERIOD 2 (1942–1970) —
# the "early era" teams our workshop story is about.
#
# Approach:
#   1. Filter to early-era seasons.
#   2. Use the best model to predict each team's "expected future PTS_PCT"
#      based on their underlying performance characteristics (PWE, SRS, etc.)
#   3. Take the average predicted PTS_PCT per franchise across their seasons.
#   4. Compare to their average *actual* PTS_PCT.
#   5. The largest gap (predicted >> actual) = "most potential unrealised."

cat("\n=== Building story output: Top 5 early teams with the most potential ===\n")

# Filter to early eras only (TIME_PERIOD 1 and 2)
early_df <- nhl %>%
  filter(TIME_PERIOD %in% c("1917-1942", "1942-1970"))

cat("Early era team-seasons:", nrow(early_df), "\n")

# Generate predictions for all early-era rows using the best model
# (We use the full model — trained on all eras — to avoid look-ahead bias
#  and to leverage the full statistical patterns in the data.)
early_X <- model.matrix(model_formula, data = early_df)[, -1]

early_preds <- switch(best_model_name,
  "Linear Regression" = predict(m_ols,  newdata = early_df),
  "Ridge Regression"  = as.vector(predict(m_ridge, newx = early_X)),
  "Lasso Regression"  = as.vector(predict(m_lasso, newx = early_X)),
  "Elastic Net"       = as.vector(predict(m_enet,  newx = early_X)),
  "Random Forest"     = predict(m_rf,   newdata = early_df),
  "Gradient Boosting" = predict(m_gbm,  newdata = early_df),
  "SVR"               = as.vector(predict(m_svr,  newdata = early_X))
)

early_df$PREDICTED_PTS_PCT <- early_preds

# Aggregate by franchise — average across all their early seasons
franchise_summary <- early_df %>%
  group_by(TEAM) %>%
  summarise(
    N_SEASONS         = n(),
    AVG_ACTUAL_PTS    = round(mean(PTS_PCT), 3),
    AVG_PWE           = round(mean(PWE), 3),
    AVG_PREDICTED_PTS = round(mean(PREDICTED_PTS_PCT), 3),
    AVG_DELTA         = round(mean(PTS_PCT_DELTA), 3),     # mean PWE - actual gap
    AVG_SRS           = round(mean(SRS), 3),
    FIRST_SEASON      = min(SEASON),
    LAST_SEASON       = max(SEASON),
    .groups = "drop"
  ) %>%
  # POTENTIAL SCORE: predicted future quality minus actual historical quality
  mutate(
    POTENTIAL_SCORE = round(AVG_PREDICTED_PTS - AVG_ACTUAL_PTS, 3)
  ) %>%
  filter(N_SEASONS >= 3) %>%     # require at least 3 seasons for reliability
  arrange(desc(POTENTIAL_SCORE))

cat("\n=== All early-era franchises ranked by Potential Score ===\n")
print(franchise_summary, n = 30)

# ---- The Top 5 ----
top5 <- franchise_summary %>%
  head(5)

cat("\n")
cat("══════════════════════════════════════════════════════════════\n")
cat("  TOP 5 EARLY NHL TEAMS WITH THE MOST UNREALISED POTENTIAL\n")
cat("══════════════════════════════════════════════════════════════\n")

top5 %>%
  mutate(RANK = row_number()) %>%
  select(RANK, TEAM, FIRST_SEASON, LAST_SEASON, N_SEASONS,
         AVG_ACTUAL_PTS, AVG_PWE, AVG_PREDICTED_PTS, POTENTIAL_SCORE) %>%
  print()

# ---- Top 5 visualisation ----
ggplot(top5, aes(x = reorder(TEAM, POTENTIAL_SCORE))) +
  geom_segment(aes(xend = TEAM,
                   y    = AVG_ACTUAL_PTS,
                   yend = AVG_PREDICTED_PTS),
               colour = "#888780", linewidth = 0.6) +
  geom_point(aes(y = AVG_ACTUAL_PTS,    colour = "Actual"),    size = 4) +
  geom_point(aes(y = AVG_PREDICTED_PTS, colour = "Predicted"), size = 4) +
  coord_flip() +
  scale_colour_manual(
    values = c("Actual" = "#D85A30", "Predicted" = "#1D9E75"),
    name   = "Performance"
  ) +
  labs(
    title    = "Top 5 Early NHL Teams: Actual vs Model-Predicted PTS%",
    subtitle = "The gap between dots = unrealised potential based on underlying team quality",
    x        = NULL,
    y        = "Points Percentage (PTS_PCT)",
    caption  = paste("Predictions from:", best_model_name,
                     "| Early eras: 1917–1970 | Min. 3 seasons")
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "bottom")

cat("\n✅ Workshop complete. The story hook:\n")
cat("   '", top5$TEAM[1], "had the largest gap between what their\n")
cat("    goal-scoring ability suggested they should achieve and\n")
cat("    what they actually delivered — making them the greatest\n")
cat("    early team that never reached their potential.'\n\n")
cat("   Next steps (Stage 2 continued): Narrative Structure →\n")
cat("   Content Design → Content Production.\n")
