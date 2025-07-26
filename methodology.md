# Wallet Risk Scoring Methodology

## Executive Summary

This document details the comprehensive methodology used to assess wallet risk profiles based on Compound protocol transaction behavior. Our scoring system evaluates multiple risk dimensions to generate scores from 0-1000, providing actionable insights for DeFi risk management.

## Data Collection Strategy

### Blockchain Data Sources

**Primary Data Collection:**
- **Etherscan API**: Transaction history, gas usage, contract interactions
- **Web3 Direct Queries**: Real-time blockchain state, contract calls
- **Compound Protocol Events**: Mint, redeem, borrow, repay, liquidation events

**Transaction Types Analyzed:**
1. **Regular Transactions**: Direct wallet-to-contract interactions
2. **Internal Transactions**: Contract-initiated value transfers  
3. **Token Transfers**: ERC-20 token movements
4. **Contract Interactions**: Method calls and state changes

### Data Quality Assurance

**Validation Steps:**
- Transaction hash verification
- Timestamp consistency checks
- Gas price reasonableness validation
- Contract address verification against known Compound contracts

**Error Handling:**
- Failed transaction analysis
- Timeout and retry mechanisms
- Fallback data sources
- Data completeness scoring

## Risk Feature Engineering

### 1. Liquidation Risk Assessment (25% Weight)

**Core Metrics:**
- `liquidation_count`: Total liquidation events
- `liquidation_rate`: Liquidations per total transactions
- `liquidation_severity`: Average liquidation amounts

**Calculation:**
$`liquidation_risk = (
liquidation_count * 0.5 +
liquidation_rate * 0.3 +
has_liquidations_binary * 0.2
)`$


**Risk Interpretation:**
- 0.0-0.2: Excellent liquidation management
- 0.2-0.5: Moderate liquidation risk
- 0.5-0.8: High liquidation risk
- 0.8-1.0: Severe liquidation history

### 2. Financial Health Score (20% Weight)

**Components:**
- **Repayment Consistency**: On-time repayment patterns
- **Position Management**: Collateralization maintenance
- **Account Maturity**: Time since first transaction
- **Gas Efficiency**: Cost management patterns

**Calculation:**

$` health_score = 1.0 # Start with perfect health
if repay_to_borrow_ratio < 0.8:
health_score -= 0.4
if action_diversity <= 2:
health_score -= 0.2
if account_age_days < 30:
health_score -= 0.2 `$


### 3. Behavioral Risk Patterns (15% Weight)

**Bot-like Behavior Detection:**
- **Regular Intervals**: CV of transaction timing < 0.05
- **Excessive Volume**: >50 transactions per day
- **Timing Patterns**: >50% night activity (12 AM - 6 AM)

**Calculation:**
$`behavioral_risk = (
(1 - success_rate) * 0.3 +
irregular_activity_penalty * 0.2 +
excessive_night_activity * 0.3 +
weekend_activity_penalty * 0.2
)`$


### 4. Repayment Behavior Analysis (15% Weight)

**Key Metrics:**
- `repay_to_borrow_ratio`: Repayment completeness
- `repayment_timing`: Average time to repay
- `partial_repayment_frequency`: Gradual vs. full repayments

**Scoring Logic:**
$`if repay_to_borrow_ratio >= 1.0:
repayment_score += 0.5 # Excellent
elif repay_to_borrow_ratio >= 0.8:
repayment_score += 0.3 # Good
elif repay_to_borrow_ratio >= 0.5:
repayment_score += 0.1 # Fair `$

##### Below 0.5 = Poor (no bonus)


### 5. Experience and Maturity (10% Weight)

**Experience Metrics:**
- **Account Age**: Days since first transaction
- **Transaction Volume**: Total protocol interactions
- **Consistency**: Regular but not mechanical usage

**Maturity Scoring:**
$`age_score_mapping = {
(0, 30): 0.1, # New user
(30, 90): 0.3, # Learning phase
(90, 180): 0.5, # Developing user
(180, 365): 0.7, # Experienced user
(365, inf): 1.0 # Veteran user
}`$


### 6. Activity Pattern Risk (10% Weight)

**Pattern Analysis:**
- **Burst Detection**: High variance in daily transaction counts
- **Regularity Assessment**: Coefficient of variation in timing
- **Volume Spikes**: Sudden increases in transaction frequency

**Risk Indicators:**
- Daily transaction variance > 90th percentile
- Activity regularity < 0.1 (too regular, potential bot)
- Maximum daily transactions > 90th percentile

### 7. Diversification Score (5% Weight)

**Diversity Metrics:**
- **Action Variety**: Number of different Compound actions used
- **Balance Assessment**: Concentration in single action type
- **Protocol Engagement**: Breadth of feature usage

**Calculation:**
$` diversification = (
min(action_diversity / 5, 0.6) +
(1 - max_action_concentration) * 0.4
) `$


## Anomaly Detection Framework

### Isolation Forest Implementation

**Feature Selection for Anomaly Detection:**
- Transaction frequency patterns
- Gas usage efficiency
- Success rate anomalies
- Timing irregularities

**Anomaly Score Integration:**
$` final_risk = composite_risk + (1 - anomaly_score) * 0.1 `$


**Anomaly Interpretation:**
- Scores < -0.5: Highly anomalous behavior
- Scores -0.5 to 0: Moderately unusual
- Scores 0 to 0.5: Normal behavior
- Scores > 0.5: Highly typical behavior

## Score Normalization and Scaling

### Composite Score Calculation

**Weighted Risk Components:**

$` composite_risk = (
liquidation_risk * 0.25 +
behavioral_risk * 0.15 +
(1 - financial_health) * 0.20 +
activity_pattern_risk * 0.10 +
(1 - repayment_behavior) * 0.15 +
(1 - experience_score) * 0.10 +
(1 - diversification) * 0.05
) `$


### Final Score Transformation

**Risk-to-Credit Score Conversion:**

##### Convert risk score (0-1, higher = more risky) to credit score (0-1000, higher = better)
$`credit_score = (1 - final_risk_score) * 1000
credit_score = round(clip(credit_score, 0, 1000))`$


## Risk Category Classification

| Score Range | Category        | Risk Level |          Recommendation             |
|-------------|-----------------|------------|-------------------------------------|
| 800-1000    | Low Risk        |  Excellent | Standard terms, lowest rates        |
| 600-799     | Medium-Low Risk |    Good    | Standard terms with monitoring      |
| 400-599     | Medium Risk     |   Moderate | Enhanced monitoring, adjusted terms |
| 200-399     | High Risk       | Concerning | Strict oversight, premium rates     |
| 0-199       | Very High Risk  |    Severe  | Consider restricting access         |