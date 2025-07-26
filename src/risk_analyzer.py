import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class WalletRiskAnalyzer:
    """
    Analyze wallet risk patterns and generate risk indicators
    """
    
    def __init__(self):
        self.risk_features = None
        self.scaler = StandardScaler()
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        
    def calculate_risk_features(self, wallet_metrics: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate comprehensive risk features for each wallet
        
        Args:
            wallet_metrics (pd.DataFrame): Basic wallet metrics
            
        Returns:
            pd.DataFrame: Risk features
        """
        logger.info("Calculating risk features")
        
        risk_features = wallet_metrics.copy()
        
        # 1. Liquidation Risk Score
        risk_features['liquidation_risk_score'] = self._calculate_liquidation_risk(wallet_metrics)
        
        # 2. Behavioral Risk Score
        risk_features['behavioral_risk_score'] = self._calculate_behavioral_risk(wallet_metrics)
        
        # 3. Financial Health Score
        risk_features['financial_health_score'] = self._calculate_financial_health(wallet_metrics)
        
        # 4. Activity Pattern Risk
        risk_features['activity_pattern_risk'] = self._calculate_activity_pattern_risk(wallet_metrics)
        
        # 5. Repayment Behavior Score
        risk_features['repayment_behavior_score'] = self._calculate_repayment_behavior(wallet_metrics)
        
        # 6. Experience/Maturity Score
        risk_features['experience_score'] = self._calculate_experience_score(wallet_metrics)
        
        # 7. Diversification Score
        risk_features['diversification_score'] = self._calculate_diversification_score(wallet_metrics)
        
        # 8. Bot-like Behavior Score
        risk_features['bot_behavior_score'] = self._calculate_bot_behavior_score(wallet_metrics)
        
        self.risk_features = risk_features
        return risk_features
    
    def _calculate_liquidation_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate liquidation-based risk score (0-1, higher = more risky)"""
        liquidation_score = np.zeros(len(df))
        
        # Direct liquidation events
        liquidation_score += df['liquidation_count'] * 0.5
        
        # Liquidation rate
        liquidation_score += df['liquidation_rate'] * 0.3
        
        # Has liquidations (binary indicator)
        liquidation_score += df['has_liquidations'] * 0.2
        
        # Normalize to 0-1 range
        return np.clip(liquidation_score, 0, 1)
    
    def _calculate_behavioral_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate behavioral risk patterns (0-1, higher = more risky)"""
        behavioral_score = np.zeros(len(df))
        
        # High failure rate
        behavioral_score += (1 - df['success_rate']) * 0.3
        
        # Irregular activity patterns
        high_variance_mask = df['activity_regularity'] > df['activity_regularity'].quantile(0.8)
        behavioral_score[high_variance_mask] += 0.2
        
        # Excessive weekend/night activity
        behavioral_score += np.clip(df['weekend_activity_ratio'] - 0.3, 0, 1) * 0.2
        behavioral_score += np.clip(df['night_activity_ratio'] - 0.2, 0, 1) * 0.3
        
        return np.clip(behavioral_score, 0, 1)
    
    def _calculate_financial_health(self, df: pd.DataFrame) -> pd.Series:
        """Calculate financial health score (0-1, higher = better health)"""
        health_score = np.ones(len(df))  # Start with perfect health
        
        # Poor repayment behavior
        poor_repayment_mask = df['repay_to_borrow_ratio'] < 0.8
        health_score[poor_repayment_mask] -= 0.4
        
        # Low transaction diversity
        low_diversity_mask = df['action_diversity'] <= 2
        health_score[low_diversity_mask] -= 0.2
        
        # Very new accounts (less than 30 days)
        new_account_mask = df['account_age_days'] < 30
        health_score[new_account_mask] -= 0.2
        
        # High gas spending relative to activity (potential inefficiency)
        if df['avg_gas_per_tx'].max() > 0:
            high_gas_mask = df['avg_gas_per_tx'] > df['avg_gas_per_tx'].quantile(0.9)
            health_score[high_gas_mask] -= 0.2
        
        return np.clip(health_score, 0, 1)
    
    def _calculate_activity_pattern_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate activity pattern risk (0-1, higher = more risky)"""
        activity_risk = np.zeros(len(df))
        
        # Burst activity (high variance in daily transactions)
        if df['daily_activity_variance'].max() > 0:
            high_variance_mask = df['daily_activity_variance'] > df['daily_activity_variance'].quantile(0.8)
            activity_risk[high_variance_mask] += 0.3
        
        # Excessive daily transactions
        if df['max_daily_transactions'].max() > 0:
            high_daily_mask = df['max_daily_transactions'] > df['max_daily_transactions'].quantile(0.9)
            activity_risk[high_daily_mask] += 0.4
        
        # Very regular intervals (potential bot)
        very_regular_mask = (df['activity_regularity'] < 0.1) & (df['total_transactions'] > 5)
        activity_risk[very_regular_mask] += 0.3
        
        return np.clip(activity_risk, 0, 1)
    
    def _calculate_repayment_behavior(self, df: pd.DataFrame) -> pd.Series:
        """Calculate repayment behavior score (0-1, higher = better)"""
        repayment_score = np.zeros(len(df))
        
        # Good repayment ratio
        good_repayment_mask = df['repay_to_borrow_ratio'] >= 1.0
        repayment_score[good_repayment_mask] += 0.5
        
        # Moderate repayment
        moderate_repayment_mask = (df['repay_to_borrow_ratio'] >= 0.8) & (df['repay_to_borrow_ratio'] < 1.0)
        repayment_score[moderate_repayment_mask] += 0.3
        
        # Has borrowing activity (needed for repayment evaluation)
        has_borrowing_mask = df['borrow_count'] > 0
        repayment_score[~has_borrowing_mask] += 0.2  # Neutral score for non-borrowers
        
        # No liquidations
        no_liquidation_mask = df['liquidation_count'] == 0
        repayment_score[no_liquidation_mask] += 0.3
        
        return np.clip(repayment_score, 0, 1)
    
    def _calculate_experience_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate experience/maturity score (0-1, higher = more experienced)"""
        experience_score = np.zeros(len(df))
        
        # Account age scoring
        age_bins = [0, 30, 90, 180, 365, float('inf')]
        age_scores = [0.1, 0.3, 0.5, 0.7, 1.0]
        
        for i in range(len(age_bins) - 1):
            mask = (df['account_age_days'] > age_bins[i]) & (df['account_age_days'] <= age_bins[i + 1])
            experience_score[mask] = age_scores[i]
        
        # Transaction volume bonus
        high_volume_mask = df['total_transactions'] > df['total_transactions'].quantile(0.7)
        experience_score[high_volume_mask] += 0.2
        
        # Consistent activity bonus
        consistent_activity_mask = (df['activity_regularity'] > 0.1) & (df['activity_regularity'] < 1.0)
        experience_score[consistent_activity_mask] += 0.1
        
        return np.clip(experience_score, 0, 1)
    
    def _calculate_diversification_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate diversification score (0-1, higher = more diversified)"""
        diversification_score = np.zeros(len(df))
        
        # Action diversity scoring
        diversification_score += np.clip(df['action_diversity'] / 5, 0, 0.6)
        
        # Balanced activity (not over-concentrated in one action)
        balance_score = 1 - np.maximum.reduce([
            df['supply_ratio'], df['withdraw_ratio'], 
            df['borrow_ratio'], df['repay_ratio']
        ])
        diversification_score += balance_score * 0.4
        
        return np.clip(diversification_score, 0, 1)
    
    def _calculate_bot_behavior_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate bot-like behavior score (0-1, higher = more bot-like)"""
        bot_score = np.zeros(len(df))
        
        # Very regular timing
        very_regular_mask = (df['activity_regularity'] < 0.05) & (df['total_transactions'] > 10)
        bot_score[very_regular_mask] += 0.4
        
        # High night activity
        high_night_mask = df['night_activity_ratio'] > 0.5
        bot_score[high_night_mask] += 0.3
        
        # Excessive transactions per day
        if df['max_daily_transactions'].max() > 0:
            excessive_daily_mask = df['max_daily_transactions'] > 50
            bot_score[excessive_daily_mask] += 0.3
        
        return np.clip(bot_score, 0, 1)
    
    def detect_anomalies(self, risk_features: pd.DataFrame) -> pd.Series:
        """
        Detect anomalous wallets using isolation forest
        
        Args:
            risk_features (pd.DataFrame): Risk feature matrix
            
        Returns:
            pd.Series: Anomaly scores (-1 to 1, lower = more anomalous)
        """
        logger.info("Detecting anomalous wallet behavior")
        
        # Select numerical features for anomaly detection
        numerical_features = [
            'total_transactions', 'success_rate', 'account_age_days',
            'liquidation_count', 'repay_to_borrow_ratio', 'action_diversity',
            'activity_regularity', 'max_daily_transactions'
        ]
        
        # Filter features that exist in the dataframe
        available_features = [f for f in numerical_features if f in risk_features.columns]
        
        if len(available_features) == 0:
            logger.warning("No numerical features available for anomaly detection")
            return pd.Series(np.zeros(len(risk_features)), index=risk_features.index)
        
        # Prepare data
        X = risk_features[available_features].fillna(0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Detect anomalies
        anomaly_scores = self.anomaly_detector.fit_predict(X_scaled)
        anomaly_decision_scores = self.anomaly_detector.decision_function(X_scaled)
        
        # Normalize decision scores to 0-1 range (higher = less anomalous)
        normalized_scores = (anomaly_decision_scores - anomaly_decision_scores.min()) / (
            anomaly_decision_scores.max() - anomaly_decision_scores.min()
        )
        
        logger.info(f"Detected {sum(anomaly_scores == -1)} anomalous wallets out of {len(anomaly_scores)}")
        
        return pd.Series(normalized_scores, index=risk_features.index)
