import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class RiskScoreCalculator:
    """
    Calculate final risk scores for wallets
    """
    
    def __init__(self):
        self.scores = None
        self.weights = {
            'liquidation_risk': 0.25,
            'behavioral_risk': 0.15,
            'financial_health': 0.20,
            'activity_pattern_risk': 0.10,
            'repayment_behavior': 0.15,
            'experience': 0.10,
            'diversification': 0.05
        }
    
    def calculate_scores(self, risk_features: pd.DataFrame, anomaly_scores: pd.Series) -> pd.DataFrame:
        """
        Calculate final risk scores (0-1000 scale)
        
        Args:
            risk_features (pd.DataFrame): Risk feature matrix
            anomaly_scores (pd.Series): Anomaly detection scores
            
        Returns:
            pd.DataFrame: Final scores and risk categories
        """
        logger.info("Calculating final risk scores")
        
        scores_df = pd.DataFrame(index=risk_features.index)
        scores_df['wallet_id'] = risk_features['wallet_address']
        
        # Calculate composite risk score
        composite_score = self._calculate_composite_score(risk_features)
        
        # Incorporate anomaly scores
        anomaly_adjustment = (1 - anomaly_scores) * 0.1  # Anomalies reduce score
        
        # Final score calculation
        # Lower risk = higher score
        final_risk_score = composite_score - anomaly_adjustment
        final_risk_score = np.clip(final_risk_score, 0, 1)
        
        # Convert risk score to credit score (inverse relationship)
        # Risk score 0 (lowest risk) = Credit score 1000
        # Risk score 1 (highest risk) = Credit score 0
        credit_scores = (1 - final_risk_score) * 1000
        
        scores_df['score'] = credit_scores.round(0).astype(int)
        scores_df['risk_category'] = self._categorize_risk(credit_scores)
        
        # Add component scores for transparency
        scores_df['liquidation_risk_component'] = risk_features.get('liquidation_risk_score', 0)
        scores_df['behavioral_risk_component'] = risk_features.get('behavioral_risk_score', 0)
        scores_df['financial_health_component'] = risk_features.get('financial_health_score', 0)
        scores_df['repayment_behavior_component'] = risk_features.get('repayment_behavior_score', 0)
        scores_df['experience_component'] = risk_features.get('experience_score', 0)
        scores_df['anomaly_score'] = anomaly_scores
        
        # Additional metrics for analysis
        scores_df['total_transactions'] = risk_features.get('total_transactions', 0)
        scores_df['account_age_days'] = risk_features.get('account_age_days', 0)
        scores_df['liquidation_count'] = risk_features.get('liquidation_count', 0)
        scores_df['success_rate'] = risk_features.get('success_rate', 0)
        
        self.scores = scores_df
        logger.info(f"Calculated scores for {len(scores_df)} wallets")
        
        return scores_df
    
    def _calculate_composite_score(self, risk_features: pd.DataFrame) -> pd.Series:
        """
        Calculate weighted composite risk score
        
        Args:
            risk_features (pd.DataFrame): Risk features
            
        Returns:
            pd.Series: Composite risk scores (0-1, higher = more risky)
        """
        composite_score = np.zeros(len(risk_features))
        
        # Risk components (higher values = more risky)
        risk_components = {
            'liquidation_risk_score': self.weights['liquidation_risk'],
            'behavioral_risk_score': self.weights['behavioral_risk'],
            'activity_pattern_risk': self.weights['activity_pattern_risk']
        }
        
        for component, weight in risk_components.items():
            if component in risk_features.columns:
                composite_score += risk_features[component].fillna(0) * weight
        
        # Positive components (higher values = lower risk, so we subtract from 1)
        positive_components = {
            'financial_health_score': self.weights['financial_health'],
            'repayment_behavior_score': self.weights['repayment_behavior'],
            'experience_score': self.weights['experience'],
            'diversification_score': self.weights['diversification']
        }
        
        for component, weight in positive_components.items():
            if component in risk_features.columns:
                # Invert positive scores (1 - score) to make them risk indicators
                composite_score += (1 - risk_features[component].fillna(0)) * weight
        
        return pd.Series(np.clip(composite_score, 0, 1), index=risk_features.index)
    
    def _categorize_risk(self, scores: pd.Series) -> List[str]:
        """
        Categorize risk levels based on scores
        
        Args:
            scores (pd.Series): Credit scores (0-1000)
            
        Returns:
            List[str]: Risk categories
        """
        categories = []
        for score in scores:
            if score >= 800:
                categories.append('Low Risk')
            elif score >= 600:
                categories.append('Medium-Low Risk')
            elif score >= 400:
                categories.append('Medium Risk')
            elif score >= 200:
                categories.append('High Risk')
            else:
                categories.append('Very High Risk')
        
        return categories
    
    def get_score_distribution(self) -> Dict:
        """
        Get distribution statistics of calculated scores
        
        Returns:
            Dict: Score distribution information
        """
        if self.scores is None:
            raise ValueError("Scores must be calculated first")
        
        scores = self.scores['score']
        
        distribution = {
            'total_wallets': len(scores),
            'mean_score': float(scores.mean()),
            'median_score': float(scores.median()),
            'std_score': float(scores.std()),
            'min_score': int(scores.min()),
            'max_score': int(scores.max()),
            'score_ranges': {
                '0-100': len(scores[(scores >= 0) & (scores < 100)]),
                '100-200': len(scores[(scores >= 100) & (scores < 200)]),
                '200-300': len(scores[(scores >= 200) & (scores < 300)]),
                '300-400': len(scores[(scores >= 300) & (scores < 400)]),
                '400-500': len(scores[(scores >= 400) & (scores < 500)]),
                '500-600': len(scores[(scores >= 500) & (scores < 600)]),
                '600-700': len(scores[(scores >= 600) & (scores < 700)]),
                '700-800': len(scores[(scores >= 700) & (scores < 800)]),
                '800-900': len(scores[(scores >= 800) & (scores < 900)]),
                '900-1000': len(scores[(scores >= 900) & (scores <= 1000)])
            },
            'risk_categories': self.scores['risk_category'].value_counts().to_dict()
        }
        
        return distribution
    
    def explain_score(self, wallet_address: str) -> Dict:
        """
        Provide detailed explanation for a wallet's score
        
        Args:
            wallet_address (str): Wallet address
            
        Returns:
            Dict: Score explanation
        """
        if self.scores is None:
            raise ValueError("Scores must be calculated first")
        
        wallet_data = self.scores[self.scores['wallet_id'] == wallet_address.lower()]
        
        if len(wallet_data) == 0:
            raise ValueError(f"Wallet {wallet_address} not found")
        
        wallet_data = wallet_data.iloc[0]
        
        explanation = {
            'wallet_id': wallet_address,
            'score': int(wallet_data['score']),
            'risk_category': wallet_data['risk_category'],
            'risk_factors': {
                'liquidation_risk': float(wallet_data['liquidation_risk_component']),
                'behavioral_risk': float(wallet_data['behavioral_risk_component']),
                'financial_health': float(wallet_data['financial_health_component']),
                'repayment_behavior': float(wallet_data['repayment_behavior_component']),
                'experience_level': float(wallet_data['experience_component']),
                'anomaly_score': float(wallet_data['anomaly_score'])
            },
            'wallet_metrics': {
                'total_transactions': int(wallet_data['total_transactions']),
                'account_age_days': int(wallet_data['account_age_days']),
                'liquidation_count': int(wallet_data['liquidation_count']),
                'success_rate': float(wallet_data['success_rate'])
            }
        }
        
        # Score interpretation
        score = wallet_data['score']
        if score >= 800:
            explanation['interpretation'] = "Excellent risk profile with strong track record"
        elif score >= 600:
            explanation['interpretation'] = "Good risk profile with minor concerns"
        elif score >= 400:
            explanation['interpretation'] = "Moderate risk with careful monitoring needed"
        elif score >= 200:
            explanation['interpretation'] = "High risk requiring strict oversight"
        else:
            explanation['interpretation'] = "Very high risk, consider limiting exposure"
        
        return explanation
        return metrics