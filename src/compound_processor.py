import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class CompoundDataProcessor:
    """
    Process raw Compound transaction data into structured format
    """
    
    def __init__(self):
        self.processed_data = None
        
    def process_wallet_data(self, wallet_data: List[Dict]) -> pd.DataFrame:
        """
        Process raw wallet data into structured DataFrame
        
        Args:
            wallet_data (List[Dict]): Raw wallet data
            
        Returns:
            pd.DataFrame: Processed transaction data
        """
        logger.info("Processing wallet transaction data")
        
        all_transactions = []
        
        for wallet in wallet_data:
            address = wallet['address']
            transactions = wallet.get('transactions', [])
            
            for tx in transactions:
                processed_tx = {
                    'wallet_address': address,
                    'tx_hash': tx.get('hash', ''),
                    'block_number': int(tx.get('blockNumber', 0)),
                    'timestamp': int(tx.get('timeStamp', 0)),
                    'from_address': tx.get('from', '').lower(),
                    'to_address': tx.get('to', '').lower(),
                    'value': float(tx.get('value', 0)) / 1e18,  # Convert from Wei
                    'gas_used': int(tx.get('gasUsed', 0)),
                    'gas_price': int(tx.get('gasPrice', 0)),
                    'tx_fee': (int(tx.get('gasUsed', 0)) * int(tx.get('gasPrice', 0))) / 1e18,
                    'compound_action': tx.get('compound_action', 'unknown'),
                    'is_error': int(tx.get('isError', 0)),
                    'tx_type': tx.get('type', 'regular')
                }
                
                # Handle token transactions
                if tx.get('type') == 'token':
                    token_decimal = int(tx.get('tokenDecimal', 18))
                    processed_tx['token_value'] = float(tx.get('value', 0)) / (10 ** token_decimal)
                    processed_tx['token_symbol'] = tx.get('tokenSymbol', '')
                    processed_tx['token_address'] = tx.get('contractAddress', '').lower()
                
                all_transactions.append(processed_tx)
        
        df = pd.DataFrame(all_transactions)
        
        if len(df) > 0:
            # Convert timestamp to datetime
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df['date'] = df['datetime'].dt.date
            df['hour'] = df['datetime'].dt.hour
            df['day_of_week'] = df['datetime'].dt.dayofweek
            
            # Sort by timestamp
            df = df.sort_values(['wallet_address', 'timestamp'])
            
            logger.info(f"Processed {len(df)} transactions for {df['wallet_address'].nunique()} wallets")
        
        self.processed_data = df
        return df
    
    def calculate_wallet_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate comprehensive metrics for each wallet
        
        Args:
            df (pd.DataFrame): Processed transaction data
            
        Returns:
            pd.DataFrame: Wallet metrics
        """
        logger.info("Calculating wallet metrics")
        
        if len(df) == 0:
            return pd.DataFrame()
        
        wallet_metrics = []
        
        for wallet_address, wallet_txs in df.groupby('wallet_address'):
            metrics = self._calculate_single_wallet_metrics(wallet_address, wallet_txs)
            wallet_metrics.append(metrics)
        
        return pd.DataFrame(wallet_metrics)
    
    def _calculate_single_wallet_metrics(self, address: str, txs: pd.DataFrame) -> Dict:
        """
        Calculate metrics for a single wallet
        
        Args:
            address (str): Wallet address
            txs (pd.DataFrame): Wallet transactions
            
        Returns:
            Dict: Wallet metrics
        """
        metrics = {'wallet_address': address}
        
        # Basic transaction metrics
        metrics['total_transactions'] = len(txs)
        metrics['successful_transactions'] = len(txs[txs['is_error'] == 0])
        metrics['failed_transactions'] = len(txs[txs['is_error'] == 1])
        metrics['success_rate'] = metrics['successful_transactions'] / max(metrics['total_transactions'], 1)
        
        # Time-based metrics
        if len(txs) > 0:
            metrics['first_tx_date'] = txs['timestamp'].min()
            metrics['last_tx_date'] = txs['timestamp'].max()
            metrics['account_age_days'] = (metrics['last_tx_date'] - metrics['first_tx_date']) / 86400
            metrics['avg_tx_interval_days'] = metrics['account_age_days'] / max(metrics['total_transactions'] - 1, 1)
        else:
            metrics['first_tx_date'] = 0
            metrics['last_tx_date'] = 0
            metrics['account_age_days'] = 0
            metrics['avg_tx_interval_days'] = 0
        
        # Action-based metrics
        action_counts = txs['compound_action'].value_counts()
        for action in ['mint', 'redeem', 'redeemUnderlying', 'borrow', 'repayBorrow', 'liquidateBorrow']:
            metrics[f'{action}_count'] = action_counts.get(action, 0)
        
        # Calculate action ratios
        total_actions = sum([metrics[f'{action}_count'] for action in ['mint', 'redeem', 'redeemUnderlying', 'borrow', 'repayBorrow']])
        
        if total_actions > 0:
            metrics['supply_ratio'] = (metrics['mint_count']) / total_actions
            metrics['withdraw_ratio'] = (metrics['redeem_count'] + metrics['redeemUnderlying_count']) / total_actions
            metrics['borrow_ratio'] = metrics['borrow_count'] / total_actions
            metrics['repay_ratio'] = metrics['repayBorrow_count'] / total_actions
        else:
            metrics['supply_ratio'] = 0
            metrics['withdraw_ratio'] = 0
            metrics['borrow_ratio'] = 0
            metrics['repay_ratio'] = 0
        
        # Liquidation risk
        metrics['liquidation_count'] = metrics['liquidateBorrow_count']
        metrics['has_liquidations'] = 1 if metrics['liquidation_count'] > 0 else 0
        metrics['liquidation_rate'] = metrics['liquidation_count'] / max(metrics['total_transactions'], 1)
        
        # Financial metrics
        successful_txs = txs[txs['is_error'] == 0]
        if len(successful_txs) > 0:
            metrics['total_gas_spent'] = successful_txs['tx_fee'].sum()
            metrics['avg_gas_per_tx'] = successful_txs['tx_fee'].mean()
            metrics['total_eth_value'] = successful_txs['value'].sum()
            metrics['avg_eth_per_tx'] = successful_txs['value'].mean()
        else:
            metrics['total_gas_spent'] = 0
            metrics['avg_gas_per_tx'] = 0
            metrics['total_eth_value'] = 0
            metrics['avg_eth_per_tx'] = 0
        
        # Behavioral patterns
        if len(txs) > 1:
            # Time between transactions (in hours)
            time_diffs = txs['timestamp'].diff().dropna() / 3600
            metrics['avg_time_between_txs'] = time_diffs.mean()
            metrics['std_time_between_txs'] = time_diffs.std()
            
            # Activity regularity (lower CV = more regular)
            if time_diffs.mean() > 0:
                metrics['activity_regularity'] = time_diffs.std() / time_diffs.mean()
            else:
                metrics['activity_regularity'] = 0
        else:
            metrics['avg_time_between_txs'] = 0
            metrics['std_time_between_txs'] = 0
            metrics['activity_regularity'] = 0
        
        # Diversification metrics
        unique_actions = txs['compound_action'].nunique()
        metrics['action_diversity'] = unique_actions
        
        # Weekend/night activity (potential bot indicators)
        if 'day_of_week' in txs.columns:
            weekend_txs = txs[txs['day_of_week'].isin([5, 6])]
            metrics['weekend_activity_ratio'] = len(weekend_txs) / max(len(txs), 1)
        else:
            metrics['weekend_activity_ratio'] = 0
        
        if 'hour' in txs.columns:
            night_txs = txs[(txs['hour'] >= 0) & (txs['hour'] <= 6)]
            metrics['night_activity_ratio'] = len(night_txs) / max(len(txs), 1)
        else:
            metrics['night_activity_ratio'] = 0
        
        # Risk indicators
        metrics['repay_to_borrow_ratio'] = metrics['repayBorrow_count'] / max(metrics['borrow_count'], 1)
        
        # High-frequency activity (potential bot behavior)
        if len(txs) > 0:
            daily_tx_counts = txs.groupby('date').size()
            metrics['max_daily_transactions'] = daily_tx_counts.max()
            metrics['avg_daily_transactions'] = daily_tx_counts.mean()
            
            if len(daily_tx_counts) > 1:
                metrics['daily_activity_variance'] = daily_tx_counts.var()
            else:
                metrics['daily_activity_variance'] = 0
        else:
            metrics['max_daily_transactions'] = 0
            metrics['avg_daily_transactions'] = 0
            metrics['daily_activity_variance'] = 0
        
        return metrics
