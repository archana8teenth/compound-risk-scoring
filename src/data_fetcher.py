import asyncio
import aiohttp
import requests
import pandas as pd
import numpy as np
from web3 import Web3
from datetime import datetime, timedelta
import logging
import time
from typing import List, Dict, Optional
import json
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompoundDataFetcher:
    """
    Fetch transaction data from Compound V2/V3 protocol
    """
    
    def __init__(self):
        # Initialize Web3 connection
        self.rpc_urls = [
            "https://mainnet.infura.io/v3/YOUR_INFURA_KEY",  # Replace with actual key
            "https://eth-mainnet.alchemyapi.io/v2/YOUR_ALCHEMY_KEY",  # Replace with actual key
            "https://eth-mainnet.public.blastapi.io",
            "https://ethereum.publicnode.com",
            "https://rpc.ankr.com/eth"
        ]
        
        self.w3 = None
        self._initialize_web3()
        
        # Compound V2 contract addresses
        self.compound_v2_contracts = {
            'comptroller': '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b',
            'ceth': '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5',
            'cdai': '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643',
            'cusdc': '0x39aa39c021dfbae8fac545936693ac917d5e7563',
            'cwbtc': '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4'
        }
        
        # Compound V3 contract addresses
        self.compound_v3_contracts = {
            'cusdc_v3': '0xc3d688b66703497daa19211eedff47f25384cae7',
            'ceth_v3': '0xa17581a9e3356d9a858b789d68b4d866e593ae94'
        }
        
        # API endpoints
        self.etherscan_api_key = os.getenv('ETHERSCAN_API_KEY', '')
        self.etherscan_base_url = "https://api.etherscan.io/api"
        
    def _initialize_web3(self):
        """Initialize Web3 connection with fallback RPCs"""
        for rpc_url in self.rpc_urls:
            try:
                self.w3 = Web3(Web3.HTTPProvider(rpc_url))
                if self.w3.is_connected():
                    logger.info(f"Connected to Ethereum via {rpc_url}")
                    break
            except Exception as e:
                logger.warning(f"Failed to connect to {rpc_url}: {str(e)}")
                continue
        
        if not self.w3 or not self.w3.is_connected():
            logger.warning("No RPC connection available, using API fallback")
    
    def fetch_etherscan_transactions(self, address: str, start_block: int = 0) -> List[Dict]:
        """
        Fetch transactions using Etherscan API
        
        Args:
            address (str): Wallet address
            start_block (int): Starting block number
            
        Returns:
            List[Dict]: Transaction data
        """
        transactions = []
        
        # Regular transactions
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': start_block,
            'endblock': 'latest',
            'page': 1,
            'offset': 1000,
            'sort': 'desc',
            'apikey': self.etherscan_api_key
        }
        
        try:
            response = requests.get(self.etherscan_base_url, params=params, timeout=30)
            data = response.json()
            
            if data['status'] == '1':
                transactions.extend(data['result'])
                logger.info(f"Fetched {len(data['result'])} regular transactions for {address}")
            
            # Internal transactions
            params['action'] = 'txlistinternal'
            response = requests.get(self.etherscan_base_url, params=params, timeout=30)
            data = response.json()
            
            if data['status'] == '1':
                internal_txs = data['result']
                for tx in internal_txs:
                    tx['type'] = 'internal'
                transactions.extend(internal_txs)
                logger.info(f"Fetched {len(internal_txs)} internal transactions for {address}")
            
            # ERC-20 token transfers
            params['action'] = 'tokentx'
            response = requests.get(self.etherscan_base_url, params=params, timeout=30)
            data = response.json()
            
            if data['status'] == '1':
                token_txs = data['result']
                for tx in token_txs:
                    tx['type'] = 'token'
                transactions.extend(token_txs)
                logger.info(f"Fetched {len(token_txs)} token transactions for {address}")
            
        except Exception as e:
            logger.error(f"Error fetching transactions for {address}: {str(e)}")
        
        # Add delay to respect rate limits
        time.sleep(0.2)
        
        return transactions
    
    def filter_compound_transactions(self, transactions: List[Dict], wallet_address: str) -> List[Dict]:
        """
        Filter transactions related to Compound protocol
        
        Args:
            transactions (List[Dict]): All transactions
            wallet_address (str): Wallet address
            
        Returns:
            List[Dict]: Compound-related transactions
        """
        compound_addresses = set(list(self.compound_v2_contracts.values()) + 
                               list(self.compound_v3_contracts.values()))
        
        compound_txs = []
        
        for tx in transactions:
            to_address = tx.get('to', '').lower()
            from_address = tx.get('from', '').lower()
            contract_address = tx.get('contractAddress', '').lower()
            
            # Check if transaction involves Compound contracts
            if (to_address in [addr.lower() for addr in compound_addresses] or
                from_address in [addr.lower() for addr in compound_addresses] or
                contract_address in [addr.lower() for addr in compound_addresses]):
                
                # Add transaction type based on method signature
                input_data = tx.get('input', '')
                tx['compound_action'] = self._classify_compound_action(input_data, tx)
                tx['wallet_address'] = wallet_address.lower()
                compound_txs.append(tx)
        
        logger.info(f"Found {len(compound_txs)} Compound transactions for {wallet_address}")
        return compound_txs
    
    def _classify_compound_action(self, input_data: str, tx: Dict) -> str:
        """
        Classify Compound transaction action based on method signature
        
        Args:
            input_data (str): Transaction input data
            tx (Dict): Transaction data
            
        Returns:
            str: Action type
        """
        if not input_data or input_data == '0x':
            return 'unknown'
        
        # Common Compound method signatures
        method_signatures = {
            '0xa0712d68': 'mint',  # mint()
            '0x1249c58b': 'mint',  # mint()
            '0x6c540baf': 'mint',  # mint(uint256)
            '0xdb006a75': 'redeem',  # redeem(uint256)
            '0x852a12e3': 'redeemUnderlying',  # redeemUnderlying(uint256)
            '0xc5ebeaec': 'borrow',  # borrow(uint256)
            '0x0e752702': 'repayBorrow',  # repayBorrow(uint256)
            '0x4e4d9fea': 'repayBorrow',  # repayBorrow()
            '0x2608f818': 'repayBorrowBehalf',  # repayBorrowBehalf(address,uint256)
            '0x47ef3b3b': 'liquidateBorrow',  # liquidateBorrow(address,uint256,address)
            '0x317b0b77': 'enterMarkets',  # enterMarkets(address[])
            '0xede4edd0': 'exitMarket',  # exitMarket(address)
        }
        
        method_sig = input_data[:10]
        action = method_signatures.get(method_sig, 'unknown')
        
        # Additional classification based on value and to/from addresses
        if action == 'unknown':
            value = int(tx.get('value', '0'))
            if value > 0:
                action = 'supply_eth'
            else:
                action = 'interact'
        
        return action
    
    async def fetch_wallet_data(self, wallet_address: str) -> Dict:
        """
        Fetch comprehensive data for a single wallet
        
        Args:
            wallet_address (str): Wallet address
            
        Returns:
            Dict: Wallet transaction data
        """
        logger.info(f"Fetching data for wallet: {wallet_address}")
        
        # Fetch all transactions
        all_transactions = self.fetch_etherscan_transactions(wallet_address)
        
        # Filter Compound transactions
        compound_transactions = self.filter_compound_transactions(all_transactions, wallet_address)
        
        # Additional wallet metrics
        wallet_data = {
            'address': wallet_address,
            'transactions': compound_transactions,
            'total_tx_count': len(all_transactions),
            'compound_tx_count': len(compound_transactions),
            'first_tx_timestamp': None,
            'last_tx_timestamp': None,
            'compound_first_tx': None,
            'compound_last_tx': None
        }
        
        # Calculate time metrics
        if all_transactions:
            timestamps = [int(tx.get('timeStamp', 0)) for tx in all_transactions if tx.get('timeStamp')]
            if timestamps:
                wallet_data['first_tx_timestamp'] = min(timestamps)
                wallet_data['last_tx_timestamp'] = max(timestamps)
        
        if compound_transactions:
            timestamps = [int(tx.get('timeStamp', 0)) for tx in compound_transactions if tx.get('timeStamp')]
            if timestamps:
                wallet_data['compound_first_tx'] = min(timestamps)
                wallet_data['compound_last_tx'] = max(timestamps)
        
        return wallet_data
    
    async def fetch_multiple_wallets(self, wallet_addresses: List[str]) -> List[Dict]:
        """
        Fetch data for multiple wallets with rate limiting
        
        Args:
            wallet_addresses (List[str]): List of wallet addresses
            
        Returns:
            List[Dict]: Wallet data for all addresses
        """
        wallet_data = []
        
        for i, address in enumerate(wallet_addresses):
            try:
                data = await self.fetch_wallet_data(address)
                wallet_data.append(data)
                
                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(wallet_addresses)} wallets")
                
                # Rate limiting
                await asyncio.sleep(0.5)  # 2 requests per second max
                
            except Exception as e:
                logger.error(f"Error processing wallet {address}: {str(e)}")
                continue
        
        return wallet_data
    
    def load_wallet_addresses(self, file_path: str) -> List[str]:
        """
        Load wallet addresses from CSV file
        
        Args:
            file_path (str): Path to CSV file
            
        Returns:
            List[str]: Wallet addresses
        """
        try:
            df = pd.read_csv(file_path)
            # Assuming the CSV has a column with wallet addresses
            address_columns = ['address', 'wallet', 'wallet_address', 'wallet_id']
            
            for col in address_columns:
                if col in df.columns:
                    addresses = df[col].dropna().astype(str).str.strip().tolist()
                    logger.info(f"Loaded {len(addresses)} wallet addresses from {col}")
                    return addresses
            
            # If no standard column found, use first column
            addresses = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
            logger.info(f"Loaded {len(addresses)} wallet addresses from first column")
            return addresses
            
        except Exception as e:
            logger.error(f"Error loading wallet addresses: {str(e)}")
            return []
    
    def save_raw_data(self, wallet_data: List[Dict], output_path: str):
        """
        Save raw transaction data to JSON
        
        Args:
            wallet_data (List[Dict]): Wallet transaction data
            output_path (str): Output file path
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(wallet_data, f, indent=2, default=str)
            logger.info(f"Raw data saved to {output_path}")
        except Exception as e:
            logger.error(f"Error saving raw data: {str(e)}")
