import asyncio
import os
import sys
import argparse
import json
import pandas as pd
import logging
from pathlib import Path
import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import CompoundDataFetcher
from compound_processor import CompoundDataProcessor
from risk_analyzer import WalletRiskAnalyzer
from score_calculator import RiskScoreCalculator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('risk_scoring.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def create_directories():
    """Create necessary directories"""
    directories = ['data', 'results']
    for dir_name in directories:
        Path(dir_name).mkdir(exist_ok=True)

def download_wallet_list():
    """Download wallet addresses from Google Sheets"""
    # Google Sheets CSV export URL
    sheet_url = "https://docs.google.com/spreadsheets/d/1ZzaeMgNYnxvriYYpe8PE7uMEblTI0GV5GIVUnsP-sBs/export?format=csv"
    
    try:
        response = requests.get(sheet_url)
        response.raise_for_status()
        
        with open('data/wallet_addresses.csv', 'wb') as f:
            f.write(response.content)
        
        logger.info("Downloaded wallet addresses from Google Sheets")
        return 'data/wallet_addresses.csv'
        
    except Exception as e:
        logger.error(f"Error downloading wallet list: {str(e)}")
        
        # Fallback sample addresses
        sample_addresses = [
            "0xfaa0768bde629806739c3a4620656c5d26f44ef2",
            "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503",
            "0x28c6c06298d514db089934071355e5743bf21d60",
            "0x2b6ed29a95753c3ad948348e3e7b1a251080ffb9",
            "0x6b175474e89094c44da98b954eedeac495271d0f"
        ]
        
        df = pd.DataFrame({'wallet_address': sample_addresses})
        df.to_csv('data/wallet_addresses.csv', index=False)
        logger.info("Created sample wallet addresses file")
        
        return 'data/wallet_addresses.csv'

async def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Compound Wallet Risk Scoring System')
    parser.add_argument('--wallet-file', '-w', type=str, help='CSV file with wallet addresses')
    parser.add_argument('--output', '-o', type=str, default='results/wallet_scores.csv', help='Output CSV file')
    parser.add_argument('--use-cache', action='store_true', help='Use cached data if available')
    parser.add_argument('--limit', type=int, help='Limit number of wallets to process')
    
    args = parser.parse_args()
    
    # Create directories
    create_directories()
    
    try:
        # Step 1: Get wallet addresses
        logger.info("=== Step 1: Loading Wallet Addresses ===")
        
        if args.wallet_file and os.path.exists(args.wallet_file):
            wallet_file = args.wallet_file
        else:
            wallet_file = download_wallet_list()
        
        # Load wallet addresses
        fetcher = CompoundDataFetcher()
        wallet_addresses = fetcher.load_wallet_addresses(wallet_file)
        
        if args.limit:
            wallet_addresses = wallet_addresses[:args.limit]
        
        print(f"Loaded {len(wallet_addresses)} wallet addresses")
        
        # Step 2: Fetch transaction data
        logger.info("=== Step 2: Fetching Transaction Data ===")
        
        cache_file = 'data/raw_wallet_data.json'
        
        if args.use_cache and os.path.exists(cache_file):
            logger.info("Loading cached transaction data")
            with open(cache_file, 'r') as f:
                wallet_data = json.load(f)
        else:
            logger.info("Fetching fresh transaction data (this may take a while...)")
            wallet_data = await fetcher.fetch_multiple_wallets(wallet_addresses)
            
            # Save raw data
            fetcher.save_raw_data(wallet_data, cache_file)
        
        print(f"Fetched data for {len(wallet_data)} wallets")
        
        # Step 3: Process transaction data
        logger.info("=== Step 3: Processing Transaction Data ===")
        
        processor = CompoundDataProcessor()
        transaction_df = processor.process_wallet_data(wallet_data)
        wallet_metrics = processor.calculate_wallet_metrics(transaction_df)
        
        print(f"Processed {len(transaction_df)} transactions")
        print(f"Generated metrics for {len(wallet_metrics)} wallets")
        
        # Step 4: Risk analysis
        logger.info("=== Step 4: Analyzing Risk Patterns ===")
        
        risk_analyzer = WalletRiskAnalyzer()
        risk_features = risk_analyzer.calculate_risk_features(wallet_metrics)
        anomaly_scores = risk_analyzer.detect_anomalies(risk_features)
        
        print(f"Calculated risk features for {len(risk_features)} wallets")
        
        # Step 5: Calculate final scores
        logger.info("=== Step 5: Calculating Risk Scores ===")
        
        score_calculator = RiskScoreCalculator()
        final_scores = score_calculator.calculate_scores(risk_features, anomaly_scores)
        
        # Step 6: Save results
        logger.info("=== Step 6: Saving Results ===")
        
        # Primary output format
        output_df = final_scores[['wallet_id', 'score']].copy()
        output_df.to_csv(args.output, index=False)
        
        # Detailed results
        detailed_output = args.output.replace('.csv', '_detailed.csv')
        final_scores.to_csv(detailed_output, index=False)
        
        logger.info(f"Results saved to {args.output}")
        logger.info(f"Detailed results saved to {detailed_output}")
        
        # Step 7: Analysis and summary
        logger.info("=== Step 7: Score Analysis ===")
        
        distribution = score_calculator.get_score_distribution()
        
        print(f"\n=== RISK SCORING RESULTS ===")
        print(f"Total wallets analyzed: {distribution['total_wallets']}")
        print(f"Mean score: {distribution['mean_score']:.1f}")
        print(f"Median score: {distribution['median_score']:.1f}")
        print(f"Score range: {distribution['min_score']} - {distribution['max_score']}")
        
        print(f"\nScore Distribution:")
        for range_name, count in distribution['score_ranges'].items():
            if count > 0:
                percentage = (count / distribution['total_wallets']) * 100
                print(f"  {range_name}: {count} wallets ({percentage:.1f}%)")
        
        print(f"\nRisk Categories:")
        for category, count in distribution['risk_categories'].items():
            percentage = (count / distribution['total_wallets']) * 100
            print(f"  {category}: {count} wallets ({percentage:.1f}%)")
        
        # Show top and bottom performers
        print(f"\nTop 5 Highest Scoring Wallets:")
        top_wallets = final_scores.nlargest(5, 'score')[['wallet_id', 'score', 'risk_category']]
        for _, row in top_wallets.iterrows():
            print(f"  {row['wallet_id']}: {row['score']} ({row['risk_category']})")
        
        print(f"\nTop 5 Lowest Scoring Wallets:")
        bottom_wallets = final_scores.nsmallest(5, 'score')[['wallet_id', 'score', 'risk_category']]
        for _, row in bottom_wallets.iterrows():
            print(f"  {row['wallet_id']}: {row['score']} ({row['risk_category']})")
        
        # Save analysis
        with open('results/score_analysis.json', 'w') as f:
            json.dump(distribution, f, indent=2, default=str)
        
        logger.info("Risk scoring analysis complete!")
        
        return final_scores, distribution
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
