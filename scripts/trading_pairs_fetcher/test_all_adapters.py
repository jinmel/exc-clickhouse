#!/usr/bin/env python3
"""
Comprehensive test script for all exchange adapters.

Tests each adapter individually and then tests the factory for fetching from all exchanges.
"""

import asyncio
import logging
import sys
from typing import List, Dict

# Use absolute imports to avoid relative import issues
try:
    from .models import ExchangeResponse, TradingPair
    from .adapter_factory import AdapterFactory, fetch_all_exchanges
    from .yaml_exporter import YAMLExporter
except ImportError:
    from models import ExchangeResponse, TradingPair
    from adapter_factory import AdapterFactory, fetch_all_exchanges
    from yaml_exporter import YAMLExporter


class AdapterTester:
    """Test runner for all exchange adapters."""
    
    def __init__(self):
        """Initialize the tester."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.factory = AdapterFactory()
        self.results = {}
    
    async def test_single_adapter(self, exchange_name: str) -> Dict[str, any]:
        """
        Test a single exchange adapter.
        
        Args:
            exchange_name: Name of the exchange to test
            
        Returns:
            Dictionary with test results
        """
        self.logger.info(f"Testing {exchange_name} adapter...")
        
        try:
            response = await self.factory.fetch_from_exchange(exchange_name)
            
            result = {
                'exchange': exchange_name,
                'success': response.success,
                'pairs_count': len(response.trading_pairs),
                'error': response.error_message if not response.success else None,
                'sample_pairs': [],
                'unique_base_assets': set(),
                'unique_quote_assets': set(),
            }
            
            if response.success and response.trading_pairs:
                # Get sample pairs (first 5)
                sample_pairs = response.trading_pairs[:5]
                result['sample_pairs'] = [
                    {
                        'pair': p.pair,
                        'base': p.base_asset,
                        'quote': p.quote_asset
                    }
                    for p in sample_pairs
                ]
                
                # Collect unique assets
                for pair in response.trading_pairs:
                    result['unique_base_assets'].add(pair.base_asset)
                    result['unique_quote_assets'].add(pair.quote_asset)
                
                # Convert sets to lists for JSON serialization
                result['unique_base_assets'] = sorted(list(result['unique_base_assets']))
                result['unique_quote_assets'] = sorted(list(result['unique_quote_assets']))
                
                self.logger.info(f"✅ {exchange_name}: {result['pairs_count']} pairs")
                
            else:
                self.logger.error(f"❌ {exchange_name}: {result['error']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_name}: Exception - {e}")
            return {
                'exchange': exchange_name,
                'success': False,
                'pairs_count': 0,
                'error': str(e),
                'sample_pairs': [],
                'unique_base_assets': [],
                'unique_quote_assets': [],
            }
    
    async def test_all_adapters(self) -> Dict[str, any]:
        """
        Test all available exchange adapters.
        
        Returns:
            Dictionary with comprehensive test results
        """
        self.logger.info("Starting comprehensive adapter tests...")
        
        exchanges = self.factory.get_available_exchanges()
        
        # Test each adapter individually
        individual_results = []
        for exchange in exchanges:
            result = await self.test_single_adapter(exchange)
            individual_results.append(result)
            self.results[exchange] = result
        
        # Test factory method for fetching all
        self.logger.info("Testing factory fetch_from_all_exchanges...")
        try:
            responses = await self.factory.fetch_from_all_exchanges(max_concurrent=3)
            factory_stats = self.factory.get_summary_stats(responses)
            factory_success = True
        except Exception as e:
            self.logger.error(f"Factory test failed: {e}")
            factory_stats = {}
            factory_success = False
        
        # Compile overall results
        successful_exchanges = [r for r in individual_results if r['success']]
        failed_exchanges = [r for r in individual_results if not r['success']]
        
        total_pairs = sum(r['pairs_count'] for r in successful_exchanges)
        
        # Collect all unique assets across exchanges
        all_base_assets = set()
        all_quote_assets = set()
        for result in successful_exchanges:
            all_base_assets.update(result['unique_base_assets'])
            all_quote_assets.update(result['unique_quote_assets'])
        
        overall_results = {
            'test_timestamp': asyncio.get_event_loop().time(),
            'total_exchanges_tested': len(exchanges),
            'successful_exchanges': len(successful_exchanges),
            'failed_exchanges': len(failed_exchanges),
            'total_pairs_fetched': total_pairs,
            'factory_test_success': factory_success,
            'factory_stats': factory_stats,
            'individual_results': individual_results,
            'summary': {
                'successful_exchange_names': [r['exchange'] for r in successful_exchanges],
                'failed_exchange_names': [r['exchange'] for r in failed_exchanges],
                'all_unique_base_assets': sorted(list(all_base_assets)),
                'all_unique_quote_assets': sorted(list(all_quote_assets)),
                'exchange_pair_counts': {r['exchange']: r['pairs_count'] for r in individual_results}
            }
        }
        
        return overall_results
    
    def print_results(self, results: Dict[str, any]):
        """
        Print formatted test results.
        
        Args:
            results: Test results dictionary
        """
        print("\n" + "="*60)
        print("EXCHANGE ADAPTER TEST RESULTS")
        print("="*60)
        
        print(f"Total Exchanges Tested: {results['total_exchanges_tested']}")
        print(f"Successful: {results['successful_exchanges']}")
        print(f"Failed: {results['failed_exchanges']}")
        print(f"Total Pairs Fetched: {results['total_pairs_fetched']}")
        print(f"Factory Test: {'✅ PASSED' if results['factory_test_success'] else '❌ FAILED'}")
        
        print("\nExchange Breakdown:")
        for result in results['individual_results']:
            status = "✅" if result['success'] else "❌"
            pairs_info = f"{result['pairs_count']} pairs" if result['success'] else result['error']
            print(f"  {status} {result['exchange']}: {pairs_info}")
        
        if results['successful_exchanges'] > 0:
            print(f"\nUnique Base Assets ({len(results['summary']['all_unique_base_assets'])}):")
            base_assets = results['summary']['all_unique_base_assets'][:20]  # Show first 20
            if len(results['summary']['all_unique_base_assets']) > 20:
                print(f"  {', '.join(base_assets)}... and {len(results['summary']['all_unique_base_assets']) - 20} more")
            else:
                print(f"  {', '.join(base_assets)}")
            
            print(f"\nUnique Quote Assets ({len(results['summary']['all_unique_quote_assets'])}):")
            quote_assets = results['summary']['all_unique_quote_assets']
            print(f"  {', '.join(quote_assets)}")
        
        if results['failed_exchanges'] > 0:
            print(f"\nFailed Exchanges ({results['failed_exchanges']}):")
            for result in results['individual_results']:
                if not result['success']:
                    print(f"  ❌ {result['exchange']}: {result['error']}")
        
        print("\n" + "="*60)
    
    async def test_yaml_export(self, results: Dict[str, any]) -> bool:
        """
        Test YAML export functionality with fetched data.
        
        Args:
            results: Test results containing trading pairs
            
        Returns:
            True if YAML export successful
        """
        try:
            # Collect all trading pairs from successful exchanges
            all_pairs = []
            for result in results['individual_results']:
                if result['success'] and result['sample_pairs']:
                    # Convert sample pairs back to TradingPair objects
                    for sample in result['sample_pairs']:
                        pair = TradingPair(
                            exchange=result['exchange'],
                            trading_type="SPOT",
                            pair=sample['pair'],
                            base_asset=sample['base'],
                            quote_asset=sample['quote']
                        )
                        all_pairs.append(pair)
            
            if not all_pairs:
                self.logger.warning("No trading pairs available for YAML export test")
                return False
            
            # Test YAML export
            exporter = YAMLExporter()
            trading_pairs_list = exporter.create_trading_pairs_list(all_pairs)
            yaml_content = exporter.to_yaml_string(trading_pairs_list)
            
            # Validate the YAML content
            if exporter.validate_yaml_format(yaml_content):
                self.logger.info(f"✅ YAML export test passed: {len(yaml_content)} bytes")
                return True
            else:
                self.logger.error("❌ YAML export test failed: Invalid format")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ YAML export test failed: {e}")
            return False


async def main():
    """Main test function."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create tester and run tests
    tester = AdapterTester()
    
    try:
        # Run all adapter tests
        results = await tester.test_all_adapters()
        
        # Print results
        tester.print_results(results)
        
        # Test YAML export
        print("\nTesting YAML Export...")
        yaml_success = await tester.test_yaml_export(results)
        print(f"YAML Export: {'✅ PASSED' if yaml_success else '❌ FAILED'}")
        
        # Final summary
        print(f"\nOverall Test Result: ", end="")
        if results['successful_exchanges'] > 0 and yaml_success:
            print("✅ TESTS PASSED")
            return 0
        else:
            print("❌ SOME TESTS FAILED")
            return 1
            
    except Exception as e:
        logging.error(f"Test runner failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 