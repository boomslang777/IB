from ib_insync import *
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from IPython.display import display, clear_output
import time
import signal
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Reduce IB-related logging noise
logging.getLogger('ib_insync.wrapper').setLevel(logging.WARNING)
logging.getLogger('ib_insync.client').setLevel(logging.WARNING)
logging.getLogger('ib_insync.ib').setLevel(logging.WARNING)

util.startLoop()

host = '127.0.0.1'
port = 7497

class IBConnection:
    def __init__(self, host=host, port=port, client_id=random.randint(1, 1000), max_attempts=3, retry_wait=5):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.max_attempts = max_attempts
        self.retry_wait = retry_wait
        self.ib = IB()

    def connect(self):
        for attempt in range(self.max_attempts):
            try:
                if not self.ib.isConnected():
                    self.ib.connect(self.host, self.port, clientId=self.client_id)
                logger.info("Successfully connected to IB")
                print("Successfully connected to IB")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to IB: {str(e)}")
                print(f"Attempt {attempt + 1} failed to connect to IB: {str(e)}")
                if attempt < self.max_attempts - 1:
                    print(f"Retrying in {self.retry_wait} seconds...")
                    time.sleep(self.retry_wait)
        logger.error("Failed to connect to IB after maximum attempts")
        print("Failed to connect to IB after maximum attempts")
        return False

    def ensure_connection(self):
        if not self.ib.isConnected():
            logger.warning("Connection lost. Attempting to reconnect...")
            return self.connect()
        return True

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            print("Disconnected from IB")

    def create_contract(self, instrument_type, symbol, expiry=None, strike=None, right=None):
        if not self.ensure_connection():
            raise ConnectionError("Failed to connect to IB")
        
        try:
            if instrument_type == "stock":
                contract = Stock(symbol, 'SMART', 'USD')
            elif instrument_type == "option":
                if not all([expiry, strike, right]):
                    raise ValueError("Expiry, strike, and right are required for option contracts")
                contract = Option(symbol, expiry, strike, right, 'SMART')
            elif instrument_type == "index":
                # Special handling for indices
                if symbol == "SPX":
                    contract = Index(symbol, 'CBOE', 'USD')
                else:
                    contract = Index(symbol, 'SMART', 'USD')
            else:
                raise ValueError("Invalid instrument type")
            
            # Qualify the contract
            qualified_contracts = self.ib.qualifyContracts(contract)
            if not qualified_contracts:
                raise ValueError(f"Could not qualify contract for {symbol}")
            
            logger.info(f"Successfully qualified contract: {qualified_contracts[0]}")
            return qualified_contracts[0]
            
        except Exception as e:
            logger.error(f"Error creating contract for {symbol}: {str(e)}")
            raise
class MarketData:
    def __init__(self, ib_connection):
        self.ib_connection = ib_connection

    def _adjust_duration(self, duration):
        """Helper method to adjust duration string to ensure complete data coverage"""
        try:
            value = int(duration.split()[0])
            unit = duration.split()[1]
            
            # Add extra time to ensure coverage
            if unit == 'S':
                value += 60  # Add 60 seconds
            elif unit == 'D':
                value += 1   # Add 1 day
            elif unit == 'W':
                value += 1   # Add 1 week
            elif unit == 'M':
                value += 1   # Add 1 month
            elif unit == 'Y':
                value += 1   # Add 1 year
                
            return f"{value} {unit}"
        except:
            return duration  # Return original duration if parsing fails

    def get_market_data(self, instrument_type, symbol, data_type, timeframe=None, duration=None, expiry=None, strike=None, right=None):
        if not self.ib_connection.ensure_connection():
            raise ConnectionError("Failed to connect to IB")

        try:
            self.ib_connection.ib.reqMarketDataType(4)  # Delayed-Frozen data
            contract = self.ib_connection.create_contract(instrument_type, symbol, expiry, strike, right)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            instrument_suffix = "STK" if instrument_type == "stock" else "OPT"
            
            if data_type == "OHLC":
                # Handle 1-second data with 2000S chunks
                if timeframe == "1 secs":
                    # Convert requested duration to seconds
                    duration_val = int(duration.split()[0])
                    duration_unit = duration.split()[1]
                    total_seconds = duration_val * {
                        'S': 1, 
                        'D': 86400, 
                        'W': 604800, 
                        'M': 2592000,
                        'Y': 31536000
                    }[duration_unit]

                    all_bars = []
                    end_datetime = datetime.now()
                    chunk_size = 2000  # Maximum seconds per request
                    remaining_seconds = total_seconds

                    logger.info(f"Starting data collection for {symbol}. Total duration: {total_seconds} seconds")

                    while remaining_seconds > 0:
                        current_chunk = min(chunk_size, remaining_seconds)
                        chunk_duration = f"{current_chunk} S"
                        
                        logger.info(f"Requesting chunk of {current_chunk} seconds, ending at {end_datetime}")
                        
                        chunk_bars = self.ib_connection.ib.reqHistoricalData(
                            contract,
                            endDateTime=end_datetime,
                            durationStr=chunk_duration,
                            barSizeSetting=timeframe,
                            whatToShow='TRADES',
                            useRTH=True
                        )
                        
                        if chunk_bars:
                            all_bars.extend(chunk_bars)
                            # Update end_datetime for next request
                            end_datetime = chunk_bars[0].date
                            logger.info(f"Received {len(chunk_bars)} bars. Next request will end at {end_datetime}")
                        else:
                            logger.warning(f"No data received for chunk ending at {end_datetime}")
                            # If no data received, move end time back by chunk size to avoid getting stuck
                            end_datetime = end_datetime - pd.Timedelta(seconds=current_chunk)
                        
                        remaining_seconds -= current_chunk
                        logger.info(f"Remaining seconds: {remaining_seconds}")
                        
                        # Add delay between requests to avoid rate limiting
                        self.ib_connection.ib.sleep(1)

                    if not all_bars:
                        logger.error(f"No data returned for {symbol}")
                        return None
                    
                    bars = all_bars
                    logger.info(f"Successfully collected {len(bars)} bars in total")
                else:
                    # Original code for other timeframes
                    bars = self.ib_connection.ib.reqHistoricalData(
                        contract,
                        endDateTime='',
                        durationStr=duration,
                        barSizeSetting=timeframe,
                        whatToShow='TRADES',
                        useRTH=True
                    )

                if not bars:
                    logger.error(f"No data returned for {symbol}")
                    return None
                df = util.df(bars)
                
                # Add additional data for options
                if instrument_type == "option":
                    underlying_contract = Stock(symbol, 'SMART', 'USD')
                    self.ib_connection.ib.qualifyContracts(underlying_contract)
                    
                    # Get underlying data with slightly longer duration to ensure coverage
                    underlying_duration = self._adjust_duration(duration)
                    
                    underlying_bars = self.ib_connection.ib.reqHistoricalData(
                        underlying_contract,
                        endDateTime='',
                        durationStr=underlying_duration,
                        barSizeSetting=timeframe,
                        whatToShow='TRADES',
                        useRTH=True
                    )
                    
                    if underlying_bars:
                        underlying_df = util.df(underlying_bars)
                        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                        underlying_df['date'] = pd.to_datetime(underlying_df['date']).dt.tz_localize(None)
                        
                        # Forward fill the underlying price
                        df['underlyingPrice'] = np.nan
                        
                        # Create a mapping of all underlying prices
                        underlying_prices = underlying_df.set_index('date')['close']
                        
                        # Merge and forward fill
                        for idx, row in df.iterrows():
                            current_date = row['date']
                            # Try to get exact match first
                            matching_price = underlying_prices.get(current_date)
                            
                            if pd.isna(matching_price):
                                # If no exact match, get the last known price before this time
                                last_price = underlying_prices[underlying_prices.index <= current_date]
                                if not last_price.empty:
                                    matching_price = last_price.iloc[-1]
                            
                            df.at[idx, 'underlyingPrice'] = matching_price
                        
                        # Forward fill any remaining NaN values
                        df['underlyingPrice'] = df['underlyingPrice'].ffill()
                        
                        logger.info(f"Successfully added underlying prices for {symbol}")
                    else:
                        logger.warning(f"No underlying data available for {symbol}")
                    
                    # Get Greeks
                    ticker = self.ib_connection.ib.reqMktData(contract)
                    self.ib_connection.ib.sleep(2)
                    
                    if ticker.modelGreeks:
                        df['impliedVol'] = ticker.modelGreeks.impliedVol
                        df['delta'] = ticker.modelGreeks.delta
                        df['gamma'] = ticker.modelGreeks.gamma
                        df['vega'] = ticker.modelGreeks.vega
                        df['theta'] = ticker.modelGreeks.theta
                    
                    self.ib_connection.ib.cancelMktData(contract)

                filename = f"{symbol}_OHLC_{instrument_suffix}_{timestamp}.csv"
                df.to_csv(filename)
                logger.info(f"OHLC data saved to {filename}")
                return df

            elif data_type == "historical":
                all_ticks = []
                end = datetime.now(timezone.utc)
                
                # Parse duration
                duration_parts = duration.split()
                duration_val = int(duration_parts[0])
                duration_unit = duration_parts[1]
                
                if duration_unit == 'D':
                    start_time = end - pd.Timedelta(days=duration_val)
                elif duration_unit == 'W':
                    start_time = end - pd.Timedelta(weeks=duration_val)
                elif duration_unit == 'M':
                    start_time = end - pd.DateOffset(months=duration_val)
                else:
                    raise ValueError("Please use duration format like '1 D', '1 W', or '1 M'")
                
                current_end = end
                total_ticks = 0
                
                logger.info(f"Starting historical data collection for {symbol}")
                
                while current_end > start_time:
                    try:
                        self.ib_connection.ib.sleep(2)
                        
                        ticks = self.ib_connection.ib.reqHistoricalTicks(
                            contract,
                            '',
                            current_end,
                            1000,
                            'TRADES',
                            useRth=True
                        )
                        
                        if not ticks:
                            if total_ticks == 0:
                                break
                            current_end = current_end - pd.Timedelta(hours=1)
                            continue
                        
                        current_end = ticks[0].time.replace(tzinfo=timezone.utc) if ticks[0].time.tzinfo is None else ticks[0].time
                        total_ticks += len(ticks)
                        all_ticks = ticks + all_ticks
                        
                        logger.info(f"Retrieved {len(ticks)} ticks. Total: {total_ticks}")
                        
                    except Exception as e:
                        logger.error(f"Error retrieving historical ticks: {str(e)}")
                        self.ib_connection.ib.sleep(5)
                        continue
                    
                    if current_end <= start_time:
                        break
                
                if not all_ticks:
                    logger.error(f"No historical tick data returned for {symbol}")
                    return None
                
                df = util.df(all_ticks)
                
                # Add last close price
                last_close = self.ib_connection.ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr='1 D',
                    barSizeSetting='1 day',
                    whatToShow='TRADES',
                    useRTH=True
                )
                if last_close:
                    df['lastClose'] = last_close[-1].close
                
                df['cumulativeVolume'] = df['size'].cumsum()
                df = df[df['time'] >= start_time]
                
                filename = f"{symbol}_TICK_{instrument_suffix}_{timestamp}.csv"
                df.to_csv(filename)
                logger.info(f"Historical tick data saved to {filename}")
                return df 
        except Exception as e:
            logger.error(f"Error in get_market_data: {str(e)}")
            raise
class LiveStreaming:
    def __init__(self, ib_connection):
        self.ib_connection = ib_connection
        self.raw_df = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.real_time_bars = {}
        self.stop_streaming = False

    def initialize_dataframes(self, contracts):
        columns = [
            'bidSize', 'bid', 'ask', 'askSize', 'last', 'lastSize',
            'high', 'low', 'volume', 'close',
            'impliedVol', 'delta', 'gamma', 'vega', 'theta',
            'rtb_open', 'rtb_high', 'rtb_low', 'rtb_close',
            'rtb_volume', 'rtb_wap', 'rtb_count'
        ]
        
        index = [contract.symbol for contract in contracts]
        self.raw_df = pd.DataFrame(0, index=index, columns=columns)
        self.filtered_df = self.raw_df.copy()

    def stream_live_data(self, contracts):
        if not isinstance(contracts, list):
            contracts = [contracts]

        if not self.ib_connection.ensure_connection():
            raise ConnectionError("Failed to connect to IB")

        try:
            self.initialize_dataframes(contracts)
            self.stop_streaming = False

            def onPendingTickers(tickers):
                for t in tickers:
                    if t.contract.symbol in self.raw_df.index:
                        # Get real-time bar data
                        rtb_data = self.real_time_bars.get(t.contract.symbol, [0] * 7)
                        
                        # Create new data array with market data
                        new_data = {
                            'bidSize': t.bidSize or 0,
                            'bid': t.bid or 0,
                            'ask': t.ask or 0,
                            'askSize': t.askSize or 0,
                            'last': t.last or 0,
                            'lastSize': t.lastSize or 0,
                            'high': t.high or 0,
                            'low': t.low or 0,
                            'volume': t.volume or 0,
                            'close': t.close or 0,
                            'impliedVol': t.modelGreeks.impliedVol if t.modelGreeks else 0,
                            'delta': t.modelGreeks.delta if t.modelGreeks else 0,
                            'gamma': t.modelGreeks.gamma if t.modelGreeks else 0,
                            'vega': t.modelGreeks.vega if t.modelGreeks else 0,
                            'theta': t.modelGreeks.theta if t.modelGreeks else 0,
                            'rtb_open': rtb_data[0],
                            'rtb_high': rtb_data[1],
                            'rtb_low': rtb_data[2],
                            'rtb_close': rtb_data[3],
                            'rtb_volume': rtb_data[4],
                            'rtb_wap': rtb_data[5],
                            'rtb_count': rtb_data[6]
                        }
                        
                        # Update raw_df
                        self.raw_df.loc[t.contract.symbol] = pd.Series(new_data)
                        
                        # Update filtered_df with non-zero values
                        for col, value in new_data.items():
                            if value != 0:
                                self.filtered_df.at[t.contract.symbol, col] = value
                            elif self.filtered_df.at[t.contract.symbol, col] == 0:
                                last_non_zero = self.filtered_df.at[t.contract.symbol, col]
                                if last_non_zero != 0:
                                    self.filtered_df.at[t.contract.symbol, col] = last_non_zero

                if not self.stop_streaming:
                    clear_output(wait=True)
                    display(self.filtered_df)
                    print("\nPress Ctrl+C to stop streaming.")

            def onBarUpdate(bars, hasNewBar):
                if hasNewBar:
                    symbol = bars.contract.symbol
                    latest_bar = bars[-1]
                    self.real_time_bars[symbol] = [
                        latest_bar.open_,
                        latest_bar.high,
                        latest_bar.low,
                        latest_bar.close,
                        latest_bar.volume,
                        latest_bar.wap,
                        latest_bar.count
                    ]

            # Request market data and real-time bars for each contract
            tickers = []
            bars = []
            for contract in contracts:
                ticker = self.ib_connection.ib.reqMktData(contract)
                tickers.append(ticker)
                
                bar = self.ib_connection.ib.reqRealTimeBars(contract, 5, 'TRADES', False)
                bar.updateEvent += onBarUpdate
                bars.append(bar)

            self.ib_connection.ib.pendingTickersEvent += onPendingTickers

            # Generate timestamp for the data file
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

            try:
                while not self.stop_streaming:
                    self.ib_connection.ib.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping live streaming...")
            finally:
                # Clean up
                self.stop_streaming = True
                self.ib_connection.ib.pendingTickersEvent -= onPendingTickers
                
                for ticker in tickers:
                    self.ib_connection.ib.cancelMktData(ticker.contract)
                
                for bar in bars:
                    self.ib_connection.ib.cancelRealTimeBars(bar)
                    bar.updateEvent.clear()

                # Save final data
                filename = f"live_data_{timestamp}.parquet"
                self.filtered_df.to_parquet(filename)
                logger.info(f"Live data saved to {filename}")
                print(f"Live data saved to {filename}")
                print("Final dataframe:")
                display(self.filtered_df)

        except Exception as e:
            logger.error(f"Error in live streaming: {str(e)}")
            raise  
class DataManager:
    def __init__(self, host=host, port=port):
        self.ib_connection = IBConnection(host, port)
        self.market_data = MarketData(self.ib_connection)
        self.live_streaming = LiveStreaming(self.ib_connection)

    def connect(self):
        return self.ib_connection.connect()

    def disconnect(self):
        self.ib_connection.disconnect()

    
    
    def get_data(self, instrument_type, symbol, data_type, timeframe=None, duration=None, expiry=None, strike=None, right=None):
        """
        Main method to get different types of market data
        
        Parameters:
        - instrument_type: "stock", "option", or "index"
        - symbol: ticker symbol
        - data_type: "OHLC", "historical", or "realtime"
        - timeframe: for OHLC data (e.g., "1 min", "1 hour", "1 day")
        - duration: lookback period (e.g., "1 D", "1 W", "1 M")
        - expiry: option expiry date (YYYYMMDD format)
        - strike: option strike price
        - right: option right ("C" for call, "P" for put)
        """
        try:
            return self.market_data.get_market_data(
                instrument_type, symbol, data_type, 
                timeframe, duration, expiry, strike, right
            )
        except Exception as e:
            logger.error(f"Error getting {data_type} data for {symbol}: {str(e)}")
            raise

    def stream_live_data(self, symbols, instrument_type="stock"):
        """
        Stream live market data for multiple symbols
        
        Parameters:
        - symbols: list of ticker symbols or single symbol
        - instrument_type: "stock", "option", or "index"
        """
        try:
            if isinstance(symbols, str):
                symbols = [symbols]

            contracts = []
            for symbol in symbols:
                contract = self.ib_connection.create_contract(instrument_type, symbol)
                contracts.append(contract)

            self.live_streaming.stream_live_data(contracts)
        except Exception as e:
            logger.error(f"Error streaming live data: {str(e)}")
            raise

def main():
    # Example usage
    data_manager = DataManager()
    
    try:
        if data_manager.connect():
            while True:
                print("\nMarket Data Options:")
                print("1. Get OHLC Data")
                print("2. Get Historical Tick Data")
                print("3. Stream Live Data (Single Symbol)")
                print("4. Stream Live Data (Multiple Symbols)")
                print("5. Exit")
                
                choice = input("\nEnter your choice (1-5): ")
                
                if choice == "1":
                    symbol = input("Enter symbol: ")
                    timeframe = input("Enter timeframe (e.g., '1 min', '1 hour', '1 day'): ")
                    duration = input("Enter duration (e.g., '1 D', '1 W', '1 M'): ")
                    df = data_manager.get_data("stock", symbol, "OHLC", timeframe, duration)
                    if df is not None:
                        print("\nData retrieved successfully:")
                        display(df)
                
                elif choice == "2":
                    symbol = input("Enter symbol: ")
                    duration = input("Enter duration (e.g., '1 D', '1 W', '1 M'): ")
                    df = data_manager.get_data("stock", symbol, "historical", duration=duration)
                    if df is not None:
                        print("\nData retrieved successfully:")
                        display(df)
                
                elif choice == "3":
                    symbol = input("Enter symbol: ")
                    data_manager.stream_live_data(symbol)
                
                elif choice == "4":
                    symbols = input("Enter symbols (comma-separated): ").split(',')
                    symbols = [s.strip() for s in symbols]
                    data_manager.stream_live_data(symbols)
                
                elif choice == "5":
                    break
                
                else:
                    print("Invalid choice. Please try again.")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
    finally:
        data_manager.disconnect()

if __name__ == "__main__":
    main()                         

