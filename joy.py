from ib_insync import *
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from IPython.display import display, clear_output
import time
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logging.getLogger('ib_insync.wrapper').setLevel(logging.WARNING)
logging.getLogger('ib_insync.client').setLevel(logging.WARNING)
logging.getLogger('ib_insync.ib').setLevel(logging.WARNING)

util.startLoop()

class IBConnection:
    def __init__(self, host='127.0.0.1', port=7497, client_id=1, max_attempts=3, retry_wait=5):
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
        self.ib.disconnect()
        print("Disconnected from IB")

    def create_contract(self, instrument_type, symbol, expiry=None, strike=None, right=None):
        if not self.ensure_connection():
            raise ConnectionError("Failed to connect to IB")
        
        if instrument_type == "stock":
            contract = Stock(symbol, 'SMART', 'USD')
        elif instrument_type == "option":
            if not all([expiry, strike, right]):
                raise ValueError("Expiry, strike, and right are required for option contracts")
            contract = Option(symbol, expiry, strike, right, 'SMART')
        else:
            raise ValueError("Invalid instrument type")
        
        self.ib.qualifyContracts(contract)
        return contract

class MarketData:
    def __init__(self, ib_connection):
        self.ib_connection = ib_connection

    def get_market_data(self, instrument_type, symbol, data_type, timeframe=None, duration=None, expiry=None, strike=None, right=None):
        if not self.ib_connection.ensure_connection():
            raise ConnectionError("Failed to connect to IB")

        try:
            self.ib_connection.ib.reqMarketDataType(4)
            contract = self.ib_connection.create_contract(instrument_type, symbol, expiry, strike, right)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            instrument_suffix = "STK" if instrument_type == "stock" else "OPT"
            
            if data_type == "OHLC":
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
                
                # Add current price, volume, and Greeks for options
                if instrument_type == "option":
                    ticker = self.ib_connection.ib.reqMktData(contract)
                    self.ib_connection.ib.sleep(2)  # Wait for data to arrive
                    df['currentPrice'] = ticker.last if ticker.last else ticker.close
                    df['volume'] = ticker.volume
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
                end = datetime.now()
                start = end - pd.Timedelta(duration)
                ticks = self.ib_connection.ib.reqHistoricalTicks(contract, start, end, 1000, 'TRADES', useRth=False)
                if not ticks:
                    logger.error(f"No historical tick data returned for {symbol}")
                    return None
                df = util.df(ticks)
                
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
                
                # Add cumulative volume
                df['cumulativeVolume'] = df['size'].cumsum()

                filename = f"{symbol}_TICK_{instrument_suffix}_{timestamp}.csv"
                df.to_csv(filename)
                logger.info(f"Historical tick data saved to {filename}")
                return df

            elif data_type == "realtime":
                # New method for 1-second frequency data
                def on_bar_update(bars, has_new_bar):
                    if has_new_bar:
                        latest_bar = bars[-1]
                        print(f"Time: {latest_bar.date}, Open: {latest_bar.open}, High: {latest_bar.high}, Low: {latest_bar.low}, Close: {latest_bar.close}, Volume: {latest_bar.volume}")

                bars = self.ib_connection.ib.reqRealTimeBars(contract, 1, 'TRADES', False)
                bars.updateEvent += on_bar_update
                
                print("Streaming 1-second data. Press Ctrl+C to stop.")
                try:
                    while True:
                        self.ib_connection.ib.sleep(1)
                except KeyboardInterrupt:
                    print("Stopped streaming.")
                finally:
                    self.ib_connection.ib.cancelRealTimeBars(bars)

        except Exception as e:
            logger.error(f"Error in get_market_data: {str(e)}", exc_info=True)
            return None

class LiveStreaming:
    def __init__(self, ib_connection):
        self.ib_connection = ib_connection
        self.raw_df = None
        self.filtered_df = None
        self.streaming = False
        self.real_time_bars = {}

    def stream_live_data(self, contracts):
        if not self.ib_connection.ensure_connection():
            raise ConnectionError("Failed to connect to IB")

        columns = ['bidSize', 'bid', 'ask', 'askSize', 'last', 'lastSize', 'high', 'low', 'volume', 'close', 
                   'impliedVol', 'delta', 'gamma', 'vega', 'theta',
                   'rtb_open', 'rtb_high', 'rtb_low', 'rtb_close', 'rtb_volume', 'rtb_wap', 'rtb_count']

        self.raw_df = pd.DataFrame(index=[c.symbol for c in contracts], columns=columns)
        self.filtered_df = self.raw_df.copy()

        def onPendingTickers(tickers):
            for t in tickers:
                if t.contract.symbol in self.raw_df.index:
                    new_data = [
                        t.bidSize or 0, t.bid or 0, t.ask or 0, t.askSize or 0, t.last or 0, t.lastSize or 0,
                        t.high or 0, t.low or 0, t.volume or 0, t.close or 0,
                        t.modelGreeks.impliedVol if t.modelGreeks else 0,
                        t.modelGreeks.delta if t.modelGreeks else 0,
                        t.modelGreeks.gamma if t.modelGreeks else 0,
                        t.modelGreeks.vega if t.modelGreeks else 0,
                        t.modelGreeks.theta if t.modelGreeks else 0
                    ]
                    rtb_data = self.real_time_bars.get(t.contract.symbol, [0] * 7)
                    new_data.extend(rtb_data)
                    self.raw_df.loc[t.contract.symbol] = new_data
                    
                    # Update filtered_df, replacing 0 with previous non-zero values
                    for i, value in enumerate(new_data):
                        if value != 0:
                            self.filtered_df.iloc[self.filtered_df.index.get_loc(t.contract.symbol), i] = value
                        elif self.filtered_df.iloc[self.filtered_df.index.get_loc(t.contract.symbol), i] == 0:
                            # If both new and old values are 0, use the last non-zero value from raw_df
                            last_non_zero = next((v for v in self.raw_df.iloc[self.raw_df.index.get_loc(t.contract.symbol), i::-1] if v != 0), 0)
                            self.filtered_df.iloc[self.filtered_df.index.get_loc(t.contract.symbol), i] = last_non_zero

            clear_output(wait=True)
            display(self.filtered_df)
            print("\nPress Ctrl+C to stop streaming.")

        def onBarUpdate(bars, hasNewBar):
            if hasNewBar:
                symbol = bars.contract.symbol
                latest_bar = bars[-1]
                self.real_time_bars[symbol] = [
                    latest_bar.open, latest_bar.high, latest_bar.low, latest_bar.close,
                    latest_bar.volume, latest_bar.wap, latest_bar.count
                ]

        self.ib_connection.ib.pendingTickersEvent += onPendingTickers
        self.ib_connection.ib.reqMarketDataType(4)
        for contract in contracts:
            self.ib_connection.ib.reqMktData(contract, '', False, False)
            try:
                bars = self.ib_connection.ib.reqRealTimeBars(contract, 5, 'TRADES', False)
                bars.updateEvent += onBarUpdate
            except Exception as e:
                logger.warning(f"Real-time bars not available for {contract.symbol}: {str(e)}")
                print(f"Real-time bars not available for {contract.symbol}. Using regular market data.")

        self.streaming = True

        def signal_handler(signum, frame):
            self.streaming = False
            print("\nStopping live streaming...")

        signal.signal(signal.SIGINT, signal_handler)

        try:
            while self.streaming:
                if not self.ib_connection.ensure_connection():
                    raise ConnectionError("Lost connection to IB")
                self.ib_connection.ib.sleep(1)
        except Exception as e:
            logger.error(f"Error during live streaming: {str(e)}", exc_info=True)
        finally:
            self.ib_connection.ib.pendingTickersEvent -= onPendingTickers
            for contract in contracts:
                self.ib_connection.ib.cancelMktData(contract)
                try:
                    self.ib_connection.ib.cancelRealTimeBars(self.ib_connection.ib.reqRealTimeBars(contract, 5, 'TRADES', False))
                except:
                    pass  # If real-time bars were not available, this will fail silently
            
            # Save filtered_df to parquet
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            filename = f"live_data_{timestamp}.parquet"
            self.filtered_df.to_parquet(filename)
            logger.info(f"Live data saved to {filename}")
            print(f"Live data saved to {filename}")

        return self.filtered_df

if __name__ == "__main__":
    ib_connection = IBConnection('127.0.0.1', 7497, 1)
    if not ib_connection.connect():
        print("Exiting due to connection failure")
        exit(1)

    market_data = MarketData(ib_connection)
    live_streaming = LiveStreaming(ib_connection)

    while True:
        print("\nOptions:")
        print("1. Get OHLC data")
        print("2. Get historical tick data")
        print("3. Stream live data")
        print("4. Get 1-second frequency data")
        print("5. Exit")
        
        choice = input("Enter your choice (1-5): ")
        
        if choice in ['1', '2', '4']:
            instrument_type = input("Enter instrument type (stock/option): ")
            symbol = input("Enter symbol: ")
            expiry = strike = right = None
            if instrument_type == 'option':
                expiry = input("Enter expiry (YYYYMMDD): ")
                strike = float(input("Enter strike price: "))
                right = input("Enter right (C/P): ")
            
            if choice == '1':
                timeframe = input("Enter timeframe (e.g., 1 min, 1 hour, 1 day): ")
                duration = input("Enter duration (e.g., 1 D, 1 W, 1 M): ")
                df = market_data.get_market_data(instrument_type, symbol, "OHLC", timeframe, duration, expiry, strike, right)
                if df is not None:
                    print(df)
            elif choice == '2':
                duration = input("Enter duration (e.g., 1 D, 1 W, 1 M): ")
                df = market_data.get_market_data(instrument_type, symbol, "historical", duration=duration, expiry=expiry, strike=strike, right=right)
                if df is not None:
                    print(df)
            elif choice == '4':
                market_data.get_market_data(instrument_type, symbol, "realtime", expiry=expiry, strike=strike, right=right)
        elif choice == '3':
            print("Enter symbols to stream (comma-separated):")
            symbols = input().split(',')
            contracts = []
            for symbol in symbols:
                instrument_type = input(f"Enter instrument type for {symbol} (stock/option): ")
                expiry = strike = right = None
                if instrument_type == 'option':
                    expiry = input(f"Enter expiry for {symbol} (YYYYMMDD): ")
                    strike = float(input(f"Enter strike price for {symbol}: "))
                    right = input(f"Enter right for {symbol} (C/P): ")
                contract = ib_connection.create_contract(instrument_type, symbol.strip(), expiry, strike, right)
                contracts.append(contract)
            df = live_streaming.stream_live_data(contracts)
            print("Final dataframe:")
            print(df)
        elif choice == '5':
            break
        else:
            print("Invalid choice. Please try again.")

    ib_connection.disconnect()