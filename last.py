from ib_insync import *
import pandas as pd
from datetime import datetime
import logging
from IPython.display import display, clear_output
import time

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
                filename = f"{symbol}_OHLC_{instrument_suffix}_{timestamp}.csv"
                df.to_csv(filename)
                logger.info(f"OHLC data saved to {filename}")
                return df

            elif data_type == "historical":
                end = datetime.now()
                start = end - pd.Timedelta(duration)
                ticks = self.ib_connection.ib.reqHistoricalTicks(contract, start, end, 1000, 'BID_ASK', useRth=False)
                if not ticks:
                    logger.error(f"No historical tick data returned for {symbol}")
                    return None
                df = util.df(ticks)
                filename = f"{symbol}_TICK_{instrument_suffix}_{timestamp}.csv"
                df.to_csv(filename)
                logger.info(f"Historical tick data saved to {filename}")
                return df

        except Exception as e:
            logger.error(f"Error in get_market_data: {str(e)}", exc_info=True)
            return None

class LiveStreaming:
    def __init__(self, ib_connection):
        self.ib_connection = ib_connection
        self.df = None

    def stream_live_data(self, contracts):
        if not self.ib_connection.ensure_connection():
            raise ConnectionError("Failed to connect to IB")

        self.df = pd.DataFrame(
            index=[c.symbol for c in contracts],
            columns=['bidSize', 'bid', 'ask', 'askSize', 'last', 'lastSize', 'high', 'low', 'volume', 'close', 'impliedVol', 'delta', 'gamma', 'vega', 'theta']
        )

        def onPendingTickers(tickers):
            for t in tickers:
                if t.contract.symbol in self.df.index:
                    self.df.loc[t.contract.symbol] = [
                        t.bidSize, t.bid, t.ask, t.askSize, t.last, t.lastSize,
                        t.high, t.low, t.volume, t.close,
                        t.modelGreeks.impliedVol if t.modelGreeks else '',
                        t.modelGreeks.delta if t.modelGreeks else '',
                        t.modelGreeks.gamma if t.modelGreeks else '',
                        t.modelGreeks.vega if t.modelGreeks else '',
                        t.modelGreeks.theta if t.modelGreeks else ''
                    ]
            clear_output(wait=True)
            display(self.df)
            print("\nPress Ctrl+C to manage subscriptions or exit.")

        self.ib_connection.ib.pendingTickersEvent += onPendingTickers
        self.ib_connection.ib.reqMarketDataType(4)
        for contract in contracts:
            self.ib_connection.ib.reqMktData(contract, '', False, False)

        try:
            while True:
                if not self.ib_connection.ensure_connection():
                    raise ConnectionError("Lost connection to IB")
                self.ib_connection.ib.sleep(1)
                
                try:
                    self.ib_connection.ib.sleep(0.1)
                except KeyboardInterrupt:
                    while True:
                        print("\nOptions:")
                        print("1. Unsubscribe from an instrument")
                        print("2. Continue streaming")
                        print("3. Exit streaming")
                        choice = input("Enter your choice (1-3): ")
                        
                        if choice == '1':
                            symbol = input("Enter the symbol to unsubscribe from: ")
                            for contract in contracts[:]:
                                if contract.symbol == symbol:
                                    self.ib_connection.ib.cancelMktData(contract)
                                    contracts.remove(contract)
                                    self.df = self.df.drop(symbol)
                                    print(f"Unsubscribed from {symbol}")
                                    break
                            else:
                                print(f"Symbol {symbol} not found in current subscriptions")
                        elif choice == '2':
                            break
                        elif choice == '3':
                            raise StopIteration
                        else:
                            print("Invalid choice. Please try again.")
                    
                    if contracts:
                        print("Continuing to stream remaining instruments...")
                    else:
                        print("No instruments left to stream. Exiting.")
                        break

        except StopIteration:
            print("Exiting streaming.")
        except Exception as e:
            logger.error(f"Error during live streaming: {str(e)}", exc_info=True)
        finally:
            self.ib_connection.ib.pendingTickersEvent -= onPendingTickers
            for contract in contracts:
                self.ib_connection.ib.cancelMktData(contract)
        return self.df

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
        print("4. Exit")
        
        choice = input("Enter your choice (1-4): ")
        
        if choice in ['1', '2']:
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
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")

    ib_connection.disconnect()