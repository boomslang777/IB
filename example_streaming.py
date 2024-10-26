from j2 import IBConnection, LiveStreaming
import random
import pandas as pd
import numpy as np
from collections import deque
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def basic_live_stream():
    """Basic example of live streaming"""
    ib_connection = IBConnection('127.0.0.1', 7497, random.randint(1, 1000))
    
    try:
        if not ib_connection.connect():
            print("Failed to connect to IB")
            return

        # Create contracts for the symbols you want to stream
        contracts = []
        
        # Example: Add a stock contract
        stock_contract = ib_connection.create_contract(
            instrument_type="stock",
            symbol="AAPL"
        )
        contracts.append(stock_contract)
        
        # Example: Add an option contract
        option_contract = ib_connection.create_contract(
            instrument_type="option",
            symbol="SPY",
            expiry="20240621",  # Format: YYYYMMDD
            strike=470,
            right="C"  # C for Call, P for Put
        )
        contracts.append(option_contract)

        # Initialize live streaming
        live_streaming = LiveStreaming(ib_connection)
        
        # Start streaming (this will run until you press Ctrl+C)
        print("Starting basic live stream... Press Ctrl+C to stop")
        df = live_streaming.stream_live_data(contracts)
        
        print("\nFinal dataframe:")
        print(df)
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        ib_connection.disconnect()

class LiveStreamWithSMA(LiveStreaming):
    def __init__(self, ib_connection, sma_window=10):
        super().__init__(ib_connection)
        self.sma_window = sma_window
        self.price_queues = {}  # Dictionary to store price queues for each symbol
        self.sma_values = {}    # Dictionary to store SMA values for each symbol
        self.stop_streaming = False
        self.real_time_bars = {}

    def onBarUpdate(self, bar):
        """Callback for real-time bar updates"""
        symbol = bar.contract.symbol
        self.real_time_bars[symbol] = [
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.wap,
            bar.count
        ]

    def stream_live_data(self, contracts):
        if not isinstance(contracts, list):
            contracts = [contracts]

        # Initialize dataframes first
        columns = [
            'bidSize', 'bid', 'ask', 'askSize', 'last', 'lastSize',
            'high', 'low', 'volume', 'close',
            'impliedVol', 'delta', 'gamma', 'vega', 'theta',
            'rtb_open', 'rtb_high', 'rtb_low', 'rtb_close',
            'rtb_volume', 'rtb_wap', 'rtb_count',
            'SMA'  # Add SMA column
        ]
        
        index = [contract.symbol for contract in contracts]
        self.raw_df = pd.DataFrame(0, index=index, columns=columns)
        self.filtered_df = self.raw_df.copy()

        # Initialize queues for each contract
        for contract in contracts:
            self.price_queues[contract.symbol] = deque(maxlen=self.sma_window)
            self.sma_values[contract.symbol] = 0

        def onPendingTickers(tickers):
            for t in tickers:
                if t.contract.symbol in self.raw_df.index:
                    # Get real-time bar data
                    rtb_data = self.real_time_bars.get(t.contract.symbol, [0] * 7)
                    
                    # Get the last price
                    last_price = t.last or t.close or 0
                    
                    if last_price != 0:
                        # Update price queue
                        self.price_queues[t.contract.symbol].append(last_price)
                        # Calculate SMA
                        self.sma_values[t.contract.symbol] = np.mean(list(self.price_queues[t.contract.symbol]))

                    # Create new data array with market data
                    new_data = {
                        'bidSize': t.bidSize or 0,
                        'bid': t.bid or 0,
                        'ask': t.ask or 0,
                        'askSize': t.askSize or 0,
                        'last': last_price,
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
                        'rtb_count': rtb_data[6],
                        'SMA': self.sma_values[t.contract.symbol]
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
                from IPython.display import clear_output, display
                clear_output(wait=True)
                print(f"\nCurrent Data with {self.sma_window}-tick SMA:")
                print(self.filtered_df)
                print("\nPress Ctrl+C to stop streaming.")

        # Request market data and real-time bars for each contract
        tickers = []
        bars = []
        for contract in contracts:
            ticker = self.ib_connection.ib.reqMktData(contract)
            tickers.append(ticker)
            
            bar = self.ib_connection.ib.reqRealTimeBars(contract, 5, 'TRADES', False)
            bar.updateEvent += self.onBarUpdate
            bars.append(bar)

        self.ib_connection.ib.pendingTickersEvent += onPendingTickers

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
                bar.updateEvent -= self.onBarUpdate

            # Save final data
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            filename = f"live_data_{timestamp}.parquet"
            self.filtered_df.to_parquet(filename)
            logger.info(f"Live data saved to {filename}")
            print(f"Live data saved to {filename}")
            print("Final dataframe:")
            print(self.filtered_df)
            
            return self.filtered_df

def live_stream_with_sma():
    """Example of live streaming with SMA calculation"""
    ib_connection = IBConnection('127.0.0.1', 7497, random.randint(1, 1000))
    
    try:
        if not ib_connection.connect():
            print("Failed to connect to IB")
            return

        # Create contract(s) for streaming
        contracts = []
        
        # Example: Stream AAPL stock
        stock_contract = ib_connection.create_contract(
            instrument_type="stock",
            symbol="AAPL"
        )
        contracts.append(stock_contract)

        # Initialize live streaming with SMA
        live_streaming = LiveStreamWithSMA(ib_connection, sma_window=10)
        
        # Start streaming with SMA calculation
        print("Starting live stream with SMA... Press Ctrl+C to stop")
        df = live_streaming.stream_live_data(contracts)
        
        print("\nFinal dataframe with SMA:")
        print(df)
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        ib_connection.disconnect()

def main():
    print("Choose streaming example:")
    print("1. Basic live streaming")
    print("2. Live streaming with SMA calculation")
    
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == "1":
        basic_live_stream()
    elif choice == "2":
        live_stream_with_sma()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
