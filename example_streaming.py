from j2 import IBConnection, LiveStreaming
import random
import pandas as pd
import numpy as np
from collections import deque

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

    def stream_live_data(self, contracts):
        # Initialize queues for each contract
        for contract in contracts:
            self.price_queues[contract.symbol] = deque(maxlen=self.sma_window)
            self.sma_values[contract.symbol] = 0

        def onPendingTickers(tickers):
            for t in tickers:
                if t.contract.symbol in self.raw_df.index:
                    # Get the last price
                    last_price = t.last or t.close or 0
                    
                    if last_price != 0:
                        # Update price queue
                        self.price_queues[t.contract.symbol].append(last_price)
                        # Calculate SMA
                        self.sma_values[t.contract.symbol] = np.mean(list(self.price_queues[t.contract.symbol]))

                    # Update raw_df with regular data
                    new_data = [
                        t.bidSize or 0, t.bid or 0, t.ask or 0, t.askSize or 0,
                        last_price, t.lastSize or 0, t.high or 0, t.low or 0,
                        t.volume or 0, t.close or 0,
                        t.modelGreeks.impliedVol if t.modelGreeks else 0,
                        t.modelGreeks.delta if t.modelGreeks else 0,
                        t.modelGreeks.gamma if t.modelGreeks else 0,
                        t.modelGreeks.vega if t.modelGreeks else 0,
                        t.modelGreeks.theta if t.modelGreeks else 0
                    ]
                    rtb_data = self.real_time_bars.get(t.contract.symbol, [0] * 7)
                    new_data.extend(rtb_data)
                    self.raw_df.loc[t.contract.symbol] = new_data

                    # Add SMA to the display
                    self.filtered_df.loc[t.contract.symbol] = new_data
                    self.filtered_df.loc[t.contract.symbol, 'SMA'] = self.sma_values[t.contract.symbol]

            from IPython.display import clear_output
            clear_output(wait=True)
            display_df = self.filtered_df.copy()
            print(f"\nCurrent Data with {self.sma_window}-tick SMA:")
            print(display_df)
            print("\nPress Ctrl+C to stop streaming.")

        # Add SMA column to dataframes
        self.raw_df['SMA'] = 0
        self.filtered_df['SMA'] = 0

        return super().stream_live_data(contracts)

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

if __name__ == "__main__":
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
