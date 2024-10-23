from j2 import IBConnection, LiveStreaming
import random

def start_live_stream():
    # Initialize IB connection
    ib_connection = IBConnection('127.0.0.1', 7497, random.randint(1, 1000))
    
    try:
        # Connect to IB
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
        print("Starting live stream... Press Ctrl+C to stop")
        df = live_streaming.stream_live_data(contracts)
        
        # The dataframe will be available after streaming stops
        print("\nFinal dataframe:")
        print(df)
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        ib_connection.disconnect()

if __name__ == "__main__":
    start_live_stream()
