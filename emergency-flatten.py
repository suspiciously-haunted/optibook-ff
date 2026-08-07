import time
import logging
from collections import defaultdict
from optibook.synchronous_client import Exchange

# Block verbose internal client logs to keep things readable
logging.getLogger("client").setLevel("ERROR")

# Slicing parameters to avoid choking liquidity pools or breaking exchange clip rules
MAX_IOC_CLIP = 20  

def robust_nuclear_flatten():
    exchange = Exchange()
    exchange.connect()
    
    pnl_before = exchange.get_pnl()
    print("=== NUCLEAR FLATTEN SEQUENCE STARTED ===")
    print(f"Initial Snapshot PnL: {pnl_before:.2f}\n")
    
    # -------------------------------------------------------------------------
    # PHASE 1: Nuclear Pull on all Outstanding Open Orders
    # -------------------------------------------------------------------------
    print(">>> Phase 1: Canceling all resting open orders across all instruments...")
    try:
        # Get a full list of all possible instruments active via snapshot
        all_instruments = exchange.get_positions().keys()
        for iid in all_instruments:
            exchange.delete_orders(iid)
            time.sleep(0.05) # Small pacing delay to ensure rate-limits aren't breached
    except Exception as e:
        print(f"Warning during Phase 1 clear: {e}")
        
    time.sleep(0.5)  # Give the exchange gateway a brief moment to process cancellations

    # -------------------------------------------------------------------------
    # PHASE 2: Iterative Sweep Loop (Runs until ALL exposures hit zero)
    # -------------------------------------------------------------------------
    print("\n>>> Phase 2: Commencing sweeping loop...")
    
    max_iterations = 5  # Safety breaking point to prevent infinite looping if a market halts
    for iteration in range(1, max_iterations + 1):
        positions = exchange.get_positions()
        
        # Filter down to assets that genuinely have an open position delta
        active_positions = {iid: pos for iid, pos in positions.items() if pos != 0}
        
        if not active_positions:
            print(f"  [✓] Portfolio successfully flattened to clean slate on iteration {iteration-1}.")
            break
            
        print(f"  --- Sweep Iteration #{iteration} | Remaining Targets: {active_positions} ---")
        
        for iid, pos in active_positions.items():
            book = exchange.get_last_price_book(iid)
            
            try:
                if pos > 0:
                    # LONG position -> Slicing out Sell orders
                    volume_to_sell = min(int(pos), MAX_IOC_CLIP)
                    target_price = book.bids[0].price if (book and book.bids) else 0.10
                    print(f"    [-] SELLING {volume_to_sell} lots of {iid} crossing at {target_price:.2f} (Remaining: {pos})")
                    exchange.insert_order(iid, price=target_price, volume=volume_to_sell, side="ask", order_type="ioc")
                    
                elif pos < 0:
                    # SHORT position -> Slicing out Buy orders
                    volume_to_buy = min(int(abs(pos)), MAX_IOC_CLIP)
                    target_price = book.asks[0].price if (book and book.asks) else 99999.0
                    print(f"    [+] BUYING {volume_to_buy} lots of {iid} crossing at {target_price:.2f} (Remaining: {pos})")
                    exchange.insert_order(iid, price=target_price, volume=volume_to_buy, side="bid", order_type="ioc")
                    
            except Exception as e:
                print(f"    [!] Order execution failed on {iid}: {e}")
                
            time.sleep(0.10)  # Safe spacing to stay comfortably beneath the 20/s rate limit cap

        # Give the private trade matching subsystem time to process before checking inventory again
        time.sleep(0.8)

    # -------------------------------------------------------------------------
    # Final Reconciliation Audit
    # -------------------------------------------------------------------------
    positions_after = exchange.get_positions()
    pnl_after = exchange.get_pnl()
    
    print(f"\n=== FINAL RECONCILIATION SUMMARY ===")
    print(f"Residual Portfolio Status: {positions_after}")
    print(f"Ending PnL:                {pnl_after:.2f}")
    print(f"Total Slippage Capital Cost:   {(pnl_after - pnl_before):.2f}")

if __name__ == "__main__":
    robust_nuclear_flatten()