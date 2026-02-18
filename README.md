# base555import time
import csv
from datetime import datetime
from web3 import Web3

RPC_URL = "https://mainnet.base.org"
CHAIN_ID = 8453

UNISWAP_V3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")

# keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

FACTORY_ABI_MIN = [
    {
        "name": "getPool",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "fee", "type": "uint24"},
        ],
        "outputs": [{"name": "pool", "type": "address"}],
    }
]

POOL_ABI_MIN = [
    {
        "name": "token0",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
    },
    {
        "name": "token1",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
    },
]

ERC20_ABI_MIN = [
    {
        "name": "symbol",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "string"}],
    },
    {
        "name": "decimals",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint8"}],
    },
]


def to_int256(b: bytes) -> int:
    v = int.from_bytes(b, "big", signed=False)
    if v >= 2**255:
        v -= 2**256
    return v


def safe_symbol(contract, fallback="???"):
    try:
        return contract.functions.symbol().call()
    except Exception:
        return fallback


def safe_decimals(contract, fallback=18):
    try:
        return contract.functions.decimals().call()
    except Exception:
        return fallback


def main():
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        raise RuntimeError("❌ Cannot connect to Base RPC")

    print("✅ Connected to Base")
    print("Chain ID:", CHAIN_ID)

    # --- EDIT THESE ---
    # Example: WETH / USDC
    tokenA = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")  # WETH
    tokenB = Web3.to_checksum_address("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")  # USDC
    fee = 500
    out_csv = "swaps.csv"
    # ------------------

    factory = w3.eth.contract(address=UNISWAP_V3_FACTORY, abi=FACTORY_ABI_MIN)
    pool_addr = factory.functions.getPool(tokenA, tokenB, fee).call()

    if pool_addr == "0x0000000000000000000000000000000000000000":
        raise RuntimeError("❌ No Uniswap v3 pool found for this pair + fee")

    pool_addr = Web3.to_checksum_address(pool_addr)
    print("\n✅ Pool:", pool_addr)

    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI_MIN)
    token0_addr = Web3.to_checksum_address(pool.functions.token0().call())
    token1_addr = Web3.to_checksum_address(pool.functions.token1().call())

    token0 = w3.eth.contract(address=token0_addr, abi=ERC20_ABI_MIN)
    token1 = w3.eth.contract(address=token1_addr, abi=ERC20_ABI_MIN)

    sym0 = safe_symbol(token0, "token0")
    sym1 = safe_symbol(token1, "token1")
    dec0 = safe_decimals(token0, 18)
    dec1 = safe_decimals(token1, 18)

    print("token0:", token0_addr, f"({sym0}, dec={dec0})")
    print("token1:", token1_addr, f"({sym1}, dec={dec1})")

    # Prepare CSV (append mode)
    file_exists = False
    try:
        with open(out_csv, "r", newline="", encoding="utf-8"):
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    f = open(out_csv, "a", newline="", encoding="utf-8")
    writer = csv.writer(f)

    if not file_exists:
        writer.writerow(
            [
                "iso_time",
                "block",
                "tx_hash",
                "token0",
                "token1",
                "amount0",
                "amount1",
                "price_token1_per_token0",
            ]
        )
        f.flush()

    last = w3.eth.block_number
    print("\nStarting block:", last)
    print(f"Writing to: {out_csv}")
    print("Watching swaps...\n")

    try:
        while True:
            current = w3.eth.block_number

            if current > last:
                for b in range(last + 1, current + 1):
                    block = w3.eth.get_block(b, full_transactions=False)
                    iso_time = datetime.utcfromtimestamp(block["timestamp"]).isoformat() + "Z"

                    logs = w3.eth.get_logs(
                        {
                            "fromBlock": b,
                            "toBlock": b,
                            "address": pool_addr,
                            "topics": [SWAP_TOPIC],
                        }
                    )

                    for log in logs:
                        raw = bytes.fromhex(log["data"][2:])

                        amount0_raw = to_int256(raw[0:32])
                        amount1_raw = to_int256(raw[32:64])

                        amount0 = amount0_raw / (10 ** dec0)
                        amount1 = amount1_raw / (10 ** dec1)

                        price = ""
                        if amount0 != 0:
                            price = abs(amount1) / abs(amount0)

                        tx = log["transactionHash"].hex()

                        writer.writerow(
                            [
                                iso_time,
                                b,
                                tx,
                                sym0,
                                sym1,
                                amount0,
                                amount1,
                                price,
                            ]
                        )
                        f.flush()

                        print(f"Block {b} | tx={tx}")
                        print(f"  {sym0}: {amount0}")
                        print(f"  {sym1}: {amount1}")
                        if price != "":
                            print(f"  Price: {price} {sym1} per {sym0}")
                        print("")

                last = current

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        f.close()


if __name__ == "__main__":
    main()
