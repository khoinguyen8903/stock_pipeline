import json
import time
import random
from confluent_kafka import Producer
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock_ticks_realtime"

def main():
    # Tăng buffer để chịu tải 10k/s
    p = Producer({
        'bootstrap.servers': KAFKA_BROKER, 
        'queue.buffering.max.messages': 1000000,
        'linger.ms': 100 # Đợi 100ms để gom tin nhắn gửi đi 1 lượt cho nhanh
    })
    
    stocks = ["FPT", "SSI", "HPG", "VIC", "VNM", "TCB", "GAS", "VHM", "MSN", "MWG"]
    
    print(f"🔥 STRESS TEST START: Aiming for 10,000+ ticks/sec...")
    
    count = 0
    start_time = time.time()

    try:
        while True:
            # Lấy timestamp hiện tại 1 lần cho mỗi cụm 1000 để tiết kiệm CPU
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for _ in range(1000): 
                symbol = random.choice(stocks)
                data = {
                    "symbol": symbol,
                    "lastPrice": round(random.uniform(20.0, 150.0), 2),
                    "totalVol": random.randint(100, 5000),
                    "id": "stress",
                    "ingested_at": now_ts
                }
                # Gửi không cần callback để đạt tốc độ tối đa
                p.produce(TOPIC, value=json.dumps(data))
                count += 1
            
            p.poll(0) 
            
            # Tính toán và in tốc độ mỗi giây
            elapsed = time.time() - start_time
            if elapsed >= 1.0:
                print(f"🚀 Current Throughput: {int(count/elapsed)} ticks/sec")
                count = 0
                start_time = time.time()

    except KeyboardInterrupt:
        print("\nStopping Stress Test...")
    finally:
        p.flush()

if __name__ == "__main__":
    main()