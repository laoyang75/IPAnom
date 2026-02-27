import time
import datetime

print(f"[{datetime.datetime.now()}] Starting 10 minute wait...")
for i in range(10):
    time.sleep(60)
    print(f"[{datetime.datetime.now()}] Minute {i+1} complete.")
print(f"[{datetime.datetime.now()}] Done.")
