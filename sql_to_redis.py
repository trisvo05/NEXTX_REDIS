import json
import mysql.connector
import redis
import os
import time
from dotenv import load_dotenv

load_dotenv()

# CONFIG
MIN_NGRAM = 3
MAX_NGRAM = 10

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)

db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", 3306)),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE")
)

db.autocommit = True
cursor = db.cursor(dictionary=True)



# Biến để check thời gian đồng bộ lần cuối -> tránh trường hợp trùng lặp data 
last_sync = None


def generate_ngrams_range(text, min_n=3, max_n=10):
    if not text:
        return []

    text = str(text).strip()
    length = len(text)
    grams = []

    for n in range(min_n, min(max_n, length) + 1):
        for i in range(length - n + 1):
            grams.append(text[i:i+n])

    return grams


# Hàm đồng bộ dữ liệu từ sql _> redis
def dong_bo_lan_1():
    # global last_sync

    # if last_sync is None:
    cursor.execute("SELECT * FROM crm_call_history")
    # else:
    #     cursor.execute(
    #         "SELECT * FROM crm_call_history WHERE updated_at > %s",
    #         (last_sync,)
    #     )

    rows = cursor.fetchall()

    for row in rows:
        call_history_id = row["id"]
        for field in ["from_number", "to_number"]:
            phone = row.get(field)
            if not phone:
                continue

        # Lưu bản ghi call-history - trong trường hợp query trực tiếp từ redis , còn không thì query ngram (redis) để lấy call_id -> lại call từ sql
        # redis_key = f"call:{call_history_id}"

        # mapping = {
        #     "phone": str(phone),
        #     "time": str(row.get("time", "")),
        #     "duration": str(row.get("duration", "")),
        #     "updated_at": str(row.get("updated_at"))
        # }
        # for key, value in mapping.items():
        #     r.hset(redis_key, key, value)

        # Tạo ngram 
            ngrams = generate_ngrams_range(
                phone,
                min_n=MIN_NGRAM,
                max_n=MAX_NGRAM
            )

        # Lưu ngram vào redis 
            for gram in ngrams:
                r.sadd(f"ngram:phone:{gram}", call_history_id)
            # r.sadd(f"call:{call_history_id}:ngrams", gram)

        print(f"✅ Synced call_history_id={call_history_id}")

    # if rows:
    #     last_sync = max(row["updated_at"] for row in rows)



    print("""___________ ĐỒNG BỘ LẦN ĐẦU THÀNH CÔNG (Bảng crm_call_history) __________ 
            ------ BẮT ĐẦU CHẠY ĐỒNG BỘ từ bảng crm_call_history_log ------
            """)



def dong_bo_tu_lan_sau():
    global last_sync

    while True:
    # 1. Query log theo mốc thời gian
        if last_sync is None:
            cursor.execute("""
                SELECT id, call_history_id, action_type, old_data, new_data, changed_at
                FROM crm_call_history_log
                ORDER BY changed_at ASC
            """)
        else:
            cursor.execute("""
                SELECT id, call_history_id, action_type, old_data, new_data, changed_at
                FROM crm_call_history_log
                WHERE changed_at > %s
                ORDER BY changed_at ASC
            """, (last_sync,))

        rows = cursor.fetchall()

        for row in rows:
            # print(row["id"], row["changed_at"])
            call_history_id = row["call_history_id"]
            action_type = row["action_type"]
            old_data = row["old_data"]
            new_data = row["new_data"]  

            # 2. Parse JSON
            old_json = json.loads(old_data) if old_data else {}
            new_json = json.loads(new_data) if new_data else {}
                
            # 3. Xử lý dữ liệu theo action_type
            # TRIGGER DELETE
            if action_type == "DELETE":
                for field in ["from_number", "to_number"]:
                    phone = old_json.get(field)
                    if not phone:
                        continue

                    ngrams = generate_ngrams_range(phone)
                    for gram in ngrams:
                        r.srem(f"ngram:phone:{gram}", call_history_id)

            # TRIGGER INSERT/UPDATE
            else:
                for field in ["from_number", "to_number"]:
                    phone = new_json.get(field)
                    if not phone:
                        continue

                    ngrams = generate_ngrams_range(phone)
                    for gram in ngrams:
                        r.sadd(f"ngram:phone:{gram}", call_history_id)

            print(f"✅ Synced call_history_id={call_history_id} | action={action_type}")

        # 2. Update last_sync
        if rows:
            last_sync = rows[-1]["changed_at"]

        print(f"⏳ last_sync = {last_sync}")
        time.sleep(5)


if __name__ == "__main__":
    print("ĐỒNG BỘ")
    dong_bo_lan_1()
    dong_bo_tu_lan_sau()