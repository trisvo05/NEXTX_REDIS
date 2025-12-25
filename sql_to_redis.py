import json
import mysql.connector
import redis
import os
import time
import re
from dotenv import load_dotenv

load_dotenv()

# ================== CONFIG ==================
NGRAM_SIZE = 5
BATCH_SIZE = 5000
REDIS_DB = 0

# ================== REDIS ==================
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=REDIS_DB,
    decode_responses=True
)

# ================== MYSQL ==================
db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", 3306)),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE"),
    autocommit=True
)

cursor = db.cursor(dictionary=True)

# ================== GLOBAL ==================
last_sync = None

# =====================================================
# UTILS – NORMALIZE PHONE (CHỈ GIỮ 0–9)
# =====================================================
def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))

# =====================================================
# UTILS – SINH NGRAM 5 SỐ
# =====================================================
def generate_ngrams_fixed(phone):
    phone = normalize_phone(phone)
    if len(phone) < NGRAM_SIZE:
        return []

    return [
        phone[i:i + NGRAM_SIZE]
        for i in range(len(phone) - NGRAM_SIZE + 1)
    ]

# =====================================================
# STEP 1 – TẠO TRƯỚC 100.000 NGRAM (EXIST KEY)
# =====================================================
# def tao_full_ngram_keys():
#     print("🚀 STEP 1: TẠO FULL NGRAM EXIST KEY")

#     pipe = r.pipeline(transaction=False)

#     for i in range(10 ** NGRAM_SIZE):
#         gram = str(i).zfill(NGRAM_SIZE)
#         pipe.set(f"ngram:phone:exist:{gram}", 1)

#         if i % 10000 == 0:
#             pipe.execute()
#             print(f"✅ Created {i} exist keys")

#     pipe.execute()
#     print("🎉 HOÀN TẤT STEP 1\n")

# =====================================================
# STEP 2 – ĐỒNG BỘ LẦN ĐẦU (SQL → REDIS)
# =====================================================
def dong_bo_lan_1():
    print("🚀 STEP 2: ĐỒNG BỘ LẦN ĐẦU")

    last_id = 0
    cursor_unbuffered = db.cursor(dictionary=True, buffered=False)

    while True:
        cursor_unbuffered.execute("""
            SELECT id, from_number, to_number
            FROM crm_call_history
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
        """, (last_id, BATCH_SIZE))

        rows = cursor_unbuffered.fetchall()
        if not rows:
            break

        pipe = r.pipeline(transaction=False)

        for row in rows:
            call_history_id = row["id"]

            for field in ("from_number", "to_number"):
                phone = row.get(field)
                if not phone:
                    continue

                for gram in generate_ngrams_fixed(phone):
                    pipe.sadd(f"ngram:phone:data:{gram}", call_history_id)

            last_id = call_history_id

        pipe.execute()
        print(f"✅ Synced tới id={last_id}")

    cursor_unbuffered.close()
    print("🎉 HOÀN TẤT STEP 2\n")

# =====================================================
# STEP 3 – REALTIME SYNC TỪ LOG
# =====================================================
def dong_bo_tu_lan_sau():
    global last_sync
    print("🚀 STEP 3: REALTIME SYNC")

    while True:
        if last_sync is None:
            cursor.execute("""
                SELECT *
                FROM crm_call_history_log
                ORDER BY changed_at ASC
            """)
        else:
            cursor.execute("""
                SELECT *
                FROM crm_call_history_log
                WHERE changed_at > %s
                ORDER BY changed_at ASC
            """, (last_sync,))

        rows = cursor.fetchall()

        for row in rows:
            call_history_id = row["call_history_id"]
            action = row["action_type"]

            old_json = json.loads(row["old_data"]) if row["old_data"] else {}
            new_json = json.loads(row["new_data"]) if row["new_data"] else {}

            # DELETE
            if action == "DELETE":
                for field in ("from_number", "to_number"):
                    phone = old_json.get(field)
                    for gram in generate_ngrams_fixed(phone):
                        r.srem(f"ngram:phone:data:{gram}", call_history_id)

            # INSERT / UPDATE
            else:
                for field in ("from_number", "to_number"):
                    old_phone = old_json.get(field)
                    new_phone = new_json.get(field)

                    for gram in generate_ngrams_fixed(old_phone):
                        r.srem(f"ngram:phone:data:{gram}", call_history_id)

                    for gram in generate_ngrams_fixed(new_phone):
                        r.sadd(f"ngram:phone:data:{gram}", call_history_id)

        if rows:
            last_sync = rows[-1]["changed_at"]
            print(f"⏳ last_sync = {last_sync}")

        time.sleep(5)

# =====================================================
# MAINx
# =====================================================
if __name__ == "__main__":
    print("========== REDIS PHONE NGRAM SYNC ==========")

    # ⚠️ CHỈ CHẠY 1 LẦN DUY NHẤT
    # tao_full_ngram_keys()

    dong_bo_lan_1()
    dong_bo_tu_lan_sau()
