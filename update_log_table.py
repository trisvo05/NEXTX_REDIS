
import mysql.connector
import os
import time
from dotenv import load_dotenv

load_dotenv()

# CONFIG
MIN_NGRAM = 3
MAX_NGRAM = 10

db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", 3306)),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE")
)

db.autocommit = True
cursor = db.cursor(dictionary=True)


from datetime import datetime, timedelta

def delete():
    cutoff_time = datetime.now() - timedelta(hours=2)

    cursor.execute(
        """
        DELETE FROM crm_call_history_log
        WHERE changed_at < %s
        """,
        (cutoff_time,)
    )

    print(f"🗑️ Đã xóa {cursor.rowcount} bản ghi cũ hơn 2 giờ")
    time.sleep(2*60*60+5*60) 
    # sau 2h5p 

if __name__ == "__main__":
    while True:
        delete()




