import sys
import sqlite3
import datetime
import time
import random
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === target_dateを取得 ===
parser = argparse.ArgumentParser()
parser.add_argument("-a", "--target_date", required=True, help="取得対象日（YYYYMMDD）")
args = parser.parse_args()

target_date = args.target_date
try:
    datetime.datetime.strptime(target_date, "%Y%m%d")
except ValueError:
    raise ValueError("target_dateはYYYYMMDD形式で入力してください")

# === ChromeDriver 設定 ===
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
driver = webdriver.Chrome(service=Service(), options=options)
wait = WebDriverWait(driver, 15)

# === DB設定 ===
db_path = "/market_data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# === テーブル作成（url列なし） ===
cursor.execute("""
    CREATE TABLE IF NOT EXISTS moneyworld_reports (
        target_date TEXT,
        code TEXT,
        rating TEXT,
        sales TEXT,
        profit TEXT,
        scale TEXT,
        cheap TEXT,
        growth TEXT,
        profitab TEXT,
        safety TEXT,
        risk TEXT,
        return_rate TEXT,
        liquidity TEXT,
        trend TEXT,
        forex TEXT,
        technical TEXT,
        PRIMARY KEY (target_date, code)
    )
""")
conn.commit()

# === 要素取得関数 ===
def sel_extract(by, value):
    try:
        return driver.find_element(by, value).text.strip()
    except:
        return ""

# === 対象コードとURLを取得 ===
cursor.execute("SELECT code, quickurl FROM consensus_url WHERE target_date = ?", (target_date,))
rows = cursor.fetchall()

if not rows:
    print(f"❌ target_date={target_date} に一致するデータが consensus_url に存在しません")
    driver.quit()
    conn.close()
    sys.exit(1)

# === メイン処理 ===
for code, url in rows:
    try:
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.ID, "stock-page")))
        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[1]/dd/span[3]')))
        time.sleep(1.5)

        rating = sel_extract(By.XPATH, '//*[@id="stock-page"]/div[1]/div[3]/div[1]/section/div[1]/div[2]/div[3]/div[1]/span')
        sales = sel_extract(By.XPATH, '//*[@id="stock-page"]/div[1]/div[3]/div[1]/section/div[1]/div[2]/div[3]/div[2]/div[2]/div[1]/span')
        profit = sel_extract(By.XPATH, '//*[@id="stock-page"]/div[1]/div[3]/div[1]/section/div[1]/div[2]/div[3]/div[2]/div[2]/div[2]/span')
        scale = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[1]/dd/span[3]')
        cheap = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[2]/dd/span[3]')
        growth = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[3]/dd/span[3]')
        profitab = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[4]/dd/span[3]')
        safety = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[5]/dd/span[3]')
        risk = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[1]/dd/span[3]')
        return_rate = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[2]/dd/span[3]')
        liquidity = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[3]/dd/span[3]')
        trend = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[4]/dd/span[3]')
        forex = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[5]/dd/span[3]')
        technical = sel_extract(By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[6]/dd/span[3]')

        if sales and not sales.endswith("%"):
            sales += "%"
        if profit and not profit.endswith("%"):
            profit += "%"

        cursor.execute("""
            INSERT OR REPLACE INTO moneyworld_reports (
                target_date, code, rating, sales, profit, scale, cheap, growth, profitab,
                safety, risk, return_rate, liquidity, trend, forex, technical
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            target_date, code, rating, sales, profit, scale, cheap, growth, profitab,
            safety, risk, return_rate, liquidity, trend, forex, technical
        ))
        conn.commit()
        print(f"✅ {code} 保存完了")

    except Exception as e:
        print(f"❌ QUICK error for {code}: {e}")
        continue

    time.sleep(random.uniform(2, 3))

# 終了処理
driver.quit()
conn.close()
print("✅ 全件処理完了")
