import csv
import datetime
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === ChromeDriver 設定 ===
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(), options=options)
wait = WebDriverWait(driver, 15)

# === 出力CSVファイル名 ===
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
output_csv_path = f"moneyworldreport_{timestamp}.csv"

# === 出力CSVヘッダー ===
headers = [
    "証券コード", "URL", "レーティング", "売上高予想", "経常利益予想", "規模", "割安度", "成長", "収益性",
    "安全性", "リスク", "リターン", "流動性", "トレンド", "為替", "テクニカル"
]

# === 要素取得関数 ===
def sel_extract(by, value):
    try:
        return driver.find_element(by, value).text.strip()
    except:
        return ""

# === 入力ファイル ===
input_csv_path = "/Users/fukuotannaka/Desktop/FILE/DataAnalysis/money/QUICKMoneyWorld/consensus2025-07-16_08-01.csv"

# === メイン処理 ===
with open(input_csv_path, newline='', encoding='utf-8') as infile, open(output_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    writer = csv.writer(outfile)
    writer.writerow(headers)

    for row in reader:
        code = row["証券コード"].strip()
        url = row["QUICK Money World"].strip()

        try:
            driver.get(url)

            # ページ構造の要確認ポイント（エラーでスキップ）
            wait.until(EC.presence_of_element_located((By.ID, "stock-page")))
            wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[1]/dd/span[3]')))
            time.sleep(1.5)  # 念のための待機

            # データ取得
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

            writer.writerow([
                code, url, rating, sales, profit, scale, cheap, growth, profitab, safety,
                risk, return_rate, liquidity, trend, forex, technical
            ])

        except Exception as e:
            print(f"❌ QUICK error for {code}: {e}")
            # 空白データを書き出し（証券コードだけ保持）
            writer.writerow([code] + [""] * (len(headers) - 1))
            continue

        # ランダムな待機時間（アクセス分散）
        time.sleep(random.uniform(2, 3))

# 終了処理
driver.quit()
print(f"✅ 保存完了: {output_csv_path}")