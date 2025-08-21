quick-moneyworld-scraper

QUICK MoneyWorldからのデータ収集ツール（Playwright版）

⸻

📌 概要

このPythonスクリプトは、Playwright (async版) を用いて「QUICK Money World」の銘柄情報を安定的にスクレイピングし、株式関連の各種指標や評価データを SQLiteデータベース に保存します。

銘柄コードと対象URLを格納した consensus_url テーブルを基に、ページを自動巡回し、指定日のデータを効率的かつ礼儀正しく取得・蓄積します。

⸻

✅ 主な機能
	•	QUICK MoneyWorldから レーティング、売上高予想、経常利益予想、スコア指標（規模・成長性・収益性など） を取得
	•	未取得のみ(missing) / 全件(all) の切り替えに対応
	•	Playwright + 非同期処理 により並列で高速スクレイピング
	•	TokenBucketアルゴリズム によるQPS制御でアクセス過多を防止
	•	画像/フォント/メディア読み込み遮断 による軽量化
	•	SQLite自動保存（WAL + バッチコミット対応） で堅牢かつ効率的に蓄積

⸻

🗂 ファイル構成
	•	moneyworld_scraper.py : スクレイピングのメインスクリプト
	•	market_data.db : SQLite形式のデータベースファイル（実行時に自動生成）
	•	consensus_url テーブル：銘柄コードとURL一覧
	•	moneyworld_reports テーブル：MoneyWorldから抽出した指標データ

⸻

⚠️ 注意事項・免責
	•	本スクリプトは 学習・個人利用を目的 としています。
	•	スクレイピング対象サイトの構造変更により、動作しなくなる可能性があります。
	•	対象サイトの 利用規約・robots.txt を遵守してください。
	•	過剰アクセスを避け、礼儀を持った利用 を推奨します。
	•	本スクリプト利用により発生したいかなる損害についても、作成者は責任を負いません。

⸻

🛠 実行例

# 全件取得
python moneyworld_scraper.py --mode all

# 未取得のみ（デフォルト）
python moneyworld_scraper.py --mode missing

# concurrency=6, QPS=0.8 で実行
python moneyworld_scraper.py --concurrency 6 --qps 0.8


⸻

🗓 更新履歴
	•	2025/07/17 - QUICK MoneyWorldからのデータ収集 初版
	•	2025/07/31 - SQLite対応（CSV管理から移行）
	•	2025/08/21 - Playwright + 並列処理 + QPS制御に刷新
