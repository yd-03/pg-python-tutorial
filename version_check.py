import os
import sys
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Pythonのバージョンを標準エラー出力に出力
sys.stderr.write("Python version: %s\n" % sys.version)

# 環境変数からdotenvファイルのパスを取得し、存在しない場合はデフォルト値を使用
dotenv_path = os.getenv("DOTENV_PATH", "database.env")
# dotenvファイルを読み込み
load_dotenv(dotenv_path)

# 環境変数からPostgreSQLの接続情報を取得
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATA_BASE = os.getenv("POSTGRES_DB")

try:
    # PostgreSQLに接続
    conn = psycopg2.connect(
        dbname=DATA_BASE, user=USER, password=PASSWORD, host="localhost"
    )
    # カーソルを作成（辞書形式で結果を取得するための設定）
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # SQLクエリを実行（PostgreSQLのバージョンを取得）
    sql_str = "SELECT version();"
    cur.execute(sql_str)
    # 結果を取得
    # rows = cur.fetchall() # 全ての結果を取得
    rows = cur.fetchone()  # 1行だけ取得
    # 結果を出力
    print(rows[0])
except psycopg2.Error as e:
    # データベースエラーが発生した場合、エラーメッセージを標準エラー出力に出力
    sys.stderr.write(f"Database error: {e}\n")
finally:
    # カーソルと接続をクローズ（リソースの解放）
    if cur:
        cur.close()
    if conn:
        conn.close()
