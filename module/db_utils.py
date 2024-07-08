import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import sys


def load_environment_variables():
    """
    環境変数をロードする関数

    args:
        None

    return:
        None
    """
    # 環境変数からdotenvファイルのパスを取得し、存在しない場合はデフォルト値を使用
    dotenv_path = os.getenv("DOTENV_PATH", "database.env")
    # dotenvファイルを読み込み
    load_dotenv(dotenv_path)


def get_db_connection():
    """
    データベース接続を取得する関数

    args:
        None

    return:
        conn: データベース接続
    """
    # 環境変数からPostgreSQLの接続情報を取得
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")

    try:
        # PostgreSQLに接続
        conn = psycopg2.connect(
            dbname=database, user=user, password=password, host="localhost"
        )
        return conn
    except psycopg2.Error as e:
        # データベースエラーが発生した場合、エラーメッセージを標準エラー出力に出力
        sys.stderr.write(f"Database connection error: {e}\n")
        sys.exit(1)


def fetch_postgresql_version(conn):
    """
    PostgreSQLのバージョンを取得する関数

    args:
        conn: データベース接続

    return:
        row[0]: PostgreSQLのバージョン
    """
    try:
        # カーソルを作成（辞書形式で結果を取得するための設定）
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # SQLクエリを実行（PostgreSQLのバージョンを取得）
        sql_str = "SELECT version();"
        cur.execute(sql_str)
        # 結果を取得
        row = cur.fetchone()  # 1行だけ取得
        return row[0]
    except psycopg2.Error as e:
        # データベースエラーが発生した場合、エラーメッセージを標準エラー出力に出力
        sys.stderr.write(f"SQL execution error: {e}\n")
        return None
    finally:
        # カーソルをクローズ（リソースの解放）
        if cur:
            cur.close()


def load_sql(file_path):
    """
    SQLファイルを読み込む関数

    args:
        file_path: SQLファイルのパス

    return:
        str: SQLファイルの中身
    """
    with open(file_path, "r") as file:
        return file.read()
