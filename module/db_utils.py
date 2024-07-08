import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import sys

import pandas as pd


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


def insert_data_from_csv(cursor, insert_sql, csv_file_path, survey_yr):
    """
    CSVファイルからデータを読み込み、データベースに挿入する関数

    args:
        cursor: データベースカーソル
        insert_sql: 挿入用SQL
        csv_file_path: CSVファイルのパス

    return:
        None
    """
    # CSVファイルを読み込み
    df = pd.read_csv(csv_file_path)
    # CSVの列名をデータベースのカラム名に変換
    column_mapping = {
        "Happiness Rank": "overall_rank",
        "Happiness Score": "score",
        "Economy (GDP per Capita)": "gdp",
        "Family": "social_support",
        "Health (Life Expectancy)": "life_exp",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust (Government Corruption)": "gov_trust",
        "Country": "country",
    }
    df = df.rename(columns=column_mapping)

    # 必要な列のみ抽出し、欠損値を0で埋める
    df = df[
        [
            "country",
            "overall_rank",
            "score",
            "gdp",
            "social_support",
            "life_exp",
            "freedom",
            "generosity",
            "gov_trust",
        ]
    ]

    # 'survey_yr'列を追加
    df["survey_yr"] = survey_yr

    # 必要な列を指定し、欠損値を0で埋める
    df = df[
        [
            "survey_yr",
            "country",
            "overall_rank",
            "score",
            "gdp",
            "social_support",
            "life_exp",
            "freedom",
            "generosity",
            "gov_trust",
        ]
    ].fillna(0)

    for index, row in df.iterrows():
        try:
            cursor.execute(insert_sql, tuple(row))
        except Exception as e:
            print(f"Error inserting row {index} from file {csv_file_path}: {e}")
            print(f"Row data: {row}")


def insert_data_from_single_csv(cursor, insert_sql, csv_file_path, survey_yr):
    """
    単一のCSVファイルからデータを読み込み、データベースに挿入する関数

    args:
        cursor: データベースカーソル
        insert_sql: 挿入用SQL
        csv_file_path: CSVファイルのパス
        survey_yr: 調査年

    return:
        None
    """
    df = pd.read_csv(csv_file_path)
    # CSVの列名をデータベースのカラム名に変換
    column_mapping = {
        "Happiness Rank": "overall_rank",
        "Happiness Score": "score",
        "Economy (GDP per Capita)": "gdp",
        "Family": "social_support",
        "Health (Life Expectancy)": "life_exp",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust (Government Corruption)": "gov_trust",
        "Country": "country",
    }
    df = df.rename(columns=column_mapping)
    # 必要な列のみ抽出し、欠損値を0で埋める
    df = df[
        [
            "country",
            "overall_rank",
            "score",
            "gdp",
            "social_support",
            "life_exp",
            "freedom",
            "generosity",
            "gov_trust",
        ]
    ]
    # 'survey_yr'列を追加
    df["survey_yr"] = survey_yr
    # 必要な列を指定し、欠損値を0で埋める
    df = df[
        [
            "survey_yr",
            "country",
            "overall_rank",
            "score",
            "gdp",
            "social_support",
            "life_exp",
            "freedom",
            "generosity",
            "gov_trust",
        ]
    ].fillna(0)
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_sql, tuple(row))
        except Exception as e:
            print(f"Error inserting row {index} from file {csv_file_path}: {e}")
            print(f"Row data: {row}")
            cursor.connection.rollback()  # トランザクションをロールバック
            cursor.connection.commit()  # トランザクションを再開
