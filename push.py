from module.db_utils import (
    get_db_connection,
    load_environment_variables,
    load_sql,
    insert_data_from_single_csv,
)


# 環境変数をロード
load_environment_variables()

# SQLファイルを読み込む
create_table_sql = load_sql("./sql/create_table.sql")
insert_sql = load_sql("./sql/insert_data.sql")

# CSVファイルが格納されているディレクトリ
csv_directory = "./raw_data"

# 処理する単一のCSVファイルとその年
csv_file_path = "./raw_data/2015.csv"
survey_yr = 2015

try:
    # データベース接続を取得し、自動でクローズする
    with get_db_connection() as conn:
        # カーソルを取得し、自動でクローズする
        with conn.cursor() as cursor:
            # テーブルを作成する
            cursor.execute(create_table_sql)
            # ディレクトリ内のすべてのCSVファイルを処理
            # for filename in os.listdir(csv_directory):
            #     if filename.endswith(".csv"):
            #         csv_file_path = os.path.join(csv_directory, filename)
            #         survey_yr = int(filename.split(".")[0])  # ファイル名から年を取得
            #         insert_data_from_csv(cursor, insert_sql, csv_file_path, survey_yr)
            # 単一のCSVファイルを処理
            insert_data_from_single_csv(cursor, insert_sql, csv_file_path, survey_yr)
            # コミット
            conn.commit()
except Exception as e:
    # エラーログを出力
    print(f"An error occurred: {e}")
    # 必要に応じてロールバック
    conn.rollback()
