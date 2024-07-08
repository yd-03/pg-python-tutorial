from module.db_utils import (
    get_db_connection,
    load_environment_variables,
    load_sql,
)
import pandas as pd


# 環境変数をロード
load_environment_variables()

# SQLファイルを読み込む
create_table_sql = load_sql("./sql/create_table.sql")
insert_sql = load_sql("./sql/insert_data.sql")

# CSVファイルを読み込む
csv_file_path = "./data/results.csv"
df = pd.read_csv(csv_file_path)

try:
    # データベース接続を取得し、自動でクローズする
    with get_db_connection() as conn:
        # カーソルを取得し、自動でクローズする
        with conn.cursor() as cursor:
            # テーブルを作成する
            cursor.execute(create_table_sql)
            # データを挿入する
            for _, row in df.iterrows():
                cursor.execute(
                    insert_sql,
                    (
                        row["survey_yr"],
                        row["country"],
                        row["overall_rank"],
                        row["score"],
                        row["gdp"],
                        row["generosity"],
                        row["freedom"],
                        row["social_support"],
                        row["life_exp"],
                        row["gov_trust"],
                    ),
                )
            # コミット
            conn.commit()
except Exception as e:
    # エラーログを出力
    print(f"An error occurred: {e}")
    # 必要に応じてロールバック
    conn.rollback()
