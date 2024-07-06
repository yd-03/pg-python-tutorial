from module.db_utils import get_db_connection, load_environment_variables

# 環境変数をロード
load_environment_variables()

try:
    # データベース接続を取得し、自動でクローズする
    with get_db_connection() as conn:
        # カーソルを取得し、自動でクローズする
        with conn.cursor() as cursor:
            # sqlファイルを読み込む
            with open("./sql/example.sql") as f:
                sql = f.read()
                cursor.execute(sql)
            # コミット
            conn.commit()
except Exception as e:
    # エラーログを出力
    print(f"An error occurred: {e}")
    # 必要に応じてロールバック
    conn.rollback()
