from module.db_utils import get_db_connection, load_environment_variables

# 環境変数をロード
load_environment_variables()
# データベース接続を取得
conn = get_db_connection()
