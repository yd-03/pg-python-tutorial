import sys
from module.db_utils import (
    load_environment_variables,
    get_db_connection,
    fetch_postgresql_version,
)


def main():
    """
    スクリプトのメイン処理
    """
    # Pythonのバージョンを標準エラー出力に出力
    sys.stderr.write("Python version: %s\n" % sys.version)

    # 環境変数をロード
    load_environment_variables()

    # データベース接続を取得
    conn = get_db_connection()

    if conn:
        # PostgreSQLのバージョンを取得して出力
        version = fetch_postgresql_version(conn)
        if version:
            print(version)

        # 接続をクローズ（リソースの解放）
        conn.close()


if __name__ == "__main__":
    main()
