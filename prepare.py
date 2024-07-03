import pandas as pd
import glob
import os


def find_columns(file_content, file_year):
    # 各年に対応する列名のマッピングを辞書で定義
    column_map = {
        "2015": [
            "Year",
            "Country",
            "Happiness Rank",
            "Happiness Score",
            "Economy (GDP per Capita)",
            "Generosity",
            "Freedom",
            "Family",
            "Health (Life Expectancy)",
            "Trust (Government Corruption)",
        ],
        "2016": [
            "Year",
            "Country",
            "Happiness Rank",
            "Happiness Score",
            "Economy (GDP per Capita)",
            "Generosity",
            "Freedom",
            "Family",
            "Health (Life Expectancy)",
            "Trust (Government Corruption)",
        ],
        "2017": [
            "Year",
            "Country",
            "Happiness.Rank",
            "Happiness.Score",
            "Economy..GDP.per.Capita.",
            "Generosity",
            "Freedom",
            "Family",
            "Health..Life.Expectancy.",
            "Trust..Government.Corruption.",
        ],
        "2018": [
            "Year",
            "Country or region",
            "Overall rank",
            "Score",
            "GDP per capita",
            "Generosity",
            "Freedom to make life choices",
            "Social support",
            "Healthy life expectancy",
            "Perceptions of corruption",
        ],
        "2019": [
            "Year",
            "Country or region",
            "Overall rank",
            "Score",
            "GDP per capita",
            "Generosity",
            "Freedom to make life choices",
            "Social support",
            "Healthy life expectancy",
            "Perceptions of corruption",
        ],
    }

    try:
        # 指定された年に対応する列を取得
        uniformed_columns = file_content[column_map[file_year]]
    except KeyError:
        # サポートされていない年の場合のエラーハンドリング
        raise ValueError(f"Unsupported year: {file_year}")
    except Exception as e:
        # その他のエラーに対するエラーハンドリング
        raise ValueError(f"Error processing columns for year {file_year}: {e}")

    return uniformed_columns


# 統一後のカラム名を定義
column_names = [
    "survey_yr",
    "country",
    "overall_rank",
    "score",
    "gdp",
    "generosity",
    "freedom",
    "social_support",
    "life_exp",
    "gov_trust",
]

# データを格納するリストを初期化
dfs = []

# 指定されたディレクトリからすべてのCSVファイルを取得
files = glob.glob("./raw_data/*.csv")

for fp in files:
    # ファイル名から年を取得
    file_year = os.path.basename(fp).split(".")[0]
    try:
        # ファイルを読み込み、内容を取得
        file_content = pd.read_csv(fp)
        if file_content.empty:
            # ファイルが空の場合のエラーハンドリング
            raise ValueError(f"File {fp} is empty.")
        # 年を新しいカラムとして追加
        file_content = file_content.assign(Year=int(file_year) - 2000)
        # 各年に対応する列を取得
        uniformed_columns = find_columns(file_content, file_year)
        # カラム名を統一
        uniformed_columns.columns = column_names
        # データフレームをリストに追加
        dfs.append(uniformed_columns)
    except Exception as e:
        # エラーが発生した場合、ファイルをスキップしてエラーメッセージを表示
        print(f"Skipping file {fp} due to error: {e}")

if dfs:
    # すべてのデータフレームを結合
    all_years_df = pd.concat(dfs)
    # 結果をCSVファイルに出力
    all_years_df.to_csv("results.csv", index=False)
else:
    # 有効なデータがない場合のメッセージを表示
    print("No valid data to write to results.csv")
