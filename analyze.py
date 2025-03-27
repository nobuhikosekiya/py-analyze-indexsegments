import json
import pandas as pd
import re

# ローカルファイル 'stats.json' からデータを読み込む
with open("stats.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# インデックスごとのセグメント数を取得する
index_segments = []
total_segments = 0  # セグメントの合計を格納する変数
grouped_segments = {}

# 正規表現で日付を特定（例: "-2024.10.30-000080" の部分を除外する）
date_pattern = re.compile(r"(-\d{4}\.\d{2}\.\d{2}-\d{6})$")

for index_name, index_data in data.get("indices", {}).items():
    segment_count = index_data.get("primaries", {}).get("segments", {}).get("count", 0)

    # プレフィックス部分を取得
    prefix = date_pattern.sub("", index_name)

    # インデックス単位のデータ
    index_segments.append({"Index Name": index_name, "Segment Count": segment_count})
    total_segments += segment_count  # セグメント数を合計に加算

    # グループ化してセグメント数とインデックス数を集計
    if prefix not in grouped_segments:
        grouped_segments[prefix] = {"Total Segment Count": 0, "Index Count": 0}
    
    grouped_segments[prefix]["Total Segment Count"] += segment_count
    grouped_segments[prefix]["Index Count"] += 1

# DataFrameを作成し、Index Nameでソート
df_indices = pd.DataFrame(index_segments).sort_values(by="Index Name")

# グループごとのデータをDataFrameに変換し、Segment数の降順でソート
df_groups = pd.DataFrame(
    [(prefix, data["Total Segment Count"], data["Index Count"]) for prefix, data in grouped_segments.items()],
    columns=["Index Prefix", "Total Segment Count", "Index Count"]
).sort_values(by="Total Segment Count", ascending=False)

# 結果を表示
print("\n=== インデックスごとのセグメント数 ===")
print(df_indices.to_string(index=False))

print("\nTotal Segment Count:", total_segments)

print("\n=== プレフィックスごとのセグメント数合計とインデックス数 (降順) ===")
print(df_groups.to_string(index=False))

# CSVとして保存（必要なら）
df_indices.to_csv("index_segments_sorted.csv", index=False, encoding="utf-8")
df_groups.to_csv("index_segments_grouped_sorted.csv", index=False, encoding="utf-8")
