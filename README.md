# Discord 犯罪報酬集計ボット

このボットは、Discord上でメッセージとリアクションを集計し、犯罪ごとの報酬や勝率を計算するものです。

## ✅ 機能

- 🔧 主なコマンドと機能

!search yyyy/mm/dd yyyy/mm/dd

指定期間中のDiscordメッセージから犯罪名・参加者・検挙（⭕/❌）情報を抽出し、シートに記録。
	•	抽出された情報は「シート2」に保存
	•	ユーザー情報（表示名・ID）は「シート1」に記録・更新

!calculate yyyy/mm/dd yyyy/mm/dd

期間内の小型強盗の成功結果に応じて、報酬（例：100万 or 50万）を算出して「シート3」に記録。
	•	Discordにも報酬額を出力

!count [user_id] yyyy/mm/dd yyyy/mm/dd

各ユーザーの対応件数・検挙数を分類（小型／中型以上）し、「シート4」に記録。
	•	指定ユーザーIDのみ抽出も可能

!add

「シート4」の件数を「シート5」に加算・累積記録。
	•	初めて「小型100件検挙」や「中型以上50件検挙」に到達したユーザーには通知

!get_ticket

「シート4」の件数に基づきチケットを付与（例：小型20件ごと、中型以上10件ごと）。
	•	チケットは「シート5」に加算され、獲得者に通知される

!rate yyyy/mm/dd yyyy/mm/dd

指定期間中の各犯罪ごとの勝率（成功率）を集計・出力。
	•	小型／中型／大型ごとに分類して表示

⸻

🗂 使用しているGoogleスプレッドシート
	•	シート1：ユーザー情報（表示名・ID）
	•	シート2：犯罪対応ログ（日付、種類、カテゴリ、参加者、検挙）
	•	シート3：報酬計算結果
	•	シート4：一時的な対応件数の集計
	•	シート5：累積記録（対応件数・検挙数・チケット枚数）
	•	シート6：勝率記録（今後の拡張用）

## 📦 セットアップ

### 1. リポジトリをクローン
```bash
git clone https://github.com/yourname/yourrepo.git
cd yourrepo

