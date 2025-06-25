import discord
from discord.ext import commands
from datetime import datetime, timedelta, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import asyncio
import os

JST = timezone(timedelta(hours=+9))

def get_timestamp_jst(message):
    return message.created_at.replace(tzinfo=timezone.utc).astimezone(JST).strftime("%Y/%m/%d %H:%M")

def get_msg_time_jst(message):
    return message.created_at.replace(tzinfo=timezone.utc).astimezone(JST)

# Discord bot setup
intents = discord.Intents.default()
intents.messages = True
intents.reactions = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("discord-bot-461305-97709530f0bf.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key("11MGwxoUOk2esCf-akJo2d3kSlLhV1rXsTwphuPn-HGI")
sheet1 = sheet.worksheet("シート1")
sheet2 = sheet.worksheet("シート2")
sheet3 = sheet.worksheet("シート3")
sheet4 = sheet.worksheet("シート4")
sheet5 = sheet.worksheet("シート5")
sheet6 = sheet.worksheet("シート6")

JST = timezone(timedelta(hours=9))

@bot.command()
async def search(ctx, start_date: str, end_date: str):
    # コマンド開始メッセージを送信
    await ctx.send("🔍 データ検索を開始します…")

    # メッセージを収集するチャンネルID一覧
    channel_ids = [1342103011396288512, 1309142327062954005]

    # 開始日と終了日を JST タイムゾーン付きで datetime に変換
    start = datetime.strptime(start_date, "%Y/%m/%d").replace(tzinfo=JST)
    end = datetime.strptime(end_date, "%Y/%m/%d").replace(tzinfo=JST) + timedelta(days=1)  # 終了日は翌日0時まで含める

    # 進捗報告用に総メッセージ数を事前カウント
    total_messages = 0
    for channel_id in channel_ids:
        channel = bot.get_channel(channel_id)
        if not channel:
            continue
        count = 0
        async for _ in channel.history(after=start, before=end, limit=None):
            count += 1
        total_messages += count

    user_map = {}
    record_data = []

    # シート2への追記
    existing_data = sheet2.get_all_values()
    sheet2_header = existing_data[0] if existing_data else ['日付', '犯罪の種類', '犯罪カテゴリ', '参加者', '検挙']
    new_rows = []

    # 犯罪名のエイリアス定義（メッセージ中のキーワードから正式名に変換）
    CRIME_ALIASES = {
        "コンビニ強盗": ["コンビニ"],
        "フリーカ強盗": ["フリーカ"],
        "モーテル強盗": ["モーテル"],
        "客船強盗": ["客船"],
        "空港強盗": ["空港"],
        "トレイン強盗": ["トレイン", "列車"],
        "コンテナ強盗": ["コンテナ"],
        "ボブキャット強盗": ["ボブキャット"],
        "オイルリグ強盗": ["オイルリグ","リグ","オイル"],
        "アーティファクト強盗": ["アーティファクト"],
        "ユニオン強盗": ["ユニオン"],
    }

    # 犯罪カテゴリの分類（小型・中型・大型）
    crime_categories = {
        "小型強盗": {"コンビニ強盗", "フリーカ強盗", "モーテル強盗"},
        "中型強盗": {"客船強盗", "空港強盗", "トレイン強盗", "コンテナ強盗", "ボブキャット強盗","オイルリグ強盗"},
        "大型強盗": {"アーティファクト強盗", "ユニオン強盗"},
    }

    # 小型強盗の報酬額（成功：100万、失敗：50万）
    reward_table = {
        "コンビニ強盗": 1000000,
        "フリーカ強盗": 1000000,
        "モーテル強盗": 1000000
    }

    # メッセージから犯罪名を抽出する関数
    def extract_crime_type(content):
        for official_name, aliases in CRIME_ALIASES.items():
            if any(alias in content for alias in aliases):
                return official_name
        return None

    # 進捗報告の頻度を設定（10回ごと、または最後）
    processed = 0
    progress_update_interval = max(total_messages // 10, 1)

    # 指定チャンネルのメッセージを1件ずつ処理
    for channel_id in channel_ids:
        channel = bot.get_channel(channel_id)
        if not channel:
            continue

        async for message in channel.history(after=start, before=end, limit=None):
            processed += 1

            # 一定間隔で進捗を表示
            if processed % progress_update_interval == 0 or processed == total_messages:
                percent = int(processed / total_messages * 100)
                await ctx.send(f"🔄 処理中... {percent}% 完了 ({processed}/{total_messages})")

            await asyncio.sleep(0.2)  # 負荷軽減

            # 犯罪名を抽出できない場合スキップ
            crime_type = extract_crime_type(message.content)
            if not crime_type:
                continue

            # 日付と初期情報の取得
            timestamp = get_timestamp_jst(message)
            participants = []
            results = set()

            guild = ctx.guild

            # リアクションから参加者と結果（⭕/❌）を抽出
            for reaction in message.reactions:
                if reaction.emoji in ["✅", "⭕", "❌"]:
                    async for user in reaction.users(limit=None):
                        await asyncio.sleep(0.1)
                        if user.bot:
                            continue
                        try:
                            member = guild.get_member(user.id) or await guild.fetch_member(user.id)
                            display_name = member.display_name
                        except discord.NotFound:
                            display_name = user.name

                        user_map[user.id] = display_name

                        if reaction.emoji == "✅":
                            participants.append(user.id)
                        elif reaction.emoji in ["⭕", "❌"]:
                            results.add(reaction.emoji)

            # 成功・失敗のリアクションが混在していたらスキップ
            if len(results) != 1:
                continue

            result = list(results)[0]  # "⭕" or "❌"
            category = next((k for k, v in crime_categories.items() if crime_type in v), "")
            names = [user_map.get(uid, str(uid)) for uid in participants]

            # シート2用データ記録
            record_data.append([timestamp, crime_type, category, ",".join(names), result])

    # --- ユーザー情報更新（シート1） ---
    sheet1_values = sheet1.get_all_values()
    existing_users = sheet1_values[1:]  # ヘッダーを除く
    header1 = sheet1_values[0] if sheet1_values else ['ディスプレイネーム', 'ユーザーID']
    user_id_to_name = {row[1]: row[0] for row in existing_users}  # {user_id: name}

    updated_sheet1_data = [header1] + existing_users.copy()

    for user_id, name in user_map.items():
        user_id_str = str(user_id)
        if user_id_str in user_id_to_name:
            if user_id_to_name[user_id_str] != name:
                # 名前変更あり → 更新
                for row in updated_sheet1_data:
                    if row[1] == user_id_str:
                        row[0] = name
                        break
        else:
            # 新規ユーザー → 追加
            updated_sheet1_data.append([name, user_id_str])

    sheet1.clear()
    sheet1.update(updated_sheet1_data, 'A1')

    # --- ログデータ更新（シート2） ---
    existing_data2 = sheet2.get_all_values()[1:]  # ヘッダー除く
    header2 = sheet2.row_values(1)
    sheet2.update([header2] + existing_data2 + record_data, 'A1')

    # 処理完了通知
    await ctx.send("✅ データ処理が完了しました！")

@bot.command()
async def calculate(ctx, start_date: str, end_date: str):
    await ctx.send("💰 calculate 処理を開始します...")

    # シートの準備
    sheet3.clear()
    sheet3.update([['名前', '金額']],'A1')
    sheet2_data = sheet2.get_all_values()[1:]  # ヘッダー除外

    # 日付範囲をパース
    start = datetime.strptime(start_date, "%Y/%m/%d").replace(tzinfo=JST)
    end = datetime.strptime(end_date, "%Y/%m/%d").replace(tzinfo=JST)

    reward_table = {
        "コンビニ強盗": 1000000,
        "フリーカ強盗": 1000000,
        "モーテル強盗": 1000000
    }

    reward_data = {}

    for row in sheet2_data:
        try:
            time_str, crime_type, category, participants_str, result = row
        except ValueError:
            continue  # 行に不備がある場合はスキップ

        timestamp = datetime.strptime(time_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
        if not (start <= timestamp <= end):
            continue

        if category != "小型強盗":
            continue

        participants = participants_str.split(",")
        for name in participants:
            if result == "⭕":
                reward = reward_table.get(crime_type, 500000)
            else:
                reward = 500000
            reward_data[name] = reward_data.get(name, 0) + reward

    output_rows = [[name, amount] for name, amount in reward_data.items()]
    if output_rows:
        sheet3.append_rows(output_rows)
        for name, amount in reward_data.items():
            await ctx.send(f"{name}：{amount:,} 円")
    else:
        await ctx.send("⚠️ 対象期間内に該当する小型強盗データがありません。")

    await ctx.send("✅ calculate 処理が完了しました！")

@bot.command()
async def count(ctx, *args):
    await ctx.send("📊 count 処理を開始します...")

    # 引数の解析
    if len(args) == 2:
        user_filter = None
        start_date, end_date = args
    elif len(args) == 3:
        user_filter = args[0]
        start_date, end_date = args[1], args[2]
    else:
        await ctx.send("⚠️ 引数の形式が正しくありません。\n`!count [user_id] yyyy/mm/dd yyyy/mm/dd` の形式で入力してください。")
        return

    # シート1からユーザーID→表示名のマップを作成
    user_id_map = {row[1]: row[0] for row in sheet1.get_all_values()[1:]}

    # シート4をクリア
    sheet4.clear()
    sheet4.update([["名前", "小型対応件数", "小型検挙数", "中型以上対応件数", "中型以上検挙数", "チケット枚数"]], 'A1')

    # シート2取得
    rows = sheet2.get_all_values()[1:]  # ヘッダー除く
    start = datetime.strptime(start_date, "%Y/%m/%d").replace(tzinfo=JST)
    end = datetime.strptime(end_date, "%Y/%m/%d").replace(tzinfo=JST) + timedelta(days=1) - timedelta(minutes=1)

    stats = {}

    for row in rows:
        try:
            time_str, crime_type, category, participants_str, result = row
        except ValueError:
            continue

        timestamp = datetime.strptime(time_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
        if not (start <= timestamp <= end):
            continue

        participants = participants_str.split(",")

        for user_id in participants:
            if user_filter and user_id != user_filter:
                continue

            if user_id not in stats:
                stats[user_id] = {"s_count": 0, "s_win": 0, "m_count": 0, "m_win": 0}

            if category == "小型強盗":
                stats[user_id]["s_count"] += 1
                if result == "⭕":
                    stats[user_id]["s_win"] += 1
            else:
                stats[user_id]["m_count"] += 1
                if result == "⭕":
                    stats[user_id]["m_win"] += 1

    output_rows = []
    for user_id, data in stats.items():
        s_count = data["s_count"]
        s_win = data["s_win"]
        m_count = data["m_count"]
        m_win = data["m_win"]
        tickets = 0  # この時点ではチケットを固定（後のコマンドで加算）

        display_name = user_id_map.get(user_id, user_id)

        output_rows.append([
            display_name,
            s_count,
            s_win,
            m_count,
            m_win,
            tickets
        ])

    if output_rows:
        sheet4.append_rows(output_rows)
        await ctx.send("✅ count 処理が完了しました！")
    else:
        await ctx.send("⚠️ 指定条件に合致する対応データが見つかりません。")

@bot.command()
async def add(ctx):
    await ctx.send("➕ add 処理を開始します...")

    # シート4とシート5を取得
    data4 = sheet4.get_all_values()[1:]  # ヘッダー除外
    data5 = sheet5.get_all_values()
    header = data5[0] if data5 else ['名前', '小型対応件数', '小型検挙数', '中型以上対応件数', '中型以上検挙数', 'チケット枚数']
    existing_rows = data5[1:] if len(data5) > 1 else []

    # 加算前データを保存しておく
    pre_values_dict = {}
    for row in existing_rows:
        name = row[0]
        values = [int(x) if x.isdigit() else 0 for x in row[1:6]]
        pre_values_dict[name] = values

    # 既存データを辞書に変換
    existing_dict = dict(pre_values_dict)

    # シート4から加算
    for row in data4:
        name = row[0]
        values = [int(x) if x.isdigit() else 0 for x in row[1:6]]

        if name in existing_dict:
            existing_dict[name] = [a + b for a, b in zip(existing_dict[name], values)]
        else:
            existing_dict[name] = values

    # 出力用リストを整形
    updated_rows = []
    for name, values in existing_dict.items():
        updated_rows.append([name] + values)

    # シート5を更新
    sheet5.clear()
    sheet5.update([header] + updated_rows, 'A1')

    # 結果出力用メッセージ生成
    messages = []
    for name, values in existing_dict.items():
        s_count, s_win, m_count, m_win, _ = values
        msg = f"◯ {name}\n　小型：対応{s_count}件 / 検挙{s_win}件\n　中型以上：対応{m_count}件 / 検挙{m_win}件"
        messages.append(msg)

    # 初めて基準を超えた人だけ抽出
    over_s = []
    over_m = []

    for name, values in existing_dict.items():
        s_win = values[1]
        m_win = values[3]
        s_win_prev = pre_values_dict.get(name, [0, 0, 0, 0])[1]
        m_win_prev = pre_values_dict.get(name, [0, 0, 0, 0])[3]

        if s_win >= 100 and s_win_prev < 100:
            over_s.append(name)
        if m_win >= 50 and m_win_prev < 50:
            over_m.append(name)

    # メッセージ分割送信
    chunk = ""
    for msg in messages:
        if len(chunk) + len(msg) + 1 > 1900:
            await ctx.send(chunk)
            chunk = ""
        chunk += msg + "\n"
    if chunk:
        await ctx.send(chunk)

    # 通知送信（今回初めて基準を超えた人に限定）
    for name in over_s:
        await ctx.send(f"🎉 {name} の小型犯罪検挙数が100件を超えました！")
    for name in over_m:
        await ctx.send(f"🎉 {name} の中型以上犯罪検挙数が50件を超えました！")

    await ctx.send("✅ add 処理が完了しました。")

@bot.command()
async def get_ticket(ctx):
    await ctx.send("🎫 チケット付与処理を開始します...")

    data4 = sheet4.get_all_values()[1:]  # ヘッダー除外

    if not data4:
        await ctx.send("⚠️ シート4にデータがありません。")
        return

    updated_rows = [['名前', '小型対応件数', '小型検挙数', '中型以上対応件数', '中型以上検挙数', 'チケット枚数']]
    ticket_messages = []

    # シート5の既存データ取得
    data5 = sheet5.get_all_values()[1:]  # ヘッダー除く
    sheet5_dict = {}
    for row in data5:
        if len(row) < 6:
            continue
        name = row[0]
        tickets = int(str(row[5])) if str(row[5]).isdigit() else 0
        sheet5_dict[name] = row[:5] + [tickets]  # 名前～検挙数＋チケット数

    for row in data4:
        name = row[0]
        s_count = int(str(row[1])) if str(row[1]).isdigit() else 0
        s_win   = int(str(row[2])) if str(row[2]).isdigit() else 0
        m_count = int(str(row[3])) if str(row[3]).isdigit() else 0
        m_win   = int(str(row[4])) if str(row[4]).isdigit() else 0

        ticket = s_count // 20 + m_count // 10
        updated_rows.append([name, s_count, s_win, m_count, m_win, ticket])

        if ticket > 0:
            ticket_messages.append(f"{name}：{ticket}枚")

        # シート5へチケット加算
        if name in sheet5_dict:
            prev_tickets = int(str(sheet5_dict[name][5])) if str(sheet5_dict[name][5]).isdigit() else 0
            new_tickets = prev_tickets + ticket
            sheet5_dict[name][5] = str(new_tickets)
        else:
            # 新規ユーザーは0件データ＋チケット数だけセット（対応数等は0で埋める）
            sheet5_dict[name] = [name, '0', '0', '0', '0', str(ticket)]

    # シート5更新用データ準備
    output_sheet5 = [['名前', '小型対応件数', '小型検挙数', '中型以上対応件数', '中型以上検挙数', 'チケット枚数']]
    for v in sheet5_dict.values():
        if len(v) < 6:
            v += ['0'] * (6 - len(v))
        output_sheet5.append(v)

    # シート4を更新
    sheet4.clear()
    sheet4.update(updated_rows, 'A1')

    # シート5を更新
    sheet5.clear()
    sheet5.update(output_sheet5, 'A1')

    # 出力（1人ずつ）
    if not ticket_messages:
        await ctx.send("⚠️ チケットを獲得したユーザーはいません。")
    else:
        for msg in ticket_messages:
            await ctx.send(msg)

    await ctx.send("✅ チケット処理が完了しました。")

@bot.command()
async def rate(ctx, start_date: str, end_date: str):
    await ctx.send("📊 勝率集計を開始します…")

    try:
        start_dt = datetime.strptime(start_date, "%Y/%m/%d")
        end_dt = datetime.strptime(end_date, "%Y/%m/%d")
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt
    except Exception:
        await ctx.send("⚠️ 日付の形式が正しくありません。例: 2025/06/01")
        return

    data = sheet2.get_all_values()
    if len(data) < 2:
        await ctx.send("⚠️ シート2にデータがありません。")
        return

    header = data[0]
    rows = data[1:]

    # 集計用辞書 {crime_name: {"category": ..., "total": ..., "win": ...}}
    rate_data = {}

    # 日付の範囲判定用に、メッセージ日時をdatetimeに変換しながら集計
    for row in rows:
        try:
            dt = datetime.strptime(row[0], "%Y/%m/%d %H:%M")
        except Exception:
            continue

        if not (start_dt <= dt <= end_dt):
            continue

        crime_name = row[1]
        crime_cat = row[2]
        arrest = row[4]  # ⭕ or ❌

        if crime_name not in rate_data:
            rate_data[crime_name] = {"category": crime_cat, "total": 0, "win": 0}
        rate_data[crime_name]["total"] += 1
        if arrest == "⭕":
            rate_data[crime_name]["win"] += 1

    if not rate_data:
        await ctx.send("指定期間に該当するデータがありません。")
        return

    # カテゴリごとに整理
    categories = {
        "小型強盗": [],
        "中型強盗": [],
        "大型強盗": []
    }
    for crime, stats in rate_data.items():
        categories.get(stats["category"], []).append(
            (crime, stats["win"], stats["total"])
        )

    # メッセージ作成
    lines = []
    for cat_name in ["小型強盗", "中型強盗", "大型強盗"]:
        crimes = categories.get(cat_name, [])
        if not crimes:
            continue
        lines.append(f"■ {cat_name}")
        for crime, win, total in crimes:
            rate = (win / total) * 100 if total > 0 else 0
            lines.append(f"・{crime} : {rate:.1f}% ({total}件中 {win}件成功)")
        lines.append("")

    # 2000文字制限を考慮して分割送信
    message = ""
    for line in lines:
        if len(message) + len(line) + 1 > 1900:
            await ctx.send(message)
            message = ""
        message += line + "\n"
    if message:
        await ctx.send(message)

    await ctx.send("✅ 勝率集計が完了しました。")

bot.run(os.getenv("DISCORD_TOKEN"))