import discord
from discord.ext import commands
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import pytz
from discord.utils import get  # ←これを追加
import asyncio
import os

PD_ROLE_ID = 1231945357936689182

# ====== Discord Bot Setup ======
intents = discord.Intents.default()
intents.messages = True
intents.reactions = True
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ====== Google Sheets Setup ======
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

# ====== 定数 ======
PD_ROLE_ID = 1231945357936689182
GANG_COLORS = ["⚪","⚫","🔴","🔵","🟣","🟡","🟠","🩷"]

CRIME_CATEGORIES = {
    "小型強盗": ["コンビニ", "フリーカ", "ブースティング", "ATM", "空き巣", "モーテル"],
    "中型強盗": ["ボブキャット", "空港", "コンテナ", "トレイン", "列車", "客船", "オイルリグ"],
    "大型強盗": ["ユニオン", "アーティファクト"]
}

JST = pytz.timezone("Asia/Tokyo")

# ====== ユーティリティ ======
def get_timestamp_jst(message):
    return message.created_at.replace(tzinfo=pytz.UTC).astimezone(JST)

def get_category(crime_name):
    for category, keywords in CRIME_CATEGORIES.items():
        if any(kw in crime_name for kw in keywords):
            return category
    return "不明"

# === 起動時処理 ===
@bot.event
async def on_ready():
    print("🔄 Bot 起動完了。シート1更新を開始します...")

    # サーバーIDを指定して取得
    GUILD_ID = 1230531293150707723
    guild = get(bot.guilds, id=GUILD_ID)
    if not guild:
        print(f"⚠️ ギルド {GUILD_ID} が見つかりません。")
        return

    # シート1の既存データを取得
    sheet1_values = sheet1.get_all_values()
    header = sheet1_values[0] if sheet1_values else ['ディスプレイネーム', 'ユーザーID', 'PDロール']
    existing_rows = sheet1_values[1:] if len(sheet1_values) > 1 else []

    # ID をキーに行を辞書化
    user_id_to_row = {row[1]: row for row in existing_rows}

    updated_rows = []

    for user_id_str in sorted(user_id_to_row.keys(), key=int):  # ID順
        try:
            member = await guild.fetch_member(int(user_id_str))
            display_name = member.display_name
            has_role = "⭕" if discord.utils.get(member.roles, id=1231945357936689182) else "❌"
        except discord.NotFound:
            display_name = user_id_to_row[user_id_str][0]
            has_role = "❌"

        # 更新行を作成
        updated_rows.append([display_name, user_id_str, has_role])

    # シート1を更新
    sheet1.clear()
    sheet1.update([header] + updated_rows, range_name='A1')

    print("✅ シート1更新完了。準備完了")

# ===== !search =====
@bot.command()
async def search(ctx, start_date: str, end_date: str):
    await ctx.send("🔎 `!search` を開始します...")

    # JSTでタイムゾーンを設定
    start_dt = JST.localize(datetime.strptime(start_date + " 00:00", "%Y/%m/%d %H:%M"))
    end_dt = JST.localize(datetime.strptime(end_date + " 23:59", "%Y/%m/%d %H:%M"))

    # --- シート1の既存データ取得 ---
    sheet1_values = sheet1.get_all_values()
    header1 = sheet1_values[0] if sheet1_values else ['ディスプレイネーム', 'ID', 'PDロール']
    existing_users = sheet1_values[1:] if len(sheet1_values) > 1 else []
    user_id_to_row = {row[1]: row for row in existing_users}  # IDをキーに行を取得

    # --- 犯罪名マッピング ---
    CRIME_MAP = {
        "コンビニ": "コンビニ強盗",
        "フリーカ": "フリーカ強盗",
        "ブースティング": "ブースティング強盗",
        "ATM": "ATM強盗",
        "空き巣": "空き巣強盗",
        "モーテル": "モーテル強盗",
        "ボブキャット": "ボブキャット強盗",
        "空港": "空港強盗",
        "コンテナ": "コンテナ強盗",
        "トレイン": "トレイン強盗",
        "列車": "トレイン強盗",
        "客船": "客船強盗",
        "オイルリグ": "オイルリグ強盗",
        "ユニオン": "ユニオン強盗",
        "アーティファクト": "アーティファクト強盗",
    }

    for channel_id in [ctx.channel.id, 1342103011396288512, 1309142327062954005]:
        channel = bot.get_channel(channel_id)
        if not channel:
            continue

        async for message in channel.history(after=start_dt, before=end_dt, oldest_first=True):
            crime_name = None
            for key, full_name in CRIME_MAP.items():
                if key in message.content:
                    crime_name = full_name
                    break
            if not crime_name:
                continue

            category = get_category(crime_name)  # カテゴリ取得関数
            timestamp = get_timestamp_jst(message).strftime("%Y/%m/%d %H:%M")

            participants, arrest, colors = [], "", []
            for reaction in message.reactions:  # ← 修正箇所
                async for user in reaction.users():
                    if user.bot:
                        continue
                    emoji = str(reaction.emoji)

                    if emoji == "✅":
                        participants.append(str(user.id))
                        # シート1に存在しなければ追加、名前更新も
                        user_id_str = str(user.id)
                        pd_role = "⭕" if discord.utils.get(user.roles, id=PD_ROLE_ID) else "❌"
                        if user_id_str not in user_id_to_row:
                            user_id_to_row[user_id_str] = [user.display_name, user_id_str, pd_role]
                        else:
                            if user_id_to_row[user_id_str][0] != user.display_name:
                                user_id_to_row[user_id_str][0] = user.display_name
                    elif emoji in ["⭕","❌"]:
                        arrest = emoji
                    elif emoji in GANG_COLORS and emoji not in colors:
                        colors.append(emoji)

            participants_str = ",".join(participants)
            colors_str = ",".join(colors)
            sheet2.append_row([timestamp, crime_name, category, participants_str, arrest, colors_str])

    # --- シート1をまとめて更新 ---
    updated_rows = [header1] + list(user_id_to_row.values())
    sheet1.clear()
    sheet1.update(values=updated_rows)

    await ctx.send("✅ `!search` が完了しました！")

# ===== !count =====
@bot.command()
async def count(ctx, *args):
    await ctx.send("📊 count 処理を開始します...")

    # 引数処理
    if len(args) == 2:
        user_filter = None
        start_date, end_date = args
    elif len(args) == 3:
        user_filter = args[0]
        start_date, end_date = args[1], args[2]
    else:
        await ctx.send("⚠️ 引数の形式が正しくありません。\n`!count [user_id] yyyy/mm/dd yyyy/mm/dd` の形式で入力してください。")
        return

    # ユーザーID → 名前のマップ
    sheet1_values = sheet1.get_all_values()[1:]  # ヘッダー除外
    user_id_map = {row[1]: row[0] for row in sheet1_values if len(row) > 1}

    # シート4を初期化
    sheet4.clear()
    sheet4.update([["名前", "ID", "小型対応件数", "小型検挙数", "中型以上対応件数", "中型以上検挙数", "チケット枚数"]], 'A1')

    # 集計対象データ取得
    rows = sheet2.get_all_values()[1:]  # ヘッダー除外
    start = datetime.strptime(start_date, "%Y/%m/%d").replace(tzinfo=JST)
    end = datetime.strptime(end_date, "%Y/%m/%d").replace(tzinfo=JST) + timedelta(days=1) - timedelta(minutes=1)

    stats = {}

    for row in rows:
        if len(row) < 5:
            continue  # 不正行スキップ

        time_str, crime_type, category, participants_str, result = row[:5]

        timestamp = datetime.strptime(time_str, "%Y/%m/%d %H:%M").replace(tzinfo=JST)
        if not (start <= timestamp <= end):
            continue

        participants = participants_str.split(",") if participants_str else []

        for user_id in participants:
            if user_filter and user_id != user_filter:
                continue

            if user_id not in stats:
                stats[user_id] = {"s_count": 0, "s_win": 0, "m_count": 0, "m_win": 0, "tickets": 0}

            if category == "小型強盗":
                stats[user_id]["s_count"] += 1
                if result == "⭕":
                    stats[user_id]["s_win"] += 1
            else:
                stats[user_id]["m_count"] += 1
                if result == "⭕":
                    stats[user_id]["m_win"] += 1

    # シート4用に整形
    output_rows = []
    for user_id, data in stats.items():
        display_name = user_id_map.get(user_id, user_id)
        output_rows.append([
            display_name,
            user_id,
            data["s_count"],
            data["s_win"],
            data["m_count"],
            data["m_win"],
            data["tickets"]  # 常に存在するので安全
        ])

    if output_rows:
        sheet4.append_rows(output_rows)

    await ctx.send("✅ count 処理が完了しました！")

# ===== !add =====
@bot.command()
async def add(ctx):
    await ctx.send("➕ add 処理を開始します...")

    # --- シート4とシート5のデータ取得 ---
    data4 = sheet4.get_all_values()[1:]  # シート4のデータ
    data5 = sheet5.get_all_values()
    header5 = data5[0] if data5 else ['名前', 'ユーザーID', '小型対応件数', '小型検挙数', '中型以上対応件数', '中型以上検挙数', 'チケット枚数']
    existing_rows = data5[1:] if len(data5) > 1 else []

    # シート5既存データを辞書に
    sheet5_dict = {}
    for row in existing_rows:
        uid = row[1]
        values = [int(x) if x.isdigit() else 0 for x in row[2:6]]
        sheet5_dict[uid] = values

    # 前回検挙数を保持
    prev_win = {uid: [values[1], values[3]] for uid, values in sheet5_dict.items()}

    # シート1の名前・ID対応と⭕判定
    sheet1_rows = sheet1.get_all_values()[1:]
    sheet1_map = {}      # ID -> 名前
    sheet1_ok = set()    # ⭕の人のID
    for r in sheet1_rows:
        name, uid, pd_role = r[0], r[1], r[2]
        sheet1_map[uid] = name
        if pd_role == "⭕":
            sheet1_ok.add(uid)

    # --- シート4のデータを加算 ---
    for row in data4:
        name = row[0]
        uid = row[1]
        values4 = [int(x) if x.isdigit() else 0 for x in row[2:6]]
        if uid in sheet5_dict:
            sheet5_dict[uid] = [a + b for a, b in zip(sheet5_dict[uid], values4)]
        else:
            sheet5_dict[uid] = values4

    # --- シート5更新 ---
    updated_rows = []
    for uid, values in sheet5_dict.items():
        name = sheet1_map.get(uid, "不明")
        updated_rows.append([name, uid] + values + [0])  # チケット列は0で固定

    sheet5.clear()
    sheet5.update([header5] + updated_rows, 'A1')

    # --- Discord出力 ---
    chunk = ""
    max_len = 1900
    over_s, over_m = [], []

    for row in updated_rows:
        name, uid = row[0], row[1]
        s_count, s_win, m_count, m_win = row[2:6]

        # シート1で⭕の人のみ出力
        if uid not in sheet1_ok:
            continue

        msg = f"◯ {name}\n　小型：対応{s_count}件 / 検挙{s_win}件\n　中型以上：対応{m_count}件 / 検挙{m_win}件"

        if len(chunk) + len(msg) + 1 > max_len:
            await ctx.send(chunk)
            chunk = ""
        chunk += msg + "\n"

        prev_s, prev_m = prev_win.get(uid, [0, 0])
        if s_win >= 100 and prev_s < 100:
            over_s.append(name)
        if m_win >= 50 and prev_m < 50:
            over_m.append(name)

    if chunk:
        await ctx.send(chunk)

    for name in over_s:
        await ctx.send(f"🎉 {name} の小型犯罪検挙数が100件を超えました！")
    for name in over_m:
        await ctx.send(f"🎉 {name} の中型以上犯罪検挙数が50件を超えました！")

    await ctx.send("✅ add 処理が完了しました。")

# ===== !rate =====
@bot.command()
async def rate(ctx, start_date: str, end_date: str):
    await ctx.send("📊 rate 処理開始")

    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d") + timedelta(days=1)
    sheet6.resize(1)  # ヘッダーだけ残す

    # カテゴリ分類
    CATEGORY_MAP = {
        "小型強盗": ["コンビニ強盗", "フリーカ強盗", "ブースティング強盗", "ATM強盗", "空き巣強盗", "モーテル強盗"],
        "中型強盗": ["ボブキャット強盗", "空港強盗", "コンテナ強盗", "トレイン強盗", "客船強盗", "オイルリグ強盗"],
        "大型強盗": ["ユニオン強盗", "アーティファクト強盗"]
    }

    colors = GANG_COLORS
    crime_dict = {}

    records = sheet2.get_all_records()
    for r in records:
        record_date_str = r.get("日付", "")
        if not record_date_str:
            continue
        record_date = datetime.strptime(record_date_str, "%Y/%m/%d %H:%M")
        if not (start_dt <= record_date < end_dt):
            continue

        crime = r.get("犯罪の種類", "")
        result = str(r.get("検挙", "")).strip()
        color_list = str(r.get("色", "")).split(",") if r.get("色") else []

        # カテゴリ判定
        category = ""
        for cat, crimes in CATEGORY_MAP.items():
            if crime in crimes:
                category = cat
                break

        if crime not in crime_dict:
            crime_dict[crime] = {
                "カテゴリ": category,
                "対応件数": 0,
                "成功数": 0,
                "色": {c: {"対応件数": 0, "成功数": 0} for c in colors}
            }

        crime_dict[crime]["対応件数"] += 1
        if result == "⭕":
            crime_dict[crime]["成功数"] += 1

        for c in color_list:
            if c in colors:
                crime_dict[crime]["色"][c]["対応件数"] += 1
                if result == "⭕":
                    crime_dict[crime]["色"][c]["成功数"] += 1

    # sheet6に出力
    header = ["犯罪の種類", "カテゴリ", "対応件数"] + colors + ["全体勝率"]
    sheet6.update([header], 'A1')

    for crime, data in crime_dict.items():
        total = data["対応件数"]
        success = data["成功数"]
        row = [crime, data["カテゴリ"], total]

        for c in colors:
            color_total = data["色"][c]["対応件数"]
            color_success = data["色"][c]["成功数"]
            win_rate = round(color_success / color_total * 100, 2) if color_total > 0 else 0
            row.append(win_rate)

        overall_rate = round(success / total * 100, 2) if total > 0 else 0
        row.append(overall_rate)

        sheet6.append_row(row)

    await ctx.send("✅ rate 処理終了")

# ===== !calculate =====
@bot.command()
async def calculate(ctx, start_date: str, end_date: str):
    await ctx.send("💰 calculate 処理を開始します...")

    # シート3初期化
    sheet3.clear()
    sheet3.update([['名前', '金額']], 'A1')

    # シート2データ取得
    sheet2_data = sheet2.get_all_values()[1:]  # ヘッダー除外
    sheet1_data = sheet1.get_all_values()[1:]
    user_id_map = {str(row[1]): row[0] for row in sheet1_data}  # ID -> 名前

    # 日付解析
    try:
        start = datetime.strptime(start_date, "%Y/%m/%d")
        end = datetime.strptime(end_date, "%Y/%m/%d")
    except ValueError:
        await ctx.send("⚠️ 日付形式が正しくありません。例: 2025/06/01")
        return

    # 小型強盗の報酬設定
    reward_table = {
        "コンビニ強盗": 1500000,
        "フリーカ強盗": 1500000,
        "モーテル強盗": 1500000,
        "ブースティング強盗": 1500000,
        "ATM強盗": 1500000
    }
    default_reward = 1000000

    reward_data = {}

    for row in sheet2_data:
        # 列順: 日付｜犯罪の種類｜犯罪のカテゴリ｜参加者のID｜検挙｜ギャングの色
        if len(row) < 5:
            continue
        time_str, crime_type, category, participants_str, result = row[:5]

        # 日付判定（時間無視）
        try:
            timestamp = datetime.strptime(time_str.strip().split()[0], "%Y/%m/%d")
        except:
            continue
        if not (start.date() <= timestamp.date() <= end.date()):
            continue

        # 小型強盗のみ
        if category.strip() != "小型強盗":
            continue

        participants = [uid.strip() for uid in participants_str.split(",") if uid.strip()]
        for user_id in participants:
            reward = reward_table.get(crime_type.strip(), default_reward)
            if result.strip() != "⭕":
                reward = default_reward
            reward_data[user_id] = reward_data.get(user_id, 0) + reward

    # 結果をシート3に記録しDiscord出力
    if reward_data:
        output_rows = []
        for user_id, amount in reward_data.items():
            name = user_id_map.get(user_id, user_id)
            output_rows.append([name, amount])
            await ctx.send(f"{name}：{amount:,} 円")  # 1行ずつ出力

        sheet3.append_rows(output_rows)
        total_reward = sum(reward_data.values())
        await ctx.send(f"💰 総報酬額：{total_reward:,} 円")
    else:
        await ctx.send("⚠️ 対象期間内に該当する小型強盗データがありません。")

    await ctx.send("✅ calculate 処理が完了しました！")

# ===== !clear =====
@bot.command()
@commands.has_permissions(manage_messages=True)
async def clear(ctx, date_str: str = None):
    if date_str is None:
        await ctx.send("⚠️ 全期間のリアクションのないメッセージを削除します...")

        def check(message):
            return len(message.reactions) == 0

        deleted = await ctx.channel.purge(limit=1000, check=check, bulk=False)
        await ctx.send(f"✅ {len(deleted)} 件のリアクションなしメッセージを削除しました。", delete_after=5)

    else:
        try:
            target_date = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=JST)
            next_day = target_date + timedelta(days=1)
        except ValueError:
            await ctx.send("⚠️ 日付の形式が正しくありません。例: `!clear 2025/08/03`")
            return

        await ctx.send(f"⚠️ {date_str} のリアクションのないメッセージを削除します...")

        def check(message):
            created_at_jst = message.created_at.astimezone(JST)
            in_range = target_date <= created_at_jst < next_day
            no_reactions = len(message.reactions) == 0
            return in_range and no_reactions

        deleted = await ctx.channel.purge(limit=1000, check=check, bulk=False)
        await ctx.send(f"✅ {len(deleted)} 件のリアクションなしメッセージを {date_str} に削除しました。", delete_after=5)

# ===== !ticket =====
@bot.command()
async def ticket(ctx):
    await ctx.send("🎟️ ticket処理開始")
    sheet4_records = sheet4.get_all_records()
    updated_rows = []
    for r in sheet4_records:
        small_tickets = r["小型対応件数"]//20
        large_tickets = r["中型以上対応件数"]//10
        total_tickets = small_tickets+large_tickets
        r["チケット枚数"] = total_tickets
        updated_rows.append([r["名前"],r["ID"],r["小型対応件数"],r["小型検挙数"],r["中型以上対応件数"],r["中型以上検挙数"],total_tickets])
        if total_tickets>0:
            await ctx.send(f"{r['名前']}：{total_tickets}枚獲得")
    sheet4.resize(1)
    sheet4.update([["名前","ID","小型対応件数","小型検挙数","中型以上対応件数","中型以上検挙数","チケット枚数"]]+updated_rows)
    await ctx.send("✅ ticket処理終了")

# ====== Bot起動 ======
bot.run(os.getenv("DISCORD_TOKEN"))
