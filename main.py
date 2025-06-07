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

def clear_except_header(worksheet):
    num_rows = len(worksheet.get_all_values())
    if num_rows > 1:
        worksheet.batch_clear([f"A2:Z{num_rows}"])

def clear_target_sheets_except_header():
    for ws in [sheet1, sheet2, sheet3, sheet4]:
        clear_except_header(ws)

@bot.command()
async def search(ctx, start_date: str, end_date: str):
    await ctx.send("🔍 データ検索を開始します…")
    clear_target_sheets_except_header()

    channel_ids = [1342103011396288512, 1309142327062954005]
    start = datetime.strptime(start_date, "%Y/%m/%d").replace(tzinfo=JST)
    end = datetime.strptime(end_date, "%Y/%m/%d").replace(tzinfo=JST) + timedelta(days=1)

    # 総メッセージ数の事前カウント
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
    pay_data = {}
    count_data = {}
    rate_data = {}

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

    crime_categories = {
        "小型強盗": {"コンビニ強盗", "フリーカ強盗", "モーテル強盗"},
        "中型強盗": {"客船強盗", "空港強盗", "トレイン強盗", "コンテナ強盗", "ボブキャット強盗","オイルリグ強盗"},
        "大型強盗": {"アーティファクト強盗", "ユニオン強盗"},
    }

    reward_table = {
        "コンビニ強盗": 200000,
        "フリーカ強盗": 200000,
        "モーテル強盗": 500000
    }

    def extract_crime_type(content):
        for official_name, aliases in CRIME_ALIASES.items():
            if any(alias in content for alias in aliases):
                return official_name
        return None

    processed = 0
    progress_update_interval = max(total_messages // 10, 1)

    for channel_id in channel_ids:
        channel = bot.get_channel(channel_id)
        if not channel:
            continue

        async for message in channel.history(after=start, before=end, limit=None):
            processed += 1
            if processed % progress_update_interval == 0 or processed == total_messages:
                percent = int(processed / total_messages * 100)
                await ctx.send(f"🔄 処理中... {percent}% 完了 ({processed}/{total_messages})")

            await asyncio.sleep(0.2)

            crime_type = extract_crime_type(message.content)
            if not crime_type:
                continue

            timestamp = get_timestamp_jst(message)
            participants = []
            results = set()

            guild = ctx.guild
            for reaction in message.reactions:
                if reaction.emoji in ["✅", "⭕", "❌"]:
                    async for user in reaction.users(limit=None):
                        await asyncio.sleep(0.1)
                        if user.bot:
                            continue
                        try:
                            member = guild.get_member(user.id)
                            if member is None:
                                member = await guild.fetch_member(user.id)
                            display_name = member.display_name
                        except discord.NotFound:
                            display_name = user.name
                        user_map[user.id] = display_name
                        if reaction.emoji == "✅":
                            participants.append(user.id)
                        elif reaction.emoji in ["⭕", "❌"]:
                            results.add(reaction.emoji)

            if len(results) != 1:
                continue
            result = list(results)[0]

            category = next((k for k, v in crime_categories.items() if crime_type in v), "")
            names = [user_map.get(uid, str(uid)) for uid in participants]
            record_data.append([timestamp, crime_type, category, ",".join(names), result])

            if category == "小型強盗":
                for uid in participants:
                    reward = reward_table.get(crime_type, 0) if result == "⭕" else 100000
                    pay_data[uid] = pay_data.get(uid, 0) + reward

            for uid in participants:
                if uid not in count_data:
                    count_data[uid] = {
                        "name": user_map.get(uid, str(uid)),
                        "s_count": 0, "s_win": 0,
                        "m_count": 0, "m_win": 0
                    }
                if category == "小型強盗":
                    count_data[uid]["s_count"] += 1
                    if result == "⭕":
                        count_data[uid]["s_win"] += 1
                else:
                    count_data[uid]["m_count"] += 1
                    if result == "⭕":
                        count_data[uid]["m_win"] += 1

            if crime_type not in rate_data:
                rate_data[crime_type] = {"category": category, "total": 0, "win": 0}
            rate_data[crime_type]["total"] += 1
            if result == "⭕":
                rate_data[crime_type]["win"] += 1

    # Google Sheets 出力
    sheet1.update([['ディスプレイネーム', 'ユーザーID']] + [[v, k] for k, v in user_map.items()])
    sheet2.update([['日付', '犯罪の種類', '犯罪カテゴリ', '参加者', '検挙']] + record_data)
    sheet3.update([['名前', '金額']] + [[user_map.get(uid, str(uid)), amount] for uid, amount in pay_data.items()])

    sheet4_data = [['名前', '小型対応件数', '小型検挙数', '中型以上対応件数', '中型以上検挙数', 'チケット枚数']]
    for uid, data in count_data.items():
        ticket = data['s_count'] // 20 + data['m_count'] // 10
        sheet4_data.append([data['name'], data['s_count'], data['s_win'], data['m_count'], data['m_win'], ticket])
    sheet4.update(sheet4_data)

    period = f"{start.strftime('%Y/%m/%d')}~{(end - timedelta(days=1)).strftime('%Y/%m/%d')}"
    sheet6_data = [['期間', '犯罪の種類', '犯罪カテゴリ', '事件数', '検挙数', '勝率']]
    for crime, stats in rate_data.items():
        win_rate = round((stats['win'] / stats['total']) * 100, 1) if stats['total'] > 0 else 0
        sheet6_data.append([period, crime, stats['category'], stats['total'], stats['win'], f"{win_rate}%"])
    sheet6.append_rows(sheet6_data[1:])

    await ctx.send("✅ データ処理が完了しました！")

@bot.command()
async def calculate(ctx):
    """シート3の報酬一覧をDiscordに出力する。"""
    await ctx.send("✅ 報酬の集計を開始します...")

    data = sheet3.get_all_values()
    if len(data) <= 1:
        await ctx.send("⚠️ シート3にデータがありません。")
        return

    rows = data[1:]
    for row in rows:
        if len(row) < 2:
            continue
        name, amount = row[0], row[1]
        await ctx.send(f"{name}：{amount}円")

    await ctx.send("✅ 報酬の出力が完了しました。")

@bot.command()
async def count(ctx):
    """シート4の対応件数をDiscordにまとめて1メッセージで出力する"""
    await ctx.send("✅ 対応件数の集計を開始します...")

    sh = client.open_by_key("11MGwxoUOk2esCf-akJo2d3kSlLhV1rXsTwphuPn-HGI")
    worksheet = sh.worksheet("シート4")

    data = worksheet.get_all_values()
    if len(data) <= 1:
        await ctx.send("⚠️ シート4にデータがありません。")
        return

    rows = data[1:]  # ヘッダーを除く

    msg_list = []
    for row in rows:
        if len(row) < 6:
            continue

        name = row[0]
        small_count = row[1]
        medium_large_count = row[3]
        ticket_count = row[5]

        # 件数が空の場合は0にする
        try:
            small_count_int = int(small_count)
        except:
            small_count_int = 0
        try:
            medium_large_count_int = int(medium_large_count)
        except:
            medium_large_count_int = 0
        try:
            ticket_count_int = int(ticket_count)
        except:
            ticket_count_int = 0

        msg_list.append(
            f"◯ {name}\n　小型：{small_count_int}件\n　中型以上：{medium_large_count_int}件\n　チケット：{ticket_count_int}枚"
        )

    # 2000文字を超える場合は分割して送信
    MAX_LEN = 2000
    chunk = ""
    for line in msg_list:
        if len(chunk) + len(line) + 1 > MAX_LEN:
            await ctx.send(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await ctx.send(chunk)

    await ctx.send("✅ 対応件数の出力が完了しました。")

@bot.command(name="add")
async def add_count(ctx):
    await ctx.send("📊 加算処理を開始します...")  # 処理開始のメッセージ

    sh = client.open_by_key("11MGwxoUOk2esCf-akJo2d3kSlLhV1rXsTwphuPn-HGI")
    sheet4 = sh.worksheet("シート4")
    sheet5 = sh.worksheet("シート5")

    existing_data = sheet5.get_all_values()
    if len(existing_data) == 0:
        await ctx.send("⚠️ シート5にデータがありません。")
        return
    header = existing_data[0]
    rows = existing_data[1:]

    existing_dict = {}
    for row in rows:
        if len(row) < 6:
            continue
        name = row[0]
        small_count = int(row[1]) if row[1].isdigit() else 0
        small_arrest = int(row[2]) if row[2].isdigit() else 0
        midplus_count = int(row[3]) if row[3].isdigit() else 0
        midplus_arrest = int(row[4]) if row[4].isdigit() else 0
        tickets = int(row[5]) if row[5].isdigit() else 0
        existing_dict[name] = [small_count, small_arrest, midplus_count, midplus_arrest, tickets]

    new_data = sheet4.get_all_values()[1:]
    for row in new_data:
        if len(row) < 6:
            continue
        name = row[0]
        small_count = int(row[1]) if row[1].isdigit() else 0
        small_arrest = int(row[2]) if row[2].isdigit() else 0
        midplus_count = int(row[3]) if row[3].isdigit() else 0
        midplus_arrest = int(row[4]) if row[4].isdigit() else 0
        tickets = int(row[5]) if row[5].isdigit() else 0

        if name in existing_dict:
            existing_dict[name][0] += small_count
            existing_dict[name][1] += small_arrest
            existing_dict[name][2] += midplus_count
            existing_dict[name][3] += midplus_arrest
            existing_dict[name][4] += tickets
        else:
            existing_dict[name] = [small_count, small_arrest, midplus_count, midplus_arrest, tickets]

    output_data = []
    existing_names = [row[0] for row in rows]

    for name in existing_names:
        counts = existing_dict.get(name, [0,0,0,0,0])
        output_data.append([name] + counts)

    new_names = [n for n in existing_dict.keys() if n not in existing_names]
    for name in new_names:
        counts = existing_dict[name]
        output_data.append([name] + counts)

    sheet5.update(f'A2', output_data)

    msg_lines = []
    for name, counts in existing_dict.items():
        small = counts[0]
        midplus = counts[2]
        tickets = counts[4]
        msg_lines.append(f"◯ {name}\n　小型：{small}件\n　中型以上：{midplus}件\n　チケット：{tickets}枚")

    MAX_LEN = 2000
    chunk = ""
    for line in msg_lines:
        if len(chunk) + len(line) + 1 > MAX_LEN:
            await ctx.send(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await ctx.send(chunk)

    await ctx.send("✅ シート5への加算処理が完了しました。")

@bot.command()
async def get_ticket(ctx):
    await ctx.send("🔍 チケット獲得者の確認を開始します…")

    sh = client.open_by_key("11MGwxoUOk2esCf-akJo2d3kSlLhV1rXsTwphuPn-HGI")
    sheet4 = sh.worksheet("シート4")
    data = sheet4.get_all_values()

    if len(data) <= 1:
        await ctx.send("データがありません。")
        await ctx.send("✅ 処理を終了しました。")
        return

    rows = data[1:]
    has_ticket = False
    for row in rows:
        if len(row) < 6:
            continue
        name = row[0]
        try:
            tickets = int(row[5])
        except ValueError:
            tickets = 0

        if tickets > 0:
            has_ticket = True
            await ctx.send(f"{name}：{tickets}枚")

    if not has_ticket:
        await ctx.send("チケットを獲得した人はいません。")

    await ctx.send("✅ 処理を終了しました。")

@bot.command()
async def have_ticket(ctx):
    await ctx.send("🎫 チケット所持者の確認を開始します…")

    sh = client.open_by_key("11MGwxoUOk2esCf-akJo2d3kSlLhV1rXsTwphuPn-HGI")
    sheet5 = sh.worksheet("シート5")
    data = sheet5.get_all_values()

    if len(data) <= 1:
        await ctx.send("データがありません。")
        await ctx.send("✅ 処理を終了しました。")
        return

    rows = data[1:]
    result = []
    for row in rows:
        if len(row) < 6:
            continue
        name = row[0]
        try:
            tickets = int(row[5])
        except ValueError:
            tickets = 0

        if tickets > 0:
            result.append(f"{name}：{tickets}枚")

    if not result:
        await ctx.send("チケットを所持している人はいません。")
    else:
        await ctx.send("\n".join(result))

    await ctx.send("✅ 処理を終了しました。")

@bot.command()
async def use_ticket(ctx, user_id: str, num: int):
    await ctx.send(f"🛠️ ユーザーID {user_id} のチケットを {num} 枚減算しようとしています…")

    sh = gc.open_by_key("シート5")
    sheet5 = sh.worksheet("シート5")
    data = sheet5.get_all_values()
    header = data[0]
    rows = data[1:]

    user_id_index = 1
    ticket_index = 5

    row_number = None
    current_tickets = 0
    for i, row in enumerate(rows, start=2):
        if len(row) <= max(user_id_index, ticket_index):
            continue
        if row[user_id_index] == user_id:
            row_number = i
            try:
                current_tickets = int(row[ticket_index])
            except ValueError:
                current_tickets = 0
            break

    if row_number is None:
        await ctx.send(f"ユーザーID {user_id} は見つかりません。")
        await ctx.send("❌ 処理を中止しました。")
        return

    if current_tickets < num:
        await ctx.send(f"ユーザーID {user_id} のチケットは {current_tickets} 枚しかありません。減算できません。")
        await ctx.send("❌ 処理を中止しました。")
        return

    new_tickets = current_tickets - num
    sheet5.update_cell(row_number, ticket_index + 1, str(new_tickets))

    await ctx.send(f"✅ ユーザーID {user_id} のチケットを {num} 枚減算しました。残り {new_tickets} 枚です。")
    await ctx.send("✅ 処理を終了しました。")

@bot.command()
async def rate(ctx, start_date: str, end_date: str):
    data = sheet6.get_all_values("シート6")  # ← ここで sheet6 を直接使えばOK
    header = data[0]
    rows = data[1:]

    # 日付の範囲をdatetime.dateに変換
    start_dt = datetime.strptime(start_date, "%Y/%m/%d").date()
    end_dt = datetime.strptime(end_date, "%Y/%m/%d").date()

    # 逆転していたら入れ替え
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    results = []

    period_index = 0
    crime_type_index = 1
    crime_category_index = 2
    case_count_index = 3
    success_count_index = 4
    rate_index = 5

    for row in rows:
        period_str = row[period_index]
        try:
            period_dt = datetime.strptime(period_str, "%Y/%m/%d").date()
        except Exception:
            continue

        if start_dt <= period_dt <= end_dt:
            crime_name = row[crime_type_index]
            crime_cat = row[crime_category_index]
            case_num = row[case_count_index]
            success_num = row[success_count_index]
            win_rate = row[rate_index]

            results.append(f"{crime_name} ({crime_cat}): 勝率 {win_rate}% (事件数: {case_num}, 検挙数: {success_num})")

    if not results:
        await ctx.send("指定期間に該当する勝率データはありません。")
        return

    # 送信メッセージを分割（2000文字制限対策）
    message = ""
    for line in results:
        if len(message) + len(line) + 1 > 1900:
            await ctx.send(message)
            message = ""
        message += line + "\n"
    if message:
        await ctx.send(message)

# Botトークンは環境変数から取得（安全のため）
bot.run(os.getenv("DISCORD_TOKEN"))