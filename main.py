import discord
from discord.ext import commands
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import jaconv
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む（トークンなど）
load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# 日本時間（JST）を定義
JST = timezone(timedelta(hours=9))

# 犯罪名のパターン（部分一致）
CRIME_ALIASES = {
    "コンビニ強盗": ["コンビニ"],
    "フリーカ強盗": ["フリーカ"],
    "モーテル強盗": ["モーテル"],
}

# 犯罪ごとの報酬額
CRIME_PAYMENT = {
    "コンビニ強盗": 200000,
    "フリーカ強盗": 200000,
    "モーテル強盗": 500000,
}
# 失敗時の報酬
FAIL_PAYMENT = 100000

# 犯罪の名前を一致させる
def match_crime_name(text):
    hira_text = jaconv.kata2hira(text)
    for canonical, aliases in CRIME_ALIASES.items():
        for alias in aliases:
            alias_hira = jaconv.kata2hira(alias)
            if alias_hira in hira_text:
                return canonical
    return None

# 統計コマンド
@bot.command()
async def calculate(ctx, start_date: str = None, end_date: str = None):
    try:
        if start_date and end_date:
            start_time = datetime.strptime(start_date, "%Y/%m/%d").replace(tzinfo=JST)
            end_time = datetime.strptime(end_date, "%Y/%m/%d").replace(hour=23, minute=59, tzinfo=JST)
        else:
            end_time = datetime.now(JST)
            start_time = end_time - timedelta(days=7)
    except ValueError:
        await ctx.send("日付の形式が正しくありません。例: `!calculate 2025/05/08 2025/05/11`")
        return

    source_channel_id = 1342103011396288512  # 集計対象チャンネルIDに書き換える
    source_channel = bot.get_channel(source_channel_id)
    output_channel = ctx.channel

    if not source_channel or not output_channel:
        await ctx.send("チャンネルが見つかりません。")
        return

    participant_rewards = defaultdict(int)
    participant_total = defaultdict(int)
    participant_rich = defaultdict(int)

    # DiscordではUTCで時間指定する必要あり
    utc_start_time = start_time.astimezone(timezone.utc)
    utc_end_time = end_time.astimezone(timezone.utc)

    messages = [m async for m in source_channel.history(limit=200, after=utc_start_time, before=utc_end_time)]

    for message in messages:
        text = message.content
        crime_match = re.search(r"犯罪種類\[(.*?)\]", text)
        if not crime_match:
            continue

        crime_raw = crime_match.group(1)
        crime = match_crime_name(crime_raw)
        if not crime:
            continue

        # リアクションの集計
        reaction_types = set()
        reward_users = set()

        for reaction in message.reactions:
            if str(reaction.emoji) in ['⭕️', '❌', '✅']:
                async for user in reaction.users():
                    if not user.bot:
                        if str(reaction.emoji) in ['⭕️', '❌']:
                            reaction_types.add(str(reaction.emoji))
                        if str(reaction.emoji) == '✅':
                            reward_users.add(user.display_name)

        # 勝率計算（⭕️と❌の両方がついている場合は対象外）
        if reaction_types == {'⭕️'}:
            participant_total[crime] += 1
            participant_rich[crime] += 1
        elif reaction_types == {'❌'}:
            participant_total[crime] += 1

        # ✅をつけた人のみに報酬を付与
        for name in reward_users:
            if reaction_types == {'⭕️'}:
                participant_rewards[name] += CRIME_PAYMENT.get(crime, FAIL_PAYMENT)
            else:
                participant_rewards[name] += FAIL_PAYMENT

    # 出力
    await output_channel.send(f"=== 危険手当未受け取りのメンバー ===\n{start_time.strftime('%Y/%m/%d %H:%M')} ～ {end_time.strftime('%Y/%m/%d %H:%M')}")
    for name, reward in participant_rewards.items():
        await output_channel.send(f"・{name} : {reward}円")

    total_reward = sum(participant_rewards.values())
    await output_channel.send(f"\n=== 合計支給額: {total_reward}円 ===")

    await output_channel.send("\n=== 犯罪別勝率統計 ===")
    for crime in sorted(participant_total.keys()):
        total = participant_total[crime]
        rich_count = participant_rich[crime]
        rate = round((rich_count / total) * 100) if total else 0
        await output_channel.send(f"{crime}: {rate}% ({total}件中 {rich_count}件が金持ち検挙)")

# Bot起動
bot.run(os.getenv("DISCORD_TOKEN"))
