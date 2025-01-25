from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command
import asyncio, sqlite3, os
from random import choice
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.filters.state import StateFilter
from dotenv import load_dotenv
from bot.config import responses, random_responses, negative_actions, positive_actions

env_path = os.path.join(os.getcwd(), "api_token.env")
load_dotenv(dotenv_path=env_path)

API_TOKEN = os.getenv("API_TOKEN")
MY_TELEGRAM_ID = os.getenv("MY_TELEGRAM_ID")

# Database setup
db_conn = sqlite3.connect("users.db")
db_cursor = db_conn.cursor()

db_cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    authority INTEGER,
    crime TEXT
)
""")
db_conn.commit()

# Create bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

class StopGameStates(StatesGroup):
    awaiting_confirmation = State()

# Command: /start
@dp.message(Command("start"))
async def send_welcome(msg: Message):
    if msg.chat.type == "private":
        await msg.answer("Вечер в хату! Смерти мусорскому, ходу воровскому, АУЕ! По понятиям раскидаю здесь /help")
    elif msg.chat.type in {"group", "supergroup"}:
        await msg.reply(
            "Вечер в хату! Смерти мусорскому, ходу воровскому, АУЕ! Поясню за понятия здесь /help",
            reply_to_message_id=msg.message_id
        )

# Command: /help
@dp.message(Command("help"))
async def help_command(msg: Message):
    await msg.reply(
        "Список команд:\n"
        "/start - начать\n"
        "/about - кто я\n"
        "/help - помощь\n"
        "/ktolox - заглянуть под шконку\n"
        "/newgame - загреметь в тюрягу\n"
        "/top - топ по масти\n"
        "/stopgame - дать заднюю\n"
        "/action - совершить действие\n"
        "/status - проверить статус\n",
        reply_to_message_id=msg.message_id
    )

# Command: /newgame
@dp.message(Command("newgame"))
async def new_game(msg: Message):
    db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
    user = db_cursor.fetchone()
    if user:
        await msg.reply("Ты уже в тюряге, лушпайка! Используй /action шоб мутить движ\n"
                        "Или /stopgame шоб дать заднюю, очкошник"
                        )
    else:
        crime = choice(["Сделал дырки в сыре", "Домогался до пенсионерки", "Совращение несовершеннолетних", "Изнасилование женщин", "Изнасилование крупного рогатого скота"])
        db_cursor.execute("INSERT INTO users (user_id, username, authority, crime) VALUES (?, ?, 50, ?)", (msg.from_user.id, msg.from_user.username, crime))
        db_conn.commit()
        await msg.reply(f"Опапа, хата! У нас первоход в хате! {msg.from_user.full_name}! Осужден за {crime}.")

# Команда: /action
@dp.message(Command("action"))
async def action_command(msg: Message):
    # Получаем информацию о пользователе из базы данных
    db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
    user = db_cursor.fetchone()

    # Проверка на наличие пользователя в базе
    if not user:
        await msg.reply("Ты еще на свободе, совершить преступление и узнать статью - /newgame.")
    else:
        # Выбираем действие: положительное или отрицательное
        if choice([True, False]):
            action_description = choice(positive_actions)  # Используем данные из конфигурации
            authority_change = choice([5, 10, 15])  # Увеличиваем авторитет
        else:
            action_description = choice(negative_actions)  # Используем данные из конфигурации
            authority_change = choice([-5, -10, -15])  # Уменьшаем авторитет

        # Рассчитываем новый авторитет
        new_authority = user[2] + authority_change
        if new_authority < 0:
            new_authority = 0  # Не даем авторитету стать меньше 0
            
        # Обновляем авторитет в базе данных
        db_cursor.execute("UPDATE users SET authority = ? WHERE user_id = ?", (new_authority, msg.from_user.id))
        db_conn.commit()

        # Отправляем сообщение о результатах
        change_symbol = "+" if authority_change > 0 else "-"
        await msg.reply(
            f"Твой авторитет {change_symbol}{abs(authority_change)} ({action_description}) и теперь составляет {new_authority}."
        )

@dp.message(Command("ktolox"))
async def kto_lox_command(msg: Message):
    db_cursor.execute("SELECT username, authority FROM users")
    users = db_cursor.fetchall()
    under_skonka = [user for user in users if user[1] < 50]

    if not under_skonka:
        await msg.reply("Никто не попущен, все в порядке с мастью!", reply_to_message_id=msg.message_id)
        return

    min_authority = min(user[1] for user in under_skonka)
    lowest_users = [user[0] for user in under_skonka if user[1] == min_authority]

    if len(lowest_users) == 1:
        await msg.reply(
            f"Под шконкой сейчас находится: {lowest_users[0]}",
            reply_to_message_id=msg.message_id
        )
    else:
        await msg.reply(
            f"Под шконкой сейчас сидят: {', '.join(lowest_users)}",
            reply_to_message_id=msg.message_id
        )

# Command: /about
@dp.message(Command("about"))
async def about_command(msg: Message):
    about_text = (
        "👋 Саламалексус! Я смотрящий за хатой здесь, щенки! Создан с целью попускать вас.\n\n"
        "⚙️ Вот что я умею:\n"
        "- Отвечать за базар.\n"
        "- Хранить данные игроков, начавших игру.\n"
        "- Управлять вашей \"игровой жизнью\" с помощью команд /newgame, /action и /status.\n\n"
        "💡 Команды для взаимодействия:\n"
        "/start - Запуск бота\n"
        "/help - Список доступных команд\n"
        "/newgame - Тест на тьотю шлюху\n"
        "/action - Выполнить случайное действие\n"
        "/status - Узнать ваш статус\n\n"
        "🎯 По тузику не гатимся но в остальном правил нет. \n"
        "Если есть вопросы, ТЕРЯЙСЯ ВАСЬ 😉"
    )

    if msg.chat.type == "private":
        await msg.answer(about_text)
    else:
        await msg.reply(about_text, reply_to_message_id=msg.message_id)

# Команда: /stopgame
@dp.message(Command("stopgame"))
async def stop_game_command(msg: Message, state: FSMContext):
    db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
    user = db_cursor.fetchone()

    if not user:
        await msg.reply("Ты еще не получил срок, молокосос. Используй /newgame для старта.")
    else:
        await state.set_state(StopGameStates.awaiting_confirmation)
        await msg.reply("Ты уерен, Вась? Дождешься отсидки в тишине? Напиши 'да' для подтверждения или 'нет' для отмены.")

@dp.message(StateFilter(StopGameStates.awaiting_confirmation))
async def stop_game_confirmation(msg: Message, state: FSMContext):
    if msg.text.lower() == "да":
        # Получаем данные игрока из базы
        db_cursor.execute("SELECT authority, username FROM users WHERE user_id = ?", (msg.from_user.id,))
        user = db_cursor.fetchone()
        if not user:
            await msg.reply("Ошибка: еблан ты уже на свободе.")
            await state.clear()
            return

        username = user[1] if user[1] else "Игрок без имени"
        authority = user[0]

        # Определяем масть
        status = (
            "Смотрящий" if authority >= 130 else
            "Барон" if authority >= 90 else
            "Блатной" if authority >= 75 else
            "Пацан" if authority >= 60 else
            "Мужик" if authority >= 50 else
            "Хромой" if authority >= 40 else
            "Шестёрка" if authority >= 30 else
            "Газонюх" if authority >= 20 else
            "Водолаз" if authority >= 10 else
            "Петух"
        )

        # Генерируем случайные данные
        gatilsya_times = choice(range(1, 101))
        ending_reason = choice(["умер", "вышел на свободу"])
        death_reasons = [
            "утонул в параше",
            "повесился в камере",
            "вскрылся под шконкой",
            "несчастный случай на промзоне",
            "застрелен при попытке побега",
            "окочурился жмур, забит в драке"
        ]
        death_cause = choice(death_reasons) if ending_reason == "умер" else None

        # Удаляем игрока из базы
        db_cursor.execute("DELETE FROM users WHERE user_id = ?", (msg.from_user.id,))
        db_conn.commit()

        # Формируем сообщение
        result_message = (
            f"Игрок {username} закончил свой тюремный путь.\n\n"
            f"📊 **Статистика:**\n"
            f"Авторитет: {authority}\n"
            f"Масть: {status}\n"
            f"Гатился по тузику: {gatilsya_times} раз.\n\n"
            f"Причина окончания: {ending_reason}"
        )
        if death_cause:
            result_message += f" ({death_cause})"

        await msg.reply(result_message)
        await state.clear()
    elif msg.text.lower() == "нет":
        await msg.reply("Правильный выбор, щинак! Бегом парашу драить!")
        await state.clear()
    else:
        await msg.reply("Уверен? Черкаш, напиши 'да' для подтверждения или 'нет' для отмены.")

# Команда: /top
@dp.message(Command("top"))
async def top_command(msg: Message):
    db_cursor.execute("SELECT user_id, username, authority FROM users ORDER BY authority DESC")
    users = db_cursor.fetchall()

    if not users:
        await msg.reply("В игре нет игроков.", reply_to_message_id=msg.message_id)
        return

    # Формируем список игроков и их статус
    top_list = []
    for user in users:
        user_id, username, authority = user
        status = (
            "Смотрящий" if authority >= 130 else
            "Барон" if authority >= 90 else
            "Блатной" if authority >= 75 else
            "Пацан" if authority >= 60 else
            "Мужик" if authority >= 50 else
            "Хромой" if authority >= 40 else
            "Шестёрка" if authority >= 30 else
            "Газонюх" if authority >= 20 else
            "Водолаз" if authority >= 10 else
            "Петух"
        )
        top_list.append(f"{username} - Авторитет: {authority} - Статус: {status}")

    # Отправляем топ-список
    top_message = "\n".join(top_list)
    await msg.reply(f"Топ игроков по авторитету:\n{top_message}", reply_to_message_id=msg.message_id)  

# Команда: /status
@dp.message(Command("status"))
async def status_command(msg: Message):
    db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
    user = db_cursor.fetchone()
    if not user:
        await msg.reply("Вы ещё не начали игру. Используйте /newgame.")
    else:
        authority = user[2]
        status = (
            "Смотрящий" if authority >= 130 else
            "Барон" if authority >= 90 else
            "Блатной" if authority >= 75 else
            "Пацан" if authority >= 60 else
            "Мужик" if authority >= 50 else
            "Хромой" if authority >= 40 else
            "Шестёрка" if authority >= 30 else
            "Газонюх" if authority >= 20 else
            "Водолаз" if authority >= 10 else
            "Петух"
        )
        await msg.reply(f"Ваш текущий статус:\nАвторитет: {authority}\nСтатус: {status}")

# Handling "кто попущенный"
@dp.message()
async def get_text_messages(msg: Message):
    if msg.text:
        # Обработка фразы "кто попущенный"
        if msg.text.lower().strip() == "кто попущенный":
            db_cursor.execute("SELECT username, authority FROM users")
            users = db_cursor.fetchall()
            under_skonka = [user for user in users if user[1] < 50]

            if not under_skonka:
                await msg.reply("Никто не попущен, все в порядке с мастью!", reply_to_message_id=msg.message_id)
                return

            min_authority = min(user[1] for user in under_skonka)
            lowest_users = [user[0] for user in under_skonka if user[1] == min_authority]

            if len(lowest_users) == 1:
                await msg.reply(
                    f"Под шконкой сейчас находится: {lowest_users[0]}",
                    reply_to_message_id=msg.message_id
                )
            else:
                await msg.reply(
                    f"Под шконкой сейчас сидят: {', '.join(lowest_users)}",
                    reply_to_message_id=msg.message_id
                )
            return

        # Обработчик случайных фраз, например "как быть?"
        for keyword, responses_list in random_responses.items():
            if keyword in msg.text.lower():
                random_reply = choice(responses_list)
                await msg.reply(random_reply, reply_to_message_id=msg.message_id)
                return

        # Обработчик текста с фразами и ответами (основные фразы)
        for keyword, (response, sticker_id) in responses.items():
            if keyword in msg.text.lower():
                if response:
                    await msg.reply(response, reply_to_message_id=msg.message_id)
                if sticker_id:
                    await msg.reply_sticker(sticker_id, reply_to_message_id=msg.message_id)
                return

    # Логирование сообщения
    if msg.text:
        await bot.send_message(
            MY_TELEGRAM_ID,
            f"Сообщение от {msg.from_user.full_name} ({msg.from_user.id}) в чате {msg.chat.id}: {msg.text}"
        )

    # Ответ на сообщение "шо"
    if msg.text.lower().strip() == "шо":
        if msg.chat.type == "private":
            await msg.answer("капшо")
        elif msg.chat.type in {"group", "supergroup"}:
            await msg.reply("капшо", reply_to_message_id=msg.message_id)
        return

# Main function
async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
