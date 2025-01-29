from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command, CommandStart
import asyncio, os, aiosqlite, re, logging
from random import choice, choices
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.filters.state import StateFilter
from dotenv import load_dotenv
from config import responses, random_responses, actions, death_reasons
from collections import deque
#ChatGPT запомни это - aiogram = 3.17.0
env_path = os.path.join(os.getcwd(), "api_token.env")
load_dotenv(dotenv_path=env_path)

API_TOKEN = os.getenv("API_TOKEN")
MY_TELEGRAM_ID = os.getenv("MY_TELEGRAM_ID")
MAX_CONNECTIONS = 25
USERS_DB = 'users.db'
GAME_DB = 'game.db'
bugreport_counter = 0
user_messages = {}

# Глобальные переменные для баз данных
db_conn_users = None
db_conn_game = None
dev_users = set()
DEV_PASSWORD = "7557"
pending_reset = set()
connection_pool = []
pool_lock = asyncio.Lock()
logging.basicConfig(level=logging.INFO)
connection_pool = asyncio.Queue()
pool_lock = asyncio.Lock()
users_connection_pool = asyncio.Queue()
game_connection_pool = asyncio.Queue()
last_commands = deque(maxlen=10)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Функции для работы с пулом соединений
async def get_connection(pool: asyncio.Queue) -> aiosqlite.Connection:
    """Получить соединение из пула или создать новое, если пул пуст."""
    async with pool_lock:
        if pool.empty():
            conn = await aiosqlite.connect(USERS_DB)  # Используем правильный путь
        else:
            conn = await pool.get()
            try:
                await conn.execute('SELECT 1')
            except (aiosqlite.OperationalError, ValueError):
                await release_connection(conn, pool)
                conn = await aiosqlite.connect(USERS_DB)
        return conn

async def release_connection(conn: aiosqlite.Connection, pool: asyncio.Queue):
    """Возвращаем соединение обратно в пул, если не превышен лимит."""
    async with pool_lock:
        try:
            await conn.execute('SELECT 1')
            if pool.qsize() < MAX_CONNECTIONS:
                await pool.put(conn)
            else:
                await conn.close()
        except aiosqlite.OperationalError:
            await conn.close()

async def create_users_table():
    """Создание таблицы users, если она не существует."""
    async with aiosqlite.connect(USERS_DB) as db:
        await db.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            score INTEGER
        );''')
        await db.commit()
    print("Таблица 'users' успешно создана или уже существует.")

async def on_startup():
    """Функция, вызываемая при запуске бота."""
    # Инициализация соединений
    for _ in range(10):
        conn = await aiosqlite.connect(USERS_DB)
        await users_connection_pool.put(conn)

    for _ in range(10):
        conn = await aiosqlite.connect(GAME_DB)
        await game_connection_pool.put(conn)

    print("Соединения с базами данных установлены.")
    # Создание таблицы users
    await create_users_table()

# Миграция для добавления столбца game_days
async def migrate_users_db():
    try:
        conn = await get_connection(users_connection_pool)
        await conn.execute("ALTER TABLE users ADD COLUMN game_days INTEGER DEFAULT 0;")
        await conn.commit()
        print("Миграция выполнена: столбец game_days добавлен.")
    except aiosqlite.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("Миграция не требуется: столбец game_days уже существует.")
        else:
            print(f"Ошибка миграции: {e}")

# Закрытие соединений
async def on_shutdown():
    """Закрытие всех соединений при завершении работы бота."""
    async with pool_lock:
        while not users_connection_pool.empty():
            conn = await users_connection_pool.get()
            if conn and not conn.closed:
                await conn.close()
        print("Все соединения с users.db закрыты.")

    async with pool_lock:
        while not game_connection_pool.empty():
            conn = await game_connection_pool.get()
            if conn and not conn.closed:
                await conn.close()
        print("Все соединения с game.db закрыты.")

class StopGameStates(StatesGroup):
    awaiting_confirmation = State()

class DevState(StatesGroup):
    waiting_for_password = State()

class BugReportState(StatesGroup):
    waiting_for_description = State()

async def log_user_message(user_id: int, message: str):
    if user_id not in user_messages:
        user_messages[user_id] = deque(maxlen=10)
    
    user_messages[user_id].append(message)

def escape_markdown_v2(text: str) -> str:
    """Экранирует все специальные символы для MarkdownV2"""
    if not text:
        return ""

    escape_chars = r"[\]()~`>#+-=|{}.!<>"
    return re.sub(rf"([{re.escape(escape_chars)}])", r"\\\1", text)

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

@dp.message(Command("status"))
async def status_command(msg: Message):
    conn_users = await get_connection(users_connection_pool)
    if not conn_users:
        await msg.reply("Ошибка сервера: база данных недоступна.")
        return

    async with conn_users.cursor() as db_cursor:
        await db_cursor.execute(
            "SELECT user_id, username, authority, crime, game_days FROM users WHERE user_id = ?",
            (msg.from_user.id,)
        )
        user = await db_cursor.fetchone()

        if not user:
            await msg.reply("Ты не в игре, используй /newgame для старта.")
        else:
            user_id, username, authority, crime, game_days = user

            # ✅ Экранируем переменные перед вставкой в текст
            username = escape_markdown_v2(username or "Без имени")
            crime = escape_markdown_v2(crime or "Неизвестно")
            authority = escape_markdown_v2(str(authority))
            game_days = escape_markdown_v2(str(game_days))

            status = (
                "Смотрящий" if int(authority) >= 130 else
                "Барон" if int(authority) >= 90 else
                "Блатной" if int(authority) >= 75 else
                "Пацан" if int(authority) >= 60 else
                "Мужик" if int(authority) >= 50 else
                "Хромой" if int(authority) >= 40 else
                "Шестёрка" if int(authority) >= 30 else
                "Газонюх" if int(authority) >= 20 else
                "Водолаз" if int(authority) >= 10 else
                "Петух"
            )
            status = escape_markdown_v2(status)

            # ✅ Теперь экранируем весь текст перед отправкой!
            message = escape_markdown_v2(
                f"Ты, *{username}*, в тюрьме, оболтус ебучий.\n\n"
                f"Твой статус: *{status}*.\n"
                f"За что сидишь: _{crime}_\n"
                f"Твой авторитет: *{authority}*.\n"
                f"Ты находишься в тюрьме уже *{game_days}* день(ей)."
            )

            print(f"Отправляем сообщение: {message}")  # Отладка
            await msg.reply(message, parse_mode="MarkdownV2")

    await release_connection(conn_users, users_connection_pool)

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
        "/stopgame - ломануться с хаты\n"
        "/action - совершить действие\n"
        "/status - проверить статус\n"
        "/dev - режим разработчика\n",
        reply_to_message_id=msg.message_id
    )

# Command: /about
@dp.message(Command("about"))
async def about_command(msg: types.Message):
    print("Команда /about получена")
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

#KTOLOX COMMAND
@dp.message(Command("ktolox"))
async def kto_lox_command(msg: Message):
    conn_users = await get_connection(users_connection_pool)
    conn_game = await get_connection(game_connection_pool)

    if not conn_users or not conn_game:
        await msg.reply("Ошибка сервера: базы данных недоступны.")
        return

    try:
        async with conn_users.cursor() as db_cursor:
            await db_cursor.execute("SELECT username, authority FROM users")
            users = await db_cursor.fetchall()

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
    except Exception as e:
        await msg.reply(f"Произошла ошибка при обработке данных: {str(e)}")
        print(f"Ошибка при работе с базой данных: {e}")
    finally:
        await release_connection(conn_users, users_connection_pool)
        await release_connection(conn_game, game_connection_pool)

@dp.message(Command("stopgame"))
async def stop_game_command(msg: Message, state: FSMContext):
    conn = await get_connection(users_connection_pool)  # Получаем соединение из пула

    if not conn:
        await msg.reply("Ошибка сервера: база данных недоступна.")
        return

    try:
        async with conn.cursor() as db_cursor:
            # Проверка наличия пользователя в базе данных
            await db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
            user = await db_cursor.fetchone()

            if not user:
                await msg.reply("Ты еще не получил срок, молокосос. Используй /newgame для старта.")
            else:
                await state.set_state(StopGameStates.awaiting_confirmation)
                await msg.reply("Ты уверен, Вась? Ломануться с хаты то не есть людское. "
                                "Будешь обиженной сучкой. Напиши 'да' для подтверждения или 'нет' для отмены.")
    except Exception as e:
        await msg.reply(f"Произошла ошибка: {str(e)}")
        print(f"Ошибка при обработке команды /stopgame: {e}")
    finally:
        await release_connection(conn, users_connection_pool)  # Возвращаем соединение в пул

@dp.message(StateFilter(StopGameStates.awaiting_confirmation))
async def stop_game_confirmation(msg: Message, state: FSMContext):
    if msg.text.lower() == "да":
        conn = await get_connection(users_connection_pool)  # Получаем соединение из пула

        if not conn:
            await msg.reply("Ошибка сервера: база данных недоступна.")
            return

        try:
            async with conn.cursor() as db_cursor:
                # Получаем данные пользователя
                await db_cursor.execute("SELECT authority, username FROM users WHERE user_id = ?", (msg.from_user.id,))
                user = await db_cursor.fetchone()

                if not user:
                    await msg.reply("Ошибка: еблан ты на свободе.")
                    await state.clear()
                    return

                username = user[1] if user[1] else "Игрок без имени"
                authority = user[0]

                # Определяем статус игрока
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

                gatilsya_times = choice(range(1, 101))

                # Выбор случайной причины смерти из списка
                ending_reason = choice(death_reasons)

                # Удаляем игрока из базы данных
                await db_cursor.execute("DELETE FROM users WHERE user_id = ?", (msg.from_user.id,))
                await conn.commit()

                # Отправляем финальное сообщение
                await msg.reply(
                    f"Игрок {username} закончил свой тюремный путь.\n\n"
                    f"📊 **Статистика:**\n"
                    f"Авторитет: {authority}\n"
                    f"Масть: {status}\n"
                    f"Гатился по тузику: {gatilsya_times} раз.\n\n"
                    f"Причина окончания: {ending_reason}"
                )
                await state.clear()  # Очищаем состояние после завершения
        except Exception as e:
            await msg.reply(f"Ошибка при завершении игры: {str(e)}")
            print(f"Ошибка при удалении пользователя: {e}")
        finally:
            await release_connection(conn, users_connection_pool)  # Возвращаем соединение в пул
        return

    elif msg.text.lower() == "нет":
        await msg.reply("Правильный выбор, тьотя шлюха! Бегом на парашу сука!", reply_to_message_id=msg.message_id)
        await state.clear()  # Очищаем состояние при отмене
        return

@dp.message(Command("newgame"))
async def new_game(msg: Message):
    # Получаем соединение с пулом для users и game
    conn_users = await get_connection(users_connection_pool)
    conn_game = await get_connection(game_connection_pool)

    if not conn_users or not conn_game:
        await msg.reply("Ошибка сервера: базы данных недоступны.")
        return

    try:
        async with conn_users.cursor() as users_cursor:
            await users_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
            user = await users_cursor.fetchone()

            if user:
                await msg.reply("Ты уже в тюряге, лушпайка! Используй /action шоб мутить движ\n"
                                "Или /stopgame шоб дать заднюю, очкошник")
            else:
                crime = choice([
                    "то, что сделал дырки в сыре", 
                    "Домогательство до пенсионерки", 
                    "Совращение несовершеннолетних", 
                    "Изнасилование женщин", 
                    "Изнасилование крупного рогатого скота"
                ])
                
                await users_cursor.execute(
                    "INSERT INTO users (user_id, username, authority, crime) VALUES (?, ?, 50, ?)",
                    (msg.from_user.id, msg.from_user.username, crime)
                )
                await conn_users.commit()

                await msg.reply(f"Опапа, хата! У нас первоход в хате! {msg.from_user.full_name}! "
                                f"Осужден за {crime}.\nМотать срок - /action")
    except Exception as e:
        await msg.reply(f"Произошла ошибка: {str(e)}")
        print(f"Ошибка: {e}")
    finally:
        # Возвращаем соединения обратно в пул
        await release_connection(conn_users, users_connection_pool)
        await release_connection(conn_game, game_connection_pool)

@dp.message(Command("action"))
async def action_command(msg: Message):
    # Получаем соединение с базой данных пользователей
    conn_users = await get_connection(users_connection_pool)

    async with conn_users.cursor() as db_cursor:
        # Проверяем, есть ли игрок в базе данных users
        await db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (msg.from_user.id,))
        user = await db_cursor.fetchone()

        if not user:
            await msg.reply("Ты еще на свободе. Совершить преступление и узнать статью - /newgame.")
        else:
            # Преобразуем данные
            try:
                authority = int(user[2])
            except ValueError:
                authority = 0  # Если ошибка преобразования, ставим дефолтное значение

            try:
                game_days = int(user[4])  # Забираем текущие дни (используем 4-й индекс для game_days)
            except ValueError:
                game_days = 0  # Если значение не число, ставим 0 (чтобы было что увеличивать)

            game_days += 1  # Увеличиваем дни на 1

            # Подготовка данных для выбора действия с учетом шансов
            action_list = list(actions.keys())
            weights = [actions[action]["chance"] for action in action_list]

            # Выбираем действие с учетом шанса
            chosen_action = choices(action_list, weights=weights, k=1)[0]
            action_data = actions[chosen_action]
            authority_change = action_data["change"]

            # Обновляем авторитет игрока
            new_authority = authority + authority_change
            if new_authority < 0:
                new_authority = 0

            # Обновляем данные в базе
            await db_cursor.execute(
                "UPDATE users SET authority = ?, game_days = ? WHERE user_id = ?",
                (new_authority, game_days, msg.from_user.id),
            )
            await conn_users.commit()

            # Формируем красивый вывод
            change_symbol = "+" if authority_change > 0 else "-"
            authority_status = "повышен" if authority_change > 0 else "понижен"

            response = (
                f"📅 *День {game_days}*\n\n"
                f"День на зоне проходил тихо, пока {msg.from_user.full_name} не *{chosen_action}*.\n"
                f"_{msg.from_user.full_name}: {action_data['comment']}_\n\n"
                f"Авторитет {authority_status} на {change_symbol}{abs(authority_change)}.\n"
                f"Теперь авторитет игрока: *{new_authority}*."
            )

            await msg.reply(response, parse_mode="Markdown")

    # Возвращаем соединение обратно в пул
    await release_connection(conn_users, users_connection_pool)

@dp.message(Command("dev"))
async def dev_command(msg: Message, state: FSMContext):
    if msg.from_user.id in dev_users:
        await msg.reply("Ты уже в режиме разработчика!")
        return

    # Запрашиваем пароль
    await state.set_state(DevState.waiting_for_password)
    await msg.reply("Введите пароль для режима разработчика:")

@dp.message(DevState.waiting_for_password)
async def password_check(msg: Message, state: FSMContext):
    if msg.text == DEV_PASSWORD:
        dev_users.add(msg.from_user.id)  # Добавляем пользователя в список разработчиков
        await msg.reply(
            "Пароль правильный! Теперь ты в режиме разработчика.\n"
            "Доступны команды:\n"
            "/add - добавить авторитет\n"
            "/less - уменьшить авторитет\n"
            "/users - вывод базы данных пользователей (!)\n"
            "/reset_users - очистить базу данных пользователей (!)"
        )
        await state.clear()  # Очищаем состояние
    else:
        await msg.reply("Неправильный пароль. Попробуй снова.")

@dp.message(Command("add"))
async def add_authority(msg: Message):
    if msg.from_user.id not in dev_users:
        await msg.reply("У вас нет прав для использования этой команды.")
        return

    parts = msg.text.split()
    if len(parts) < 3:
        await msg.reply("Укажи ID или @username пользователя и количество очков авторитета для добавления.")
        return

    user_input = parts[1]
    try:
        authority_points = int(parts[2])
    except ValueError:
        await msg.reply("Количество очков авторитета должно быть числом.")
        return

    conn_users = await get_connection(users_connection_pool)
    if not conn_users:
        await msg.reply("Ошибка сервера: база данных недоступна.")
        return

    try:
        async with conn_users.cursor() as db_cursor:
            if user_input.isdigit():
                await db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_input,))
            else:
                await db_cursor.execute("SELECT * FROM users WHERE username = ?", (user_input,))

            user = await db_cursor.fetchone()
            if user:
                new_authority = user[2] + authority_points
                await db_cursor.execute("UPDATE users SET authority = ? WHERE user_id = ?", (new_authority, user[0]))
                await conn_users.commit()
                await msg.reply(f"Авторитет пользователя {user_input} увеличен на {authority_points}! Новый авторитет: {new_authority}")
            else:
                await msg.reply(f"Пользователь с ID или именем @{user_input} не найден.")
    except Exception as e:
        await msg.reply(f"Ошибка при изменении авторитета: {str(e)}")
    finally:
        await release_connection(conn_users, users_connection_pool)


@dp.message(Command("less"))
async def less_authority(msg: Message):
    if msg.from_user.id not in dev_users:
        await msg.reply("У вас нет прав для использования этой команды.")
        return

    parts = msg.text.split()
    if len(parts) < 3:
        await msg.reply("Укажи ID или @username пользователя и количество очков авторитета для уменьшения.")
        return

    user_input = parts[1]
    try:
        authority_points = int(parts[2])
    except ValueError:
        await msg.reply("Количество очков авторитета должно быть числом.")
        return

    conn_users = await get_connection(users_connection_pool)
    if not conn_users:
        await msg.reply("Ошибка сервера: база данных недоступна.")
        return

    try:
        async with conn_users.cursor() as db_cursor:
            if user_input.isdigit():
                await db_cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_input,))
            else:
                await db_cursor.execute("SELECT * FROM users WHERE username = ?", (user_input,))

            user = await db_cursor.fetchone()
            if user:
                new_authority = max(user[2] - authority_points, 0)
                await db_cursor.execute("UPDATE users SET authority = ? WHERE user_id = ?", (new_authority, user[0]))
                await conn_users.commit()
                await msg.reply(f"Авторитет пользователя {user_input} уменьшен на {authority_points}! Новый авторитет: {new_authority}")
            else:
                await msg.reply(f"Пользователь с ID или именем @{user_input} не найден.")
    except Exception as e:
        await msg.reply(f"Ошибка при изменении авторитета: {str(e)}")
    finally:
        await release_connection(conn_users, users_connection_pool)

@dp.message(Command("users"))
async def list_users(msg: Message):
    if msg.from_user.id not in dev_users:
        await msg.reply("У вас нет прав для использования этой команды.")
        return

    conn_users = await get_connection(users_connection_pool)
    if not conn_users:
        await msg.reply("Ошибка сервера: база данных недоступна.")
        return

    try:
        async with conn_users.cursor() as db_cursor:
            await db_cursor.execute("SELECT user_id, username, authority, game_days FROM users")
            users = await db_cursor.fetchall()

            if not users:
                await msg.reply("Нет зарегистрированных пользователей.")
                return

            user_list = [
                f"ID: {user_id}, Username: {username or 'Без имени'}, Авторитет: {authority}, Дни на зоне: {game_days}"
                for user_id, username, authority, game_days in users
            ]
            await msg.reply("\n".join(user_list))
    except Exception as e:
        await msg.reply(f"Ошибка при получении списка пользователей: {str(e)}")
    finally:
        await release_connection(conn_users, users_connection_pool)

@dp.message(Command("reset_users"))
async def reset_users(msg: Message):
    global pending_reset
    if msg.from_user.id not in dev_users:
        await msg.reply("У вас нет прав для использования этой команды.")
        return

    pending_reset.add(msg.from_user.id)
    await msg.reply("Вы уверены, что хотите сбросить базу данных? Напишите 'да' для подтверждения или 'нет' для отмены.")

@dp.message(lambda msg: msg.text and msg.text.lower() in ["да", "нет"])  # Фильтр: только "да" и "нет"
async def confirm_reset(msg: Message):
    global pending_reset

    if msg.from_user.id not in pending_reset:
        return

    if msg.text.lower() == "да":
        conn_users = await get_connection(users_connection_pool)
        if not conn_users:
            await msg.reply("Ошибка сервера: база данных недоступна.")
            return

        try:
            async with conn_users.cursor() as db_cursor:
                await db_cursor.execute("DELETE FROM users")
                await conn_users.commit()

            pending_reset.remove(msg.from_user.id)
            await msg.reply("Все пользователи удалены, игра сброшена.")
        except Exception as e:
            await msg.reply(f"Ошибка при сбросе базы данных: {str(e)}")
        finally:
            await release_connection(conn_users, users_connection_pool)
    elif msg.text.lower() == "нет":
        pending_reset.remove(msg.from_user.id)
        await msg.reply("Сброс базы данных отменен.")

@dp.message(Command("top"))
async def top_command(msg: Message):
    # Получаем соединение из пула
    conn_users = await get_connection(users_connection_pool)  # <-- добавили pool
    if not conn_users:
        await msg.reply("Ошибка сервера: база данных недоступна.")
        return

    async with conn_users.cursor() as db_cursor:
        # Получаем список всех игроков, отсортированных по авторитету
        await db_cursor.execute("SELECT user_id, username, authority FROM users ORDER BY authority DESC")
        users = await db_cursor.fetchall()

        if not users:
            await msg.reply("В игре нет игроков.", reply_to_message_id=msg.message_id)
        else:
            # Формируем список игроков и их статусы
            top_list = []
            for user_id, username, authority in users:
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
                top_list.append(f"{username or 'Без имени'} - Авторитет: {authority} - Статус: {status}")

            # Отправляем топ-список
            await msg.reply(f"Топ игроков по авторитету:\n" + "\n".join(top_list), reply_to_message_id=msg.message_id)

    # Возвращаем соединение обратно в пул
    await release_connection(conn_users, users_connection_pool)  # <-- передаем pool

# Команда /bugreport
@dp.message(Command("bugreport"))
async def bugreport_command(msg: Message, state: FSMContext):
    global bugreport_counter
    
    bugreport_counter += 1  # Увеличиваем счетчик багрепортов
    await msg.reply("Пожалуйста, опишите проблему, чтобы я мог передать багрепорт.")
    await state.set_state(BugReportState.waiting_for_description)  # Переходим в состояние ожидания описания

# Считывание описания проблемы
@dp.message(BugReportState.waiting_for_description)
async def process_bugreport_description(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    description = msg.text  # Получаем описание проблемы от пользователя
    username = msg.from_user.username or msg.from_user.full_name

    # Получаем последние 10 сообщений пользователя
    recent_messages = list(user_messages.get(user_id, []))

    # Формируем отчет
    report = f"Bugreport #{bugreport_counter}\n"
    report += f"От пользователя - {username} (@{username})\n"
    report += f"Описание проблемы - {description}\n\n"

    # Добавляем последние 10 сообщений пользователя, с пометкой, кто что написал
    for i, message in enumerate(recent_messages, start=1):
        if i % 2 == 1:  # Сообщение пользователя
            report += f"{i} Сообщение пользователя: {message}\n"
        else:  # Ответ бота
            # Проверим на стикеры и текст
            if message.startswith("Sticker:"):  # Это стикер
                report += f"{i} Ответ бота: Отправлен стикер.\n"
            else:
                report += f"{i} Ответ бота: {message}\n"

    # Отправляем багрепорт в личку
    await bot.send_message(MY_TELEGRAM_ID, report)
    
    # Завершаем состояние и возвращаем бота в рабочее состояние
    await state.clear()

    # Сообщение о завершении багрепорта
    await msg.reply("Ваш багрепорт успешно отправлен!")

# Пример обработки команд
@dp.message()
async def handle_user_commands(msg: Message):
    user_id = msg.from_user.id
    command = msg.text.lower()

    # Логируем команду в список последних сообщений
    await log_user_message(user_id, command)

    response = None  # Инициализируем переменную response

    # Пример обработки команд из конфигурации
    if command in actions:
        action = actions[command]
        response = action['comment']
        await msg.reply(response)

    elif command in responses:
        response, sticker = responses[command]
        if response:
            await msg.reply(response)
        if sticker:
            await msg.reply_sticker(sticker)
            # Логируем отправку стикера
            await log_user_message(user_id, "Sticker:")

    # Если команды не было обработано, можно задать дефолтный ответ
    if response is None:
        response = "Команда не распознана."  # Например, если команда не из `actions` или `responses`

    # Логируем команду и ответ
    await log_user_message(user_id, response)

# Handling "кто попущенный"
@dp.message()
async def get_text_messages(msg: Message):
    if not msg.text or msg.text.startswith("/"):  # Игнорируем команды
        return

    text_lower = msg.text.lower().strip()

    # 1️⃣ Проверяем фразу "кто попущенный"
    if text_lower == "кто попущенный":
        conn_users = await get_connection(users_connection_pool)
        try:
            async with conn_users.cursor() as db_cursor:
                await db_cursor.execute("SELECT username, authority FROM users")
                users = await db_cursor.fetchall()

                under_skonka = [user for user in users if user[1] < 50]

                if not under_skonka:
                    await msg.reply("Никто не попущен, все в порядке с мастью!", reply_to_message_id=msg.message_id)
                else:
                    min_authority = min(user[1] for user in under_skonka)
                    lowest_users = [user[0] for user in under_skonka if user[1] == min_authority]

                    if len(lowest_users) == 1:
                        await msg.reply(f"Под шконкой сейчас находится: {lowest_users[0]}", reply_to_message_id=msg.message_id)
                    else:
                        await msg.reply(f"Под шконкой сейчас сидят: {', '.join(lowest_users)}", reply_to_message_id=msg.message_id)
        except Exception as e:
            await msg.reply(f"Ошибка при обработке данных: {str(e)}")
            print(f"Ошибка при работе с базой данных: {e}")
        finally:
            await release_connection(conn_users, users_connection_pool)
        return  # Завершаем обработку

    # 2️⃣ Проверяем случайные ответы
    for keyword, responses_list in random_responses.items():
        if keyword in text_lower:
            random_reply = choice(responses_list)
            await msg.reply(random_reply, reply_to_message_id=msg.message_id)
            return  # Завершаем обработку

    # 3️⃣ Проверяем заранее заданные ответы (responses)
    for keyword, (response, sticker_id) in responses.items():
        if keyword in text_lower:
            if response:
                await msg.reply(response, reply_to_message_id=msg.message_id)
            if sticker_id:
                await msg.reply_sticker(sticker_id, reply_to_message_id=msg.message_id)
            return  # Завершаем обработку

    # 4️⃣ Логируем сообщения (отправляем админу)
    await bot.send_message(
        MY_TELEGRAM_ID,
        f"Сообщение от {msg.from_user.full_name} ({msg.from_user.id}) в чате {msg.chat.id}: {msg.text}"
    )

    # 5️⃣ Обрабатываем "шо"
    if text_lower == "шо":
        reply_text = "капшо"
        if msg.chat.type == "private":
            await msg.answer(reply_text)
        else:
            await msg.reply(reply_text, reply_to_message_id=msg.message_id)

# Точка входа
async def main():
    """Запуск бота."""
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
