# StatsBattle

**Живая версия: https://battlestats.onrender.com**

Разбор реплеев «Мира Танков» (`.mtreplay`) и статистика по своему взводу или клану.
Загружаешь пачку реплеев — получаешь таблицы по урону, ассисту, заблокированному,
фрагам и точности, а нужные бои сохраняешь в базу и смотришь накопленную аналитику.

Стек: FastAPI + PostgreSQL, фронтенд — три статические страницы без сборки.

## Возможности

- **Быстрый чек** (`/`) — перетащил реплеи, сразу увидел матрицу «игроки × бои» по пяти метрикам.
  Ничего не сохраняется, пока не нажмёшь «Сохранить в базу».
- **Аналитика** (`/analytics`) — накопленная статистика: игроки, игрок × танк, карты, список боёв.
  Переключатель «Все бои / Мои бои».
- **Дедупликация** — один и тот же бой, выгруженный разными людьми, попадает в базу один раз
  (ключ — хеш от карты и пар «ник: урон»).
- **Скрытие игроков** — случайных союзников можно убрать из своей аналитики, не удаляя данные боя.
- **Аккаунты** — регистрация, вход, JWT в `localStorage`; бои привязаны к тому, кто их выгрузил.

## Быстрый старт

### Через docker compose (нужен только Docker)

```bash
docker compose up --build
```

Приложение поднимется на http://localhost:8000 вместе с локальным Postgres.

### Локально, без Docker

Понадобится Python 3.12 и доступный Postgres (например, бесплатная база на [Neon](https://neon.tech)).

```bash
python -m venv .venv && .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env                                 # и заполнить DATABASE_URL
export DATABASE_URL="postgresql://..."               # Windows: $env:DATABASE_URL="..."
uvicorn src.app:app --reload
```

Схема базы создаётся сама при старте — миграции запускать не нужно.

### Тесты

```bash
pytest
```

78 тестов: парсер реплеев, аутентификация, конфигурация, разбор `DATABASE_URL`
и все эндпоинты API поверх подменённого слоя базы. Агрегирующий SQL тесты не покрывают —
для него нужен настоящий Postgres.

## Переменные окружения

| Переменная | Обязательна | По умолчанию | Что делает |
|---|---|---|---|
| `DATABASE_URL` | да | — | Строка подключения к Postgres. `postgres://` чинится сам, для внешних хостов добавляется `sslmode=require` |
| `SECRET_KEY` | в проде | dev-заглушка | Подпись JWT. Минимум 32 символа, иначе прод не стартует |
| `ENV` | нет | `development` | `production` включает строгие проверки `SECRET_KEY` |
| `TOKEN_TTL_DAYS` | нет | `30` | Срок жизни сессии |
| `MAX_FILES_PER_UPLOAD` | нет | `200` | Лимит файлов за одну загрузку |
| `MAX_FILE_SIZE` | нет | `10485760` | Лимит на один файл, байт |
| `MAX_UPLOAD_SIZE` | нет | `157286400` | Лимит на всю загрузку, байт |
| `DB_POOL_MIN` / `DB_POOL_MAX` | нет | `1` / `5` | Размер пула соединений |
| `LOGIN_MAX_ATTEMPTS` | нет | `10` | Неудачных входов до блокировки пары IP+логин |
| `LOGIN_LOCKOUT_SECONDS` | нет | `300` | Длительность блокировки |

Сгенерировать ключ:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

## Деплой на Render + Neon

1. **База.** На [neon.tech](https://neon.tech) создай проект (регион `eu-central-1`) и скопируй
   connection string вида `postgresql://user:pass@ep-....neon.tech/dbname`.
2. **Приложение.** На [render.com](https://render.com) → **New → Blueprint**, укажи этот репозиторий.
   Render прочитает [`render.yaml`](render.yaml), создаст free web service и сам сгенерирует `SECRET_KEY`.
3. В настройках сервиса задай `DATABASE_URL` — это единственная переменная, которую Render не знает.
4. Деплой. Health check ходит в `/healthz`; если он красный, смотри логи — там будет `[DB] init failed`
   с причиной (чаще всего опечатка в connection string).

Free-тариф Render усыпляет сервис после 15 минут простоя, первый запрос после этого
занимает около 30 секунд. Neon тоже засыпает, но просыпается за доли секунды.

Проект также совместим с Railway и Heroku — там используется [`Procfile`](Procfile).

### Перенос данных со старого хостинга

Пока старая база жива:

```bash
pg_dump "<старый DATABASE_URL>" --no-owner --no-privileges -f battlestats.sql
psql "<новый DATABASE_URL>" -f battlestats.sql
```

Начисто почистить базу (аккаунты сохраняются, если не передать `--users`):

```bash
DATABASE_URL="postgresql://..." python scripts/clear_db.py
```

## API

Всё, кроме страниц и `/healthz`, требует заголовок `Authorization: Bearer <token>`.

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/api/auth/register` | Регистрация, возвращает токен |
| `POST` | `/api/auth/login` | Вход, возвращает токен |
| `GET` | `/api/auth/me` | Текущий пользователь |
| `POST` | `/api/upload` | Разбор `.mtreplay` (multipart). Ничего не сохраняет |
| `POST` | `/api/battles/save` | Сохранение выбранных боёв в базу |
| `GET` | `/api/battles?mine=` | Список боёв |
| `DELETE` | `/api/battles/{id}` | Убрать бой из своей истории |
| `GET` | `/api/analytics/summary?mine=` | Агрегаты: итоги, игроки, карты, танки |
| `GET` | `/api/players/hidden` | Скрытые игроки |
| `POST` | `/api/players/hidden` | Скрыть игрока (`{"name": "..."}`) |
| `DELETE` | `/api/players/hidden/{name}` | Вернуть игрока |
| `GET` | `/healthz` | Health check: `200` если база отвечает, иначе `503` |

Интерактивная схема — `/docs`.

## Структура

```
src/
  app.py             FastAPI: роуты, авторизация, лимиты загрузки, троттлинг входа
  config.py          Настройки из окружения, проверки при старте
  auth.py            bcrypt + JWT
  database.py        Схема, пул соединений, запросы и агрегаты
  replay_parser.py   Разбор бинарного формата .mtreplay
static/              index.html, analytics.html, login.html
tests/               pytest
scripts/clear_db.py  Очистка базы
```

## Формат .mtreplay

`magic (4 байта) | число блоков (4) | размер блока (4) | JSON | размер блока (4) | JSON`

Первый блок — метаданные боя (карта, дата, состав), второй — результаты
(`[итоги, игроки, фраги]`). Незавершённые реплеи содержат только первый блок:
такой бой разбирается как ничья с составом, но без статистики.
