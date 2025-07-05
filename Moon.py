# triangle_bybit_bot.py — торговый бот для Bybit (тестнет и реальная торговля)
import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from telegram import Bot
from telegram.constants import ParseMode
from telegram.ext import Application

# === Настройка логгирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('TriangleArbitrageBot')

# === Конфигурация режимов ===
TRADE_MODE = os.getenv("TRADE_MODE", "testnet")  # testnet или real
DEBUG_MODE = os.getenv("DEBUG_MODE", "True") == "True"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === Параметры торговли ===
COMMISSION_RATE = Decimal('0.001')  # 0.1%
MIN_PROFIT = Decimal('0.15') if TRADE_MODE == "real" else Decimal('0.01')
MAX_PROFIT = Decimal('2.0') if TRADE_MODE == "real" else Decimal('5.0')
START_COINS = ['USDT', 'BTC', 'ETH']
TARGET_VOLUME_USDT = Decimal('100') if TRADE_MODE == "real" else Decimal('10')
LOG_FILE = "real_trades.csv" if TRADE_MODE == "real" else "testnet_trades.csv"
TRIANGLE_HOLD_TIME = 5  # секунд
MAX_SLIPPAGE = Decimal('0.005') if TRADE_MODE == "real" else Decimal('0.01')
MAX_RETRIES = 2
RETRY_DELAY = 1

# === Ограничения частоты сделок ===
MAX_TRADES_PER_MINUTE = 2 if TRADE_MODE == "real" else 5
MAX_TRADES_PER_HOUR = 10 if TRADE_MODE == "real" else 20
MAX_TRADES_PER_DAY = 30 if TRADE_MODE == "real" else 50
trade_history = []

# === Инициализация биржи ===
exchange_config = {
    "enableRateLimit": True,
    "options": {"defaultType": "spot"},
}

if TRADE_MODE == "real":
    exchange_config.update({
        "apiKey": os.getenv("BYBIT_REAL_API_KEY"),
        "secret": os.getenv("BYBIT_REAL_API_SECRET"),
    })
    logger.info("Режим работы: РЕАЛЬНАЯ ТОРГОВЛЯ")
else:
    exchange_config.update({
        "apiKey": os.getenv("BYBIT_TESTNET_API_KEY"),
        "secret": os.getenv("BYBIT_TESTNET_API_SECRET"),
        "urls": {
            "api": {
                "public": "https://api-testnet.bybit.com",
                "private": "https://api-testnet.bybit.com",
            }
        }
    })
    logger.info("Режим работы: ТЕСТОВАЯ СЕТЬ")

exchange = ccxt.bybit(exchange_config)

# Инициализация файла лога
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w") as f:
        f.write("timestamp,mode,route,profit_percent,volume_usdt,status,details\n")

# Инициализация Telegram
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# === Вспомогательные функции ===
def decimal_round(value, precision=8):
    """Округление с использованием Decimal для точных расчетов"""
    return Decimal(str(value)).quantize(
        Decimal('1e-{}'.format(precision)), 
        rounding=ROUND_DOWN
    )

async def load_symbols():
    """Загрузка и фильтрация торговых пар"""
    markets = await exchange.load_markets()
    filtered_symbols = [
        symbol for symbol, market in markets.items() 
        if market.get('spot', True) and market['active'] and market['percentage']
    ]
    return filtered_symbols, markets

async def find_triangles(symbols):
    """Поиск треугольных арбитражных возможностей"""
    triangles = []
    for base in START_COINS:
        for sym1 in symbols:
            if not sym1.endswith(f'/{base}'): 
                continue
            mid1 = sym1.split('/')[0]
            for sym2 in symbols:
                if not sym2.startswith(f'{mid1}/'): 
                    continue
                mid2 = sym2.split('/')[1]
                third = f"{mid2}/{base}"
                if third in symbols:
                    triangles.append((base, mid1, mid2))
    return triangles

def apply_price_filters(price, market):
    """Применение фильтров цены (tick size)"""
    if 'precision' in market and price is not None:
        # Округление до допустимого шага цены
        if 'price' in market['precision']:
            tick_size = Decimal(str(10 ** -market['precision']['price']))
            price = Decimal(str(price)).quantize(tick_size, rounding=ROUND_DOWN)
        # Проверка минимальной/максимальной цены
        if 'limits' in market and 'price' in market['limits']:
            min_price = market['limits']['price'].get('min')
            max_price = market['limits']['price'].get('max')
            if min_price and price < Decimal(str(min_price)):
                return None
            if max_price and price > Decimal(str(max_price)):
                return None
    return float(price) if price is not None else None

def apply_amount_filters(amount, market):
    """Применение фильтров количества (min amount, step size)"""
    if 'precision' in market and amount is not None:
        # Округление до допустимого шага объема
        if 'amount' in market['precision']:
            step_size = Decimal(str(10 ** -market['precision']['amount']))
            amount = amount.quantize(step_size, rounding=ROUND_DOWN)
        # Проверка минимального/максимального объема
        if 'limits' in market and 'amount' in market['limits']:
            min_amount = market['limits']['amount'].get('min')
            max_amount = market['limits']['amount'].get('max')
            if min_amount and amount < Decimal(str(min_amount)):
                return None
            if max_amount and amount > Decimal(str(max_amount)):
                return None
    return float(amount) if amount is not None else None

async def get_execution_price(symbol, side, target_volume, markets):
    """Расчет цены исполнения с учетом ликвидности и фильтров"""
    try:
        market = markets[symbol]
        orderbook = await exchange.fetch_order_book(symbol, limit=20)
        book_side = orderbook['asks'] if side == "buy" else orderbook['bids']
        
        total_base = Decimal('0')
        total_cost = Decimal('0')
        max_liquidity = Decimal('0')
        target_volume_dec = Decimal(str(target_volume))
        
        for price, amount in book_side:
            price_dec = Decimal(str(price))
            amount_dec = Decimal(str(amount))
            cost = price_dec * amount_dec
            
            if total_cost + cost >= target_volume_dec:
                remaining_cost = target_volume_dec - total_cost
                base_amount = remaining_cost / price_dec
                total_base += base_amount
                total_cost += remaining_cost
                break
            else:
                total_base += amount_dec
                total_cost += cost
                
            max_liquidity += cost
        
        # Проверка достаточной ликвидности
        if total_cost < target_volume_dec * Decimal('0.8'):
            return None, 0, 0
            
        avg_price = total_cost / total_base
        filtered_price = apply_price_filters(avg_price, market)
        
        if filtered_price is None:
            return None, 0, 0
            
        return float(filtered_price), float(total_cost), float(max_liquidity)
        
    except Exception as e:
        logger.error(f"Orderbook error {symbol}: {str(e)}")
        return None, 0, 0

def format_line(index, pair, price, side, volume_usd, color, liquidity):
    """Форматирование строки для Telegram"""
    emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(color, "")
    return f"{emoji} {index}. {pair} - {price:.6f} ({side}), исполнено ${volume_usd:.2f}, доступно ${liquidity:.2f}"

async def send_telegram_message(text):
    """Отправка сообщения в Telegram"""
    try:
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, 
            text=text, 
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Telegram error: {str(e)}")

def log_trade(mode, base, mid1, mid2, profit, volume, status, details=""):
    """Логирование сделки в CSV файл"""
    with open(LOG_FILE, "a") as f:
        route = f"{base}->{mid1}->{mid2}->{base}"
        f.write(f"{datetime.utcnow()},{mode},{route},{profit:.4f},{volume},{status},{details}\n")

async def fetch_balances():
    """Получение балансов"""
    try:
        balances = await exchange.fetch_balance()
        return {k: Decimal(str(v)) for k, v in balances["total"].items() if Decimal(str(v)) > 0}
    except Exception as e:
        logger.error(f"Balance error: {str(e)}")
        return {}

def check_trade_limits():
    """Проверка ограничений на частоту сделок"""
    now = datetime.utcnow()
    
    # Фильтрация истории сделок
    trade_history[:] = [t for t in trade_history if t > now - timedelta(days=1)]
    
    # Проверка ограничений
    last_min = sum(1 for t in trade_history if t > now - timedelta(minutes=1))
    last_hour = sum(1 for t in trade_history if t > now - timedelta(hours=1))
    last_day = len(trade_history)
    
    if last_min >= MAX_TRADES_PER_MINUTE:
        return False, "Превышен лимит сделок в минуту"
    if last_hour >= MAX_TRADES_PER_HOUR:
        return False, "Превышен лимит сделок в час"
    if last_day >= MAX_TRADES_PER_DAY:
        return False, "Превышен дневной лимит сделок"
    
    return True, "OK"

async def safe_sell_to_usdt(currency, amount, markets):
    """Безопасная конвертация в USDT при проблемах с арбитражем"""
    if currency == "USDT":
        return amount
    
    symbol = f"{currency}/USDT"
    if symbol not in markets:
        logger.warning(f"Нет пары для конвертации {currency} в USDT")
        return Decimal('0')
    
    try:
        market = markets[symbol]
        amount_dec = Decimal(str(amount))
        
        # Применение фильтров
        filtered_amount = apply_amount_filters(amount_dec, market)
        if filtered_amount is None:
            return Decimal('0')
            
        min_amount = Decimal(str(market['limits']['amount'].get('min', 0)))
        if filtered_amount < min_amount:
            return Decimal('0')
            
        # Создание ордера
        order = await exchange.create_market_sell_order(symbol, float(filtered_amount))
        return Decimal(str(order['cost']))
    except Exception as e:
        logger.error(f"Ошибка конвертации {currency} в USDT: {str(e)}")
        return Decimal('0')

async def check_balance(symbol, side, amount, markets):
    """Проверка достаточности баланса для сделки"""
    if TRADE_MODE != "real":
        return True  # В тестовом режиме пропускаем проверку
        
    try:
        base_currency = symbol.split('/')[0] if side == "sell" else symbol.split('/')[1]
        balances = await fetch_balances()
        available_balance = balances.get(base_currency, Decimal('0'))
        
        if available_balance < Decimal(str(amount)):
            logger.warning(f"Недостаточно {base_currency} для сделки: требуется {amount}, доступно {available_balance}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Balance check error: {str(e)}")
        return False

async def execute_real_trade(route_id, steps, markets):
    """Выполнение реальных торговых операций с защитой"""
    # Проверка ограничений
    limit_ok, limit_msg = check_trade_limits()
    if not limit_ok:
        logger.warning(limit_msg)
        await send_telegram_message(f"⚠️ {limit_msg}")
        return False, limit_msg
    
    # Проверка балансов
    for symbol, side, amount in steps:
        if not await check_balance(symbol, side, amount, markets):
            error_msg = f"❌ Недостаточно средств для {symbol} {side} {amount}"
            await send_telegram_message(error_msg)
            return False, error_msg
    
    # Тестовый режим
    if TRADE_MODE == "testnet":
        test_msg = ["🧪 <b>ТЕСТОВАЯ СДЕЛКА</b>", f"Маршрут: {route_id}", "Действия:"]
        for i, (symbol, side, amount) in enumerate(steps):
            test_msg.append(f"{i+1}. {symbol} {side.upper()} {amount:.6f}")
        test_msg.append("\n⚠️ <i>В тестовом режиме сделка не исполняется</i>")
        await send_telegram_message("\n".join(test_msg))
        return True, "Test trade simulated"
    
    # Реальный режим
    executed = []
    trade_details = []
    start_balance = await fetch_balances()
    
    try:
        for i, (symbol, side, amount) in enumerate(steps):
            market = markets[symbol]
            amount_dec = Decimal(str(amount))
            
            # Применение фильтров
            filtered_amount = apply_amount_filters(amount_dec, market)
            if filtered_amount is None:
                error_msg = f"❌ Невалидный объем для {symbol}: {amount}"
                raise ValueError(error_msg)
            
            min_amount = Decimal(str(market['limits']['amount'].get('min', 0)))
            if filtered_amount < min_amount:
                error_msg = f"❌ Объем меньше минимального для {symbol}: {filtered_amount} < {min_amount}"
                raise ValueError(error_msg)
            
            # Создание ордера
            order = await exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=float(filtered_amount)
            executed.append(order)
            trade_details.append(f"{i+1}. {symbol} {side} {filtered_amount:.6f} (ID: {order['id']})")
            
            # Логирование
            logger.info(f"Исполнен ордер: {symbol} {side} {filtered_amount}")
            
            # Задержка между ордерами
            if i < len(steps) - 1:
                await asyncio.sleep(1 if TRADE_MODE == "real" else 0.5)
        
        # Обновление истории сделок
        trade_history.append(datetime.utcnow())
        
        # Расчет реальной прибыли
        end_balance = await fetch_balances()
        profit = end_balance.get('USDT', Decimal('0')) - start_balance.get('USDT', Decimal('0'))
        
        return True, {
            "orders": executed,
            "profit": profit,
            "details": trade_details
        }
        
    except Exception as e:
        # Восстановление после ошибки
        error_msg = f"🚨 Ошибка исполнения: {str(e)}"
        logger.error(error_msg)
        
        # Конвертация остатков в USDT
        if executed:
            currency = steps[len(executed)][0].split('/')[0] if len(executed) < len(steps) else ""
            if currency:
                balance = await fetch_balances()
                amount = balance.get(currency, Decimal('0'))
                if amount > Decimal('0'):
                    usdt_amount = await safe_sell_to_usdt(currency, amount, markets)
                    recovery_msg = f"🔁 Конвертировано {amount} {currency} в {usdt_amount:.2f} USDT"
                    logger.info(recovery_msg)
                    await send_telegram_message(recovery_msg)
        
        return False, error_msg

async def check_triangle(base, mid1, mid2, symbols, markets):
    """Проверка треугольной арбитражной возможности"""
    try:
        s1 = f"{mid1}/{base}"
        s2 = f"{mid2}/{mid1}"
        s3 = f"{mid2}/{base}"
        
        # Проверка существования пар
        if not all(s in symbols for s in [s1, s2, s3]):
            return
            
        # Получение цен исполнения
        price1, vol1, liq1 = await get_execution_price(s1, "buy", TARGET_VOLUME_USDT, markets)
        if not price1 or vol1 < float(TARGET_VOLUME_USDT) * 0.8:
            return
            
        price2, vol2, liq2 = await get_execution_price(s2, "buy", TARGET_VOLUME_USDT, markets)
        if not price2 or vol2 < float(TARGET_VOLUME_USDT) * 0.8:
            return
            
        price3, vol3, liq3 = await get_execution_price(s3, "sell", TARGET_VOLUME_USDT, markets)
        if not price3 or vol3 < float(TARGET_VOLUME_USDT) * 0.8:
            return
            
        # Расчет прибыли с использованием Decimal
        price1_dec = Decimal(str(price1))
        price2_dec = Decimal(str(price2))
        price3_dec = Decimal(str(price3))
        
        result = (TARGET_VOLUME_USDT / price1_dec) * (1 - COMMISSION_RATE)
        result = (result / price2_dec) * (1 - COMMISSION_RATE)
        result = (result * price3_dec) * (1 - COMMISSION_RATE)
        
        profit_percent = ((result - TARGET_VOLUME_USDT) / TARGET_VOLUME_USDT) * 100
        profit_percent = profit_percent.quantize(Decimal('0.01'))
        
        if profit_percent < MIN_PROFIT or profit_percent > MAX_PROFIT: 
            return
            
        # Формирование сообщения
        route_id = f"{base}->{mid1}->{mid2}->{base}"
        min_liquidity = round(min(liq1, liq2, liq3), 2)
        pure_profit_usdt = (result - TARGET_VOLUME_USDT).quantize(Decimal('0.01'))

        message_lines = [
            f"🔁 <b>Арбитражная возможность ({TRADE_MODE.upper()})</b>",
            format_line(1, s1, price1, "BUY", vol1, "green", liq1),
            format_line(2, s2, price2, "BUY", vol2, "yellow", liq2),
            format_line(3, s3, price3, "SELL", vol3, "red", liq3),
            "",
            f"💰 <b>Чистая прибыль:</b> {pure_profit_usdt:.2f} USDT",
            f"📈 <b>Спред:</b> {profit_percent:.2f}%",
            f"💧 <b>Мин. ликвидность:</b> ${min_liquidity:.2f}"
        ]

        logger.info("\n".join(message_lines))
        await send_telegram_message("\n".join(message_lines))
        log_trade(TRADE_MODE, base, mid1, mid2, float(profit_percent), min_liquidity, "detected")

        # Подготовка к исполнению
        steps = []
        amount = TARGET_VOLUME_USDT
        
        # Шаг 1: base -> mid1
        steps.append((s1, "buy", float(amount)))
        amount = amount / price1_dec * (1 - COMMISSION_RATE)
        
        # Шаг 2: mid1 -> mid2
        steps.append((s2, "buy", float(amount)))
        amount = amount / price2_dec * (1 - COMMISSION_RATE)
        
        # Шаг 3: mid2 -> base
        steps.append((s3, "sell", float(amount)))
        
        # Исполнение сделки
        trade_success, trade_result = await execute_real_trade(route_id, steps, markets)
        
        if trade_success:
            if TRADE_MODE == "real":
                # Для реального режима показываем детали ордеров и реальную прибыль
                profit_real = trade_result["profit"]
                profit_percent_real = (profit_real / TARGET_VOLUME_USDT * 100).quantize(Decimal('0.01'))
                
                success_msg = [
                    f"✅ <b>РЕАЛЬНАЯ СДЕЛКА ВЫПОЛНЕНА</b>",
                    f"Маршрут: {route_id}",
                    *trade_result["details"],
                    "",
                    f"💵 <b>Реальная прибыль:</b> {profit_real:.2f} USDT",
                    f"📊 <b>Реальная доходность:</b> {profit_percent_real}%",
                    f"⚙️ <b>Ожидаемая доходность:</b> {profit_percent}%"
                ]
                log_trade(TRADE_MODE, base, mid1, mid2, float(profit_percent_real), 
                         float(TARGET_VOLUME_USDT), "executed", f"profit={profit_real}")
            else:
                success_msg = [
                    f"✅ <b>ТЕСТОВАЯ СДЕЛКА СИМУЛИРОВАНА</b>",
                    f"Маршрут: {route_id}",
                    f"Ожидаемая прибыль: {profit_percent}%",
                    f"Сумма прибыли: {pure_profit_usdt:.2f} USDT",
                    f"<i>В тестовом режиме реальные ордера не создаются</i>"
                ]
                log_trade(TRADE_MODE, base, mid1, mid2, float(profit_percent), 
                         float(TARGET_VOLUME_USDT), "simulated")
            
            await send_telegram_message("\n".join(success_msg))
            
    except Exception as e:
        error_msg = f"⚠️ <b>Ошибка треугольника ({TRADE_MODE}):</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        logger.error(f"Triangle error: {str(e)}", exc_info=True)

async def send_balance_update():
    """Отправка баланса в Telegram"""
    try:
        balances = await fetch_balances()
        if not balances:
            return
            
        msg = [f"💰 <b>БАЛАНС ({TRADE_MODE.upper()}):</b>"]
        for coin, amount in balances.items():
            if amount > Decimal('0.0001'):
                msg.append(f"{coin}: {amount:.6f}")
        
        await send_telegram_message("\n".join(msg))
    except Exception as e:
        logger.error(f"Balance update error: {str(e)}")

async def check_exchange_connection():
    """Проверка соединения с биржей"""
    try:
        await exchange.fetch_time()
        logger.info(f"Соединение с Bybit ({TRADE_MODE}) установлено")
        return True
    except Exception as e:
        error_msg = f"❌ <b>Ошибка подключения к Bybit ({TRADE_MODE}):</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        logger.error(f"Connection error: {str(e)}", exc_info=True)
        return False

async def main_loop():
    """Основной цикл торговли"""
    last_balance_update = 0
    symbols, markets = await load_symbols()
    triangles = await find_triangles(symbols)
    
    logger.info(f"Найдено треугольников: {len(triangles)}")
    await send_telegram_message(
        f"🤖 Бот запущен в режиме <b>{TRADE_MODE.upper()}</b>\n"
        f"🔍 Найдено треугольников: {len(triangles)}\n"
        f"💵 Стартовый объем: {TARGET_VOLUME_USDT} USDT"
    )
    
    while True:
        try:
            # Проверка треугольников
            tasks = [check_triangle(base, mid1, mid2, symbols, markets) 
                     for base, mid1, mid2 in triangles]
            await asyncio.gather(*tasks)
            
            # Ежечасное обновление баланса
            if time.time() - last_balance_update > 3600:
                await send_balance_update()
                last_balance_update = time.time()
                
            await asyncio.sleep(15)
            
        except Exception as e:
            logger.error(f"Main loop error: {str(e)}", exc_info=True)
            await asyncio.sleep(60)

async def main():
    """Точка входа в программу"""
    try:
        # Инициализация Telegram
        await telegram_app.initialize()
        await telegram_app.start()
        
        # Проверка соединения
        if not await check_exchange_connection():
            return
            
        # Запуск основного цикла
        await main_loop()
            
    except KeyboardInterrupt:
        logger.info("Остановка по запросу пользователя")
    except Exception as e:
        error_msg = f"🚨 <b>Критическая ошибка ({TRADE_MODE}):</b>\n{str(e)}"
        await send_telegram_message(error_msg)
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        try:
            await exchange.close()
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception as e:
            logger.error(f"Shutdown error: {str(e)}")

if __name__ == '__main__':
    asyncio.run(main())