from __future__ import absolute_import
from time import sleep, time, gmtime, strftime
import sys
from os.path import getmtime
import random
import requests
import atexit
import signal
import MySQLdb
import redis

from market_maker import bitmex
from market_maker.settings import settings
from market_maker.utils import log, constants, errors, math

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]


#
# Helpers
#
logger = log.setup_custom_logger('root')


class ExchangeInterface:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        if len(sys.argv) > 1:
            self.symbol = sys.argv[1]
        else:
            self.symbol = settings.SYMBOL
        self.bitmex = bitmex.BitMEX(base_url=settings.BASE_URL, symbol=self.symbol,
                                    apiKey=settings.API_KEY, apiSecret=settings.API_SECRET,
                                    orderIDPrefix=settings.ORDERID_PREFIX, postOnly=settings.POST_ONLY,
                                    timeout=settings.TIMEOUT)

    def cancel_order(self, order):
        tickLog = self.get_instrument()['tickLog']
        logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
        while True:
            try:
                self.bitmex.cancel(order['orderID'])
                sleep(settings.API_REST_INTERVAL)
            except ValueError as e:
                logger.info(e)
                sleep(settings.API_ERROR_INTERVAL)
            else:
                break

    def cancel_all_orders(self):
        if self.dry_run:
            return

        logger.info("Resetting current position. Canceling all existing orders.")
        tickLog = self.get_instrument()['tickLog']

        # In certain cases, a WS update might not make it through before we call this.
        # For that reason, we grab via HTTP to ensure we grab them all.
        orders = self.bitmex.http_open_orders()

        for order in orders:
            logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))

        if len(orders):
            self.bitmex.cancel([order['orderID'] for order in orders])

        sleep(settings.API_REST_INTERVAL)

    def get_portfolio(self):
        contracts = settings.CONTRACTS
        portfolio = {}
        for symbol in contracts:
            position = self.bitmex.position(symbol=symbol)
            instrument = self.bitmex.instrument(symbol=symbol)

            if instrument['isQuanto']:
                future_type = "Quanto"
            elif instrument['isInverse']:
                future_type = "Inverse"
            elif not instrument['isQuanto'] and not instrument['isInverse']:
                future_type = "Linear"
            else:
                raise NotImplementedError("Unknown future type; not quanto or inverse: %s" % instrument['symbol'])

            if instrument['underlyingToSettleMultiplier'] is None:
                multiplier = float(instrument['multiplier']) / float(instrument['quoteToSettleMultiplier'])
            else:
                multiplier = float(instrument['multiplier']) / float(instrument['underlyingToSettleMultiplier'])

            portfolio[symbol] = {
                "currentQty": float(position['currentQty']),
                "futureType": future_type,
                "multiplier": multiplier,
                "markPrice": float(instrument['markPrice']),
                "spot": float(instrument['indicativeSettlePrice'])
            }

        return portfolio

    def calc_delta(self):
        """Calculate currency delta for portfolio"""
        portfolio = self.get_portfolio()
        spot_delta = 0
        mark_delta = 0
        for symbol in portfolio:
            item = portfolio[symbol]
            if item['futureType'] == "Quanto":
                spot_delta += item['currentQty'] * item['multiplier'] * item['spot']
                mark_delta += item['currentQty'] * item['multiplier'] * item['markPrice']
            elif item['futureType'] == "Inverse":
                spot_delta += (item['multiplier'] / item['spot']) * item['currentQty']
                mark_delta += (item['multiplier'] / item['markPrice']) * item['currentQty']
            elif item['futureType'] == "Linear":
                spot_delta += item['multiplier'] * item['currentQty']
                mark_delta += item['multiplier'] * item['currentQty']
        basis_delta = mark_delta - spot_delta
        delta = {
            "spot": spot_delta,
            "mark_price": mark_delta,
            "basis": basis_delta
        }
        return delta

    def get_delta(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.get_position(symbol)['currentQty']

    def get_instrument(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.instrument(symbol)

    def get_margin(self):
        if self.dry_run:
            return {'marginBalance': float(settings.DRY_BTC), 'availableFunds': float(settings.DRY_BTC)}
        return self.bitmex.funds()

    def get_orders(self):
        if self.dry_run:
            return []
        return self.bitmex.open_orders()

    def get_execution_trades(self, symbol=None, count=1, startTime=None, endTime=None):
        if self.dry_run:
            return []
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.execution_trades(symbol, count, startTime, endTime)

    def get_trade_bucketed(self, symbol=None, binSize="1d", count=20, rethrow_errors=True):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.trade_bucketed(symbol, binSize, count)

    def get_highest_buy(self):
        buys = [o for o in self.get_orders() if o['side'] == 'Buy']
        if not len(buys):
            return {'price': -2**32}
        highest_buy = max(buys or [], key=lambda o: o['price'])
        return highest_buy if highest_buy else {'price': -2**32}

    def get_lowest_sell(self):
        sells = [o for o in self.get_orders() if o['side'] == 'Sell']
        if not len(sells):
            return {'price': 2**32}
        lowest_sell = min(sells or [], key=lambda o: o['price'])
        return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone

    def get_position(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.position(symbol)

    def get_ticker(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.ticker_data(symbol)

    def is_open(self):
        """Check that websockets are still open."""
        return not self.bitmex.ws.exited

    def check_market_open(self):
        instrument = self.get_instrument()
        if instrument["state"] != "Open" and instrument["state"] != "Closed":
            raise errors.MarketClosedError("The instrument %s is not open. State: %s" %
                                           (self.symbol, instrument["state"]))

    def check_if_orderbook_empty(self):
        """This function checks whether the order book is empty"""
        instrument = self.get_instrument()
        if instrument['midPrice'] is None:
            raise errors.MarketEmptyError("Orderbook is empty, cannot quote")

    def amend_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.amend_bulk_orders(orders)

    def create_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.create_bulk_orders(orders)

    def cancel_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.cancel([order['orderID'] for order in orders])


class OrderManager:
    def __init__(self):
        self.exchange = ExchangeInterface(settings.DRY_RUN)
        # Once exchange is created, register exit handler that will always cancel orders
        # on any error.
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        logger.info("Using symbol %s." % self.exchange.symbol)

        if settings.DRY_RUN:
            logger.info("Initializing dry run. Orders printed below represent what would be posted to BitMEX.")
        else:
            logger.info("Order Manager initializing, connecting to BitMEX. Live run: executing real trades.")

        
        self.start_time = time()
        self.end_time = int((self.start_time+14400)/28800)*28800+14400
        self.instrument = self.exchange.get_instrument()
        position = self.exchange.get_position()
        self.starting_qty = position['currentQty']
        self.running_qty = self.starting_qty
        self.current_qty = self.starting_qty
        self.tickId = int(self.start_time/60)
        self.current_cost = position['currentCost']
        self.current_comm = position['currentComm']
        self.buy_order_start_size = settings.ORDER_START_SIZE
        self.sell_order_start_size = settings.ORDER_START_SIZE
        self.base_price = self.instrument['markPrice']
        self.interval_factor = 1.0
        self.relist = True
        self.target_usd = 0.0
        self.target_xbt = 0.0
        self.mid_qty = 0
        self.smart_order = None
        self.sell_order_number = settings.ORDER_PAIRS
        self.buy_order_number = settings.ORDER_PAIRS
        
        self.db = MySQLdb.connect("localhost", "bitmex_bot", "A_B0t_Us3d_f0r_r3cord_da7a", "bitmex_test", charset='utf8' )
        # self.db = MySQLdb.connect("localhost", "bitmex_bot", "A_B0t_Us3d_f0r_r3cord_da7a", "bitmex", charset='utf8' )
        self.cursor = self.db.cursor()
        sql = "select * from %s order by id desc limit 1;" % settings.POSITION_TABLE_NAME
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                self.relist = False
                self.target_xbt = row[5]
                self.target_usd = row[3]
                self.mid_qty = -int(self.target_usd / 2)
        except:
            logger.info("Error: unable to fecth data")
        
        self.record_time = self.start_time
        self.get_sma()


    def reset(self):
        self.exchange.cancel_all_orders()
        self.sanity_check()
        self.print_status()

        # Create orders and converge.
        self.place_orders()

    def print_status(self):
        """Print the current MM status."""

        margin = self.exchange.get_margin()
        position = self.exchange.get_position()
        self.instrument = self.exchange.get_instrument()
        existing_orders = self.exchange.get_orders()

        self.running_qty = position['currentQty']
        tickLog = self.instrument['tickLog']
        self.start_XBt = margin["marginBalance"]

        if self.tickId % 240 == 0: 
            self.get_sma()
        
        if time() > self.tickId*60:
            sql = "INSERT INTO %s (id, markPrice, currentQty, targetUSD, totalXBT, targetXBT, currentCost, \
                   currentComm, recordTime) VALUES (%d, %f, %d, %d, %d, %d, %d, %d, %d);" \
                  % (settings.POSITION_TABLE_NAME, self.tickId, self.instrument['markPrice'], self.current_qty, self.target_usd, \
                     self.start_XBt, self.target_xbt, self.current_cost, self.current_comm, self.record_time)
            logger.info(sql)
            try:
                self.cursor.execute(sql)
                self.db.commit()
            except:
                self.db.rollback()
            self.tickId = self.tickId + 1
            self.base_price = self.base_price * settings.BASE_PRICE_FACTOR + self.instrument['markPrice'] * (1 - settings.BASE_PRICE_FACTOR)

        if self.current_qty != position['currentQty'] or time()>self.end_time:
            self.end_time = time()
            start_time=strftime("%Y-%m-%d %H:%M:%S.001", gmtime(self.start_time)) 
            end_time=strftime("%Y-%m-%d %H:%M:%S.000", gmtime(self.end_time)) 
            logger.info("start time: %s; end time: %s" % (start_time, end_time))
            logger.info("Get trades now.") #temp
            self.smart_order = None
            for trade in self.exchange.get_execution_trades(count=100, startTime=start_time, endTime=end_time): #temp
                trade_message = "(symbol: %s, side: %s, lastQty: %d, lastPx: %f , execComm: %d, execCost: %d, transactTime: %s)" % \
                      (trade["symbol"], trade["side"], trade["lastQty"],
                       trade["lastPx"], trade["execComm"], trade["execCost"], trade["transactTime"][:23])
                logger.info(trade_message) #temp
                if (trade["side"]=="Sell"):
                    self.current_qty = self.current_qty - trade["lastQty"]
                    self.current_cost = self.current_cost + trade["execCost"]
                    self.buy_order_start_size = settings.ORDER_START_SIZE
                    self.sell_order_start_size = self.sell_order_start_size + settings.ORDER_STEP_SIZE
                    self.sell_order_number = self.sell_order_number - 1
                    self.buy_order_number = self.buy_order_number + 1
                elif (trade["side"]=="Buy"):
                    self.current_qty = self.current_qty + trade["lastQty"];
                    self.current_cost = self.current_cost + trade["execCost"];
                    self.sell_order_start_size = settings.ORDER_START_SIZE
                    self.buy_order_start_size = self.buy_order_start_size + settings.ORDER_STEP_SIZE
                    self.sell_order_number = self.sell_order_number + 1
                    self.buy_order_number = self.buy_order_number - 1
                self.current_comm = self.current_comm + trade["execComm"];
            if self.current_qty == position['currentQty']:
                self.record_time = self.end_time
                self.start_time = self.end_time
                self.end_time = int((self.start_time+14400)/28800)*28800+14400
                if abs(self.current_qty-self.mid_qty) < 12:
                    self.relist = True
            else:
                try:
                    self.sell_order_start_size = settings.ORDER_START_SIZE
                    self.buy_order_start_size = settings.ORDER_START_SIZE
                    self.cursor.execute("delete from %s where id = (select id from %s order by id desc limit 1);" \
                                        % (settings.POSITION_TABLE_NAME, settings.POSITION_TABLE_NAME))
                    self.db.commit()
                    self.cursor.execute("delete from %s where id = (select id from %s order by id desc limit 1);" \
                                        % (settings.POSITION_TABLE_NAME, settings.POSITION_TABLE_NAME))
                    self.db.commit()
                except:
                    self.db.rollback()
                self.restart()

        try:
            self.cursor.execute("delete from %s;" % settings.ORDER_TABLE_NAME)
            self.db.commit()
        except:
            self.db.rollback()
        index = 0
        for order in existing_orders:
            # insert into mysql
            sql = "INSERT INTO %s (id, price, orderBy, side, timestamp) VALUES (%d, %f, %d, '%s', now());" \
                  % (settings.ORDER_TABLE_NAME, index, order['price'], order['leavesQty'], order['side'])
            index = index + 1
            try:
                self.cursor.execute(sql)
                self.db.commit()
            except:
                self.db.rollback()

        
        self.interval_factor = 1 / math.harmonicFactor(self.running_qty, settings.MIN_POSITION, settings.MAX_POSITION)
        #if (position['currentQty'] * (position['avgCostPrice']-instrument['markPrice']) < 0):
        #    self.interval_factor = 1 / self.interval_factor
        if self.relist:
            self.relist = False
            self.target_xbt = self.start_XBt
            self.target_usd = int(self.instrument['markPrice'] * XBt_to_XBT(self.start_XBt))
            self.mid_qty = -int(self.target_usd / 2)

        logger.info("Current Mark Price: %.*f" % (tickLog, float(self.instrument['markPrice'])))
        logger.info("Base Price: %f" % self.base_price)
        logger.info("Current Interval Factor: %.*f" % (2, float(self.interval_factor)))
        logger.info("Target XBT Balance: %.6f" % XBt_to_XBT(self.target_xbt))
        logger.info("Target USD Balance: %d" % self.target_usd)
        logger.info("Mid Quantity: %d" % self.mid_qty)
        logger.info("Current XBT Balance: %.6f" % XBt_to_XBT(self.start_XBt))
        logger.info("Current USD Balance: %.*f" % (2, self.instrument['markPrice'] * XBt_to_XBT(self.start_XBt)))
        logger.info("Current Contract Position: %d" % self.running_qty)
        if settings.CHECK_POSITION_LIMITS:
            logger.info("Position limits: %d/%d" % (settings.MIN_POSITION, settings.MAX_POSITION))
        if position['currentQty'] != 0:
            logger.info("Avg Cost Price: %.*f" % (tickLog, float(position['avgCostPrice'])))
            logger.info("Avg Entry Price: %.*f" % (tickLog, float(position['avgEntryPrice'])))
        logger.info("Contracts Traded This Run: %d" % (self.running_qty - self.starting_qty))
        logger.info("Total Contract Delta: %.4f XBT" % self.exchange.calc_delta()['spot'])

    def get_ticker(self):
        ticker = self.exchange.get_ticker()
        tickLog = self.instrument['tickLog']

        # Set up our buy & sell positions as the smallest possible unit above and below the current spread
        # and we'll work out from there. That way we always have the best price but we don't kill wide
        # and potentially profitable spreads.
        self.start_position_buy = ticker["buy"] + self.instrument['tickSize']
        self.start_position_sell = ticker["sell"] - self.instrument['tickSize']

        # If we're maintaining spreads and we already have orders in place,
        # make sure they're not ours. If they are, we need to adjust, otherwise we'll
        # just work the orders inward until they collide.
        if settings.MAINTAIN_SPREADS:
            if ticker['buy'] == self.exchange.get_highest_buy()['price']:
                self.start_position_buy = ticker["buy"]
            if ticker['sell'] == self.exchange.get_lowest_sell()['price']:
                self.start_position_sell = ticker["sell"]

        # Back off if our spread is too small.
        if self.start_position_buy * (1.00 + settings.MIN_SPREAD * self.interval_factor) > self.start_position_sell:
            self.start_position_buy *= (1.00 - (settings.MIN_SPREAD * self.interval_factor / 2))
            self.start_position_sell *= (1.00 + (settings.MIN_SPREAD * self.interval_factor / 2))

        # If the price changing too quickly, wait...
        if self.start_position_sell < self.base_price * 0.99:
            self.start_position_sell = self.base_price * 0.99
        if self.start_position_buy > self.base_price * 1.01:
            self.start_position_buy = self.base_price * 1.01

        # Midpoint, used for simpler order placement.
        self.start_position_mid = ticker["mid"]
        logger.info(
            "%s Ticker: Buy: %.*f, Sell: %.*f" %
            (self.instrument['symbol'], tickLog, ticker["buy"], tickLog, ticker["sell"])
        )
        logger.info('Start Positions: Buy: %.*f, Sell: %.*f, Mid: %.*f' %
                    (tickLog, self.start_position_buy, tickLog, self.start_position_sell,
                     tickLog, self.start_position_mid))
        return ticker

    def get_price_offset(self, index):
        """Given an index (1, -1, 2, -2, etc.) return the price for that side of the book.
           Negative is a buy, positive is a sell."""
        # Maintain existing spreads for max profit
        if settings.MAINTAIN_SPREADS:
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            # First positions (index 1, -1) should start right at start_position, others should branch from there
            # index = index + 1 if index < 0 else index - 1
        else:
            # Offset mode: ticker comes from a reference exchange and we define an offset.
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            index = index + 1 if index < 0 else index - 1

            # If we're attempting to sell, but our sell price is actually lower than the buy,
            # move over to the sell side.
            if index > 0 and start_position < self.start_position_buy:
                start_position = self.start_position_sell
            # Same for buys.
            if index < 0 and start_position > self.start_position_sell:
                start_position = self.start_position_buy

        return math.toNearest(start_position * (1 + settings.INTERVAL * self.interval_factor) ** index, self.instrument['tickSize'])

    def get_sma(self):
        pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
        r = redis.Redis(host='localhost', port=6379, decode_responses=True) 
        sma_1d = 0.0
        for candle in self.exchange.get_trade_bucketed(count=20, binSize="1d"):
            sma_1d = sma_1d + candle["close"] / 20.0
        sma_4h = 0.0
        for candle in self.exchange.get_trade_bucketed(count=80, binSize="1h"):
            sma_4h = sma_4h + candle["close"] / 80.0
        r.set(settings.POSITION_TABLE_NAME+'sma_1d', sma_1d)
        r.set(settings.POSITION_TABLE_NAME+'sma_4h', sma_4h)
        logger.info("1D SMA is: %f" % sma_1d)
        logger.info("4H SMA is: %f" % sma_4h)
        if self.instrument['markPrice'] > sma_1d and self.instrument['markPrice'] > sma_4h and self.current_qty-self.mid_qty < -12:
            self.sell_order_number = 0
            self.buy_order_number = settings.ORDER_PAIRS * 2
        if self.instrument['markPrice'] < sma_1d and self.instrument['markPrice'] < sma_4h and self.current_qty-self.mid_qty > 12:
            self.buy_order_number = 0
            self.sell_order_number = settings.ORDER_PAIRS * 2
    ###
    # Orders
    ###

    def place_orders(self):
        """Create order items for use in convergence."""

        buy_orders = []
        sell_orders = []

        # Create orders from the outside in. This is intentional - let's say the inner order gets taken;
        # then we match orders from the outside in, ensuring the fewest number of orders are amended and only
        # a new order is created in the inside. If we did it inside-out, all orders would be amended
        # down and a new order would be created at the outside.
        for i in reversed(range(1, self.buy_order_number + 1)):
            if not self.long_position_limit_exceeded():
                buy_orders.append(self.prepare_order(-i))
        for i in reversed(range(1, self.sell_order_number + 1)):
            if not self.short_position_limit_exceeded():
                sell_orders.append(self.prepare_order(i))

        hold_XBT = XBt_to_XBT(self.start_XBt) + self.running_qty/self.instrument['markPrice']
        if self.running_qty-self.mid_qty > 10:
            if self.target_usd+self.running_qty>0 and hold_XBT > 0 and self.start_XBt > self.target_xbt:
                quantity = self.running_qty - self.mid_qty + 8
                price = (self.target_usd+self.running_qty) / hold_XBT
                if (price < self.start_position_sell):
                    logger.info("Want to sell %d @ %f" % (quantity, price))
                    price = self.start_position_sell * (1+settings.INTERVAL/3)
                self.smart_order = {'price': math.toNearest(price, self.instrument['tickSize']), 'orderQty': quantity, 'side': "Sell"}
        if self.running_qty-self.mid_qty < -10:
            if self.running_qty < 0 and hold_XBT < XBt_to_XBT(self.target_xbt) and self.instrument['markPrice']*XBt_to_XBT(self.start_XBt) > self.target_usd:
                quantity = self.mid_qty - self.running_qty + 8
                price = self.running_qty / (hold_XBT-XBt_to_XBT(self.target_xbt))
                if (price > self.start_position_buy):
                    logger.info("Want to buy %d @ %f" % (quantity, price))
                    price = self.start_position_buy * (1-settings.INTERVAL/3)
                self.smart_order = {'price': math.toNearest(price, self.instrument['tickSize']), 'orderQty': quantity, 'side': "Buy"}
        if self.smart_order:
            if self.smart_order['side'] == 'Buy':
                buy_orders.append(self.smart_order)
            else:
                sell_orders.append(self.smart_order)
        return self.converge_orders(buy_orders, sell_orders)

    def prepare_order(self, index):
        """Create an order object."""

        if settings.RANDOM_ORDER_SIZE is True:
            quantity = random.randint(settings.MIN_ORDER_SIZE, settings.MAX_ORDER_SIZE)
        else:
            if index < 0:
                quantity = self.buy_order_start_size + ((abs(index) - 1) * settings.ORDER_STEP_SIZE)
            else:
                quantity = self.sell_order_start_size + ((abs(index) - 1) * settings.ORDER_STEP_SIZE)

        price = self.get_price_offset(index)

        return {'price': price, 'orderQty': quantity, 'side': "Buy" if index < 0 else "Sell"}

    def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        tickLog = self.instrument['tickLog']
        to_amend = []
        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = self.exchange.get_orders()

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            try:
                if order['side'] == 'Buy':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1

                # Found an existing order. Do we need to amend it?
                if desired_order['orderQty'] != order['leavesQty'] or (
                        # If price has changed, and the change is more than our RELIST_INTERVAL, amend.
                        desired_order['price'] != order['price'] and
                        abs((desired_order['price'] / order['price']) - 1) > settings.RELIST_INTERVAL * self.interval_factor):
                    to_amend.append({'orderID': order['orderID'], 'orderQty': order['cumQty'] + desired_order['orderQty'],
                                     'price': desired_order['price'], 'side': order['side']})
            except IndexError:
                # Will throw if there isn't a desired order to match. In that case, cancel it.
                to_cancel.append(order)

        while buys_matched < len(buy_orders):
            to_create.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            to_create.append(sell_orders[sells_matched])
            sells_matched += 1

        if len(to_amend) > 0:
            for amended_order in reversed(to_amend):
                reference_order = [o for o in existing_orders if o['orderID'] == amended_order['orderID']][0]
                logger.info("Amending %4s: %d @ %.*f to %d @ %.*f (%+.*f)" % (
                    amended_order['side'],
                    reference_order['leavesQty'], tickLog, reference_order['price'],
                    (amended_order['orderQty'] - reference_order['cumQty']), tickLog, amended_order['price'],
                    tickLog, (amended_order['price'] - reference_order['price'])
                ))
            # This can fail if an order has closed in the time we were processing.
            # The API will send us `invalid ordStatus`, which means that the order's status (Filled/Canceled)
            # made it not amendable.
            # If that happens, we need to catch it and re-tick.
            try:
                self.exchange.amend_bulk_orders(to_amend)
            except requests.exceptions.HTTPError as e:
                errorObj = e.response.json()
                if errorObj['error']['message'] == 'Invalid ordStatus':
                    logger.warn("Amending failed. Waiting for order data to converge and retrying.")
                    sleep(0.5)
                    return self.place_orders()
                else:
                    logger.error("Unknown error on amend: %s. Exiting" % errorObj)
                    sys.exit(1)

        if len(to_create) > 0:
            logger.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logger.info("%4s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
            self.exchange.create_bulk_orders(to_create)

        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logger.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logger.info("%4s %d @ %.*f" % (order['side'], order['leavesQty'], tickLog, order['price']))
            self.exchange.cancel_bulk_orders(to_cancel)

    ###
    # Position Limits
    ###

    def short_position_limit_exceeded(self):
        """Returns True if the short position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position <= settings.MIN_POSITION

    def long_position_limit_exceeded(self):
        """Returns True if the long position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position >= settings.MAX_POSITION

    ###
    # Sanity
    ##

    def sanity_check(self):
        """Perform checks before placing orders."""

        # Check if OB is empty - if so, can't quote.
        self.exchange.check_if_orderbook_empty()

        # Ensure market is still open.
        self.exchange.check_market_open()

        # Get ticker, which sets price offsets and prints some debugging info.
        ticker = self.get_ticker()

        # Sanity check:
        if self.get_price_offset(-1) >= ticker["sell"] or self.get_price_offset(1) <= ticker["buy"]:
            logger.error("Buy: %s, Sell: %s" % (self.start_position_buy, self.start_position_sell))
            logger.error("First buy position: %s\nBitMEX Best Ask: %s\nFirst sell position: %s\nBitMEX Best Bid: %s" %
                         (self.get_price_offset(-1), ticker["sell"], self.get_price_offset(1), ticker["buy"]))
            logger.error("Sanity check failed, exchange data is inconsistent")
            self.exit()

        # Messaging if the position limits are reached
        if self.long_position_limit_exceeded():
            logger.info("Long delta limit exceeded")
            logger.info("Current Position: %.f, Maximum Position: %.f" %
                        (self.exchange.get_delta(), settings.MAX_POSITION))

        if self.short_position_limit_exceeded():
            logger.info("Short delta limit exceeded")
            logger.info("Current Position: %.f, Minimum Position: %.f" %
                        (self.exchange.get_delta(), settings.MIN_POSITION))

    ###
    # Running
    ###

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def check_connection(self):
        """Ensure the WS connections are still open."""
        return self.exchange.is_open()

    def exit(self):
        logger.info("Shutting down. All open orders will be cancelled.")
        try:
            self.exchange.cancel_all_orders()
            self.exchange.bitmex.exit()
        except errors.AuthenticationError as e:
            logger.info("Was not authenticated; could not cancel orders.")
        except Exception as e:
            logger.info("Unable to cancel orders: %s" % e)
        self.db.close()
        sys.exit()

    def run_loop(self):
        while True:
            sys.stdout.write("-----\n")
            sys.stdout.flush()

            self.check_file_change()
            sleep(settings.LOOP_INTERVAL)

            # This will restart on very short downtime, but if it's longer,
            # the MM will crash entirely as it is unable to connect to the WS on boot.
            if not self.check_connection():
                logger.error("Realtime data connection unexpectedly closed, restarting.")
                self.restart()

            self.sanity_check()  # Ensures health of mm - several cut-out points here
            self.print_status()  # Print skew, delta, etc
            self.place_orders()  # Creates desired orders and converges to existing orders

    def restart(self):
        logger.info("Restarting the market maker...")
        os.execv(sys.executable, [sys.executable] + sys.argv)

#
# Helpers
#


def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT


def cost(instrument, quantity, price):
    mult = instrument["multiplier"]
    P = mult * price if mult >= 0 else mult / price
    return abs(quantity * P)


def margin(instrument, quantity, price):
    return cost(instrument, quantity, price) * instrument["initMargin"]


def run():
    logger.info('BitMEX Market Maker Version: %s\n' % constants.VERSION)

    om = OrderManager()
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        om.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
