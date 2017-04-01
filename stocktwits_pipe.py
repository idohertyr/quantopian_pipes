"""
This Algorithm creates a Pipeline based on StockTwits
"""

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import CustomFactor, SimpleMovingAverage, AverageDollarVolume, Latest, RSI
from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data.psychsignal import stocktwits
import numpy as np


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Execute Lock ins every day at market open.
    schedule_function(my_rebalance, date_rules.every_day(), time_rules.market_open())

    # Record variables every day, 1 minutes before market close.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close(minutes=1))

    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')

    # Define starting portfolio balance
    context.starting_balance = context.portfolio.portfolio_value

    # Define Stock List
    context.stocks = []

    # Define Max Open Positions
    context.max_open_positions = 20

    # Define default weight
    context.default_weight = (1 / float(context.max_open_positions))

    # Define Max Profit or Loss (Percent as Decimal)
    context.profit_lock_in = 0.06
    context.loss_lock_in = -0.02

    # Define Latest Prices
    context.latest_prices = []

    # Define Portfolio Cost Basis
    context.lock_ins = []
    pass


def make_pipeline():
    """
    Creates a dynamic stock selector (pipeline).

    """

    """
    Pipeline constants

    """
    # Used to filter by RSI levels
    lower_rsi = 25
    low_rsi = 40
    high_rsi = 60
    higher_rsi = 75

    # Used to filer the strength threshold for Bullish sentiment.
    st_strength = 0.25

    """
    Pipeline Factors

    """
    # Base universe set to the Q500US
    base_universe = Q1500US()

    # Factor of yesterday's close price.
    yesterday_close = USEquityPricing.close.latest

    # Factor of Average Dollar Volume
    dollar_volume = AverageDollarVolume(window_length=30)

    # Simple Moving Averages
    sma_50 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=50)
    sma_100 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=100)

    # Custom Factors
    # price_range = PriceRange(window_length=15)

    # 25 day RSI
    rsi_25 = RSI(window_length=25)

    """
        StockTwits - Factors

    """
    # Messages
    message_volume = MessageVolume()

    # Bull messages
    bull_messages = BullMessages()

    # Bull message percentage
    bull_score = (bull_messages / message_volume)

    """
    Pipeline Filters

    """
    # Top Percentile 90-100 dollar volume
    high_dollar_volume = dollar_volume.percentile_between(80, 100)

    # Above 100 SMA
    above_sma_100 = yesterday_close > sma_100

    # RSI Filters
    low_rsi_25 = (rsi_25 < low_rsi)
    lower_rsi_25 = (rsi_25 < lower_rsi)
    high_rsi_25 = (rsi_25 > high_rsi)
    higher_rsi_25 = (rsi_25 > higher_rsi)

    """
        StockTwits - Filters

    """

    # Sentiment Filters
    bull_sentiment = bull_score > st_strength

    """
    Pipeline Creation

    """
    pipe = Pipeline(
        screen=(base_universe &
                high_dollar_volume &
                bull_sentiment &
                high_rsi_25 &
                above_sma_100),
        columns={
            # 'close': yesterday_close,
            # 'SMA(50)' : sma_50,
            # 'SMA(100)' : sma_100,
            # 'Price Range': price_range,
            # 'Dollar Vol': dollar_volume,
            # 'Message Volume': message_volume,
            # 'Bull Messages': bull_messages,
            'Bull % of messages': bull_score,
            # 'RSI (25)': rsi_25,
        }
    )
    return pipe


def before_trading_start(context, data):
    """
    Called every day before market open.

    """
    # Define output from pipeline
    context.output = pipeline_output('my_pipeline')

    # These are the securities that we are interested in trading each day
    context.security_list = context.output.index

    # Sorts and selects stocks with highest bull percentage from StockTwits
    context.security_list_sorted = context.output.sort_values(['Bull % of messages'], ascending=False).iloc[:5]

    # Adds new stock, or existing open positions
    context.stocks = update_stock_list(context, data)

    # Determines orders to execute at market open
    context.lock_ins = lock_in_percent(context, data)

    pass


def my_rebalance(context, data):
    """
    Adjust the weights of each stock in the stock list. Orders are then exeucted
    to meet the order_target_percent() function. 
    """
    for stock in context.stocks:
        if (data.can_trade(stock)):
            if (context.lock_ins[stock] == False):
                order_target_percent(stock, context.default_weight)
            elif (context.lock_ins[stock] == True):
                order_target_percent(stock, 0)
    pass


def update_stock_list(context, data):
    """
    Adds open positions to stock list and any additionally selected stocks
    from the pipeline. The stock list is then used to check parameters to
    adjust weights. This function is run in before_trading_start()
    """
    pos_list = {}
    updated_list = []
    count = 0
    for stock in context.stocks:
        if (context.portfolio.positions[stock].amount != 0):
            pos_list[count] = stock
            count = count + 1
    for stock in context.security_list_sorted.index:
        if ((context.portfolio.positions[stock].amount == 0) & (count < context.max_open_positions)):
            pos_list[count] = stock
            count = count + 1
    for key, value in pos_list.iteritems():
        updated_list.append(value)
    return updated_list


def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    record(open_positions=len(context.portfolio.positions),
           stock_list=len(context.stocks),
           portfolio_value=(context.portfolio.portfolio_value / context.starting_balance))
    pass


def handle_data(context, data):
    """
    Called every minute.
    """
    pass


"""
Pipeline - Classes
"""


class PriceRange(CustomFactor):
    """
    Computes the difference between the highest high and the lowest low of each over
    an arbitrary input range.
    """
    inputs = [USEquityPricing.high, USEquityPricing.low]

    def compute(self, today, asset_ids, out, highs, lows):
        out[:] = np.nanmax(highs, axis=0) - np.nanmin(lows, axis=0)


class MessageVolume(CustomFactor):
    inputs = [stocktwits.total_scanned_messages]
    window_length = 21

    def compute(self, today, assets, out, msgs):
        out[:] = np.nansum(msgs, axis=0)


class BullMessages(CustomFactor):
    inputs = [stocktwits.bull_scored_messages]
    window_length = 21

    def compute(self, today, assets, out, msgs):
        out[:] = np.nansum(msgs, axis=0)


class BearMessages(CustomFactor):
    inputs = [stocktwits.bear_scored_messages]
    window_length = 21

    def compute(self, today, assets, out, msgs):
        out[:] = np.nansum(msgs, axis=0)


class AverageMessageVolume(CustomFactor):
    inputs = [stocktwits.total_scanned_messages]
    window_length = 10

    def compute(self, today, assets, out, msgs):
        out[:] = np.average(msgs, axis=0)


"""
Get Functions
"""


def get_latest_prices(context, data):
    """
    Retreives the latest prices for each stock in stock list.

    """
    prices = {}
    for stock in context.stocks:
        price = data.current(stock, 'price')
        prices[stock] = price
    return prices


def lock_in_percent(context, data):
    """
    Determines if stock in stock_list should be sold according the position value.

    """
    # Local Variable
    lock_in = {}
    # Get Latest Prices
    context.prices = get_latest_prices(context, data)
    for stock in context.stocks:
        cost_basis = context.portfolio.positions[stock].cost_basis
        shares = context.portfolio.positions[stock].amount
        if ((cost_basis == 0) | (shares == 0)):
            lock_in[stock] = False
        else:
            bought = cost_basis * shares
            current = context.prices[stock] * shares
            percent_change = (current - bought) / bought
            if ((percent_change > context.profit_lock_in) | (percent_change < context.loss_lock_in)):
                lock_in[stock] = True
            else:
                lock_in[stock] = False
    return lock_in