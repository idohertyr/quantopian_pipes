"""
Author: Ian Doherty
Date: May 10, 2017

This is a Quantopian pipeline using StockTwits sentiment data.
"""

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import CustomFactor
from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.data.psychsignal import stocktwits
import numpy as np

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Record variables every day, 1 minutes before market close.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())

    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')

    # Define Stock List
    context.stocks = []
    pass

def make_pipeline():
    """
    Creates a dynamic stock selector (pipeline).

    """
    # Pipeline constants ----

    # Used to filer the strength threshold for Bullish sentiment.
    st_strength = 0.25

    # Pipeline Factors -----

    # Base universe set to the Q500US
    base_universe = Q1500US()

    # Messages
    message_volume = MessageVolume()

    # Bull messages
    bull_messages = BullMessages()

    # Bull message percentage
    bull_score = (bull_messages / message_volume)

    # Sentiment Filters
    bull_sentiment = bull_score > st_strength

    # Pipeline Creation ----

    pipe = Pipeline(
        screen=(base_universe &
                bull_sentiment),
        columns={
            'Message Volume': message_volume,
            'Bull Messages': bull_messages,
            'Bull % of messages': bull_score,
        }
    )
    return pipe

def before_trading_start(context, data):
    """
    Called every day before market open.

    """
    # Define output from pipeline
    context.output = pipeline_output('my_pipeline')

    # Sorts and selects stocks with highest bull percentage from StockTwits
    context.security_list_sorted = context.output.sort_values(['Bull % of messages'], ascending=False).iloc[-5:]

    log.info('Top 5 bullish sentiment on StockTwits ' + str(context.security_list_sorted))

    pass

def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    record(open_positions=len(context.portfolio.positions))
    pass

def handle_data(context, data):
    """
    Called every minute.
    """
    pass

"""
Pipeline - Classes
"""
class MessageVolume(CustomFactor):
    inputs = [stocktwits.total_scanned_messages]
    window_length = 21

    def compute(self, today, assets, out, msgs):
        out[:] = np.nansum(msgs, axis=0)
        pass
    pass

class BullMessages(CustomFactor):
    inputs = [stocktwits.bull_scored_messages]
    window_length = 21
    def compute(self, today, assets, out, msgs):
        out[:] = np.nansum(msgs, axis=0)
        pass
    pass

class BearMessages(CustomFactor):
    inputs = [stocktwits.bear_scored_messages]
    window_length = 21
    def compute(self, today, assets, out, msgs):
        out[:] = np.nansum(msgs, axis=0)
        pass
    pass

class AverageMessageVolume(CustomFactor):
    inputs = [stocktwits.total_scanned_messages]
    window_length = 10
    def compute(self, today, assets, out, msgs):
        out[:] = np.average(msgs, axis=0)
        pass
    pass
