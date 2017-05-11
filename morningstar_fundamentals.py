"""
Author: Ian Doherty - idohertyr
Date: Mar 29, 2017

This algorithm pulls Morningstar fundamental data daily.

"""

# Import libraries
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import CustomFactor, SimpleMovingAverage, AverageDollarVolume, Latest, RSI
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data.psychsignal import stocktwits
import numpy as np

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Record variables every day, 1 minutes before market close.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close(minutes=1))

    # Define Stock List
    context.stocks = list()
    pass

def before_trading_start(context, data):
    """
    Called every day before market open.
    
    """
    #Morningstar - Fundamental Data
    fundamental_df = get_fundamentals(
        # Retrieve data based on PE ratio and economic sector
        query(
            fundamentals.asset_classification.growth_grade,
            fundamentals.asset_classification.profitability_grade,
        )
            # Only take profitability grade that equals A
            .filter(fundamentals.asset_classification.profitability_grade == 'A')
            # Only take growth rate of A
            .filter(fundamentals.asset_classification.growth_grade == 'A')
    )

    context.fundamental_df = fundamental_df

    log.info(context.fundamental_df)

    pass

def my_assign_weights(context, data):
    """
    my_assign_weights
    """
    pass

def my_rebalance(context,data):
    """
    my_rebalance
    """
    pass

def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    record(open_positions=len(context.portfolio.positions))
    pass

def handle_data(context,data):
    """
    Called every minute.
    """
    pass