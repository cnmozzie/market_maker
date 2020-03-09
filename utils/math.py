from decimal import Decimal

def toNearest(num, tickSize):
    """Given a number, round it to the nearest tick. Very useful for sussing float error
       out of numbers: e.g. toNearest(401.46, 0.01) -> 401.46, whereas processing is
       normally with floats would give you 401.46000000000004.
       Use this after adding/subtracting/multiplying numbers."""
    tickDec = Decimal(str(tickSize))
    return float((Decimal(round(num / tickSize, 0)) * tickDec))

def harmonicFactor(num, minimum, maximum):
    """Given a number, return a buy factor and a sell factor that indicate how close it 
       is from the minimum and maximum. It's set to range from 0.25 to 1."""
    interval = maximum - minimum
    a = (num - minimum) / interval
    b = (maximum - num) / interval
    if a < b: # need to buy more
        if a < 0.067:
            return (0.25, 4)
        else:
            return (4/(1/a+1/b), (1/a+1/b)/4)
    else: # need to sell more
        if b < 0.067:
            return (4, 0.25)
        else:
            return ((1/a+1/b)/4, 4/(1/a+1/b))
