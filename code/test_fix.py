
import risk_return
from pprint import pprint

def test():
    res = risk_return.process_single_stock('AAPL')
    pprint(res)

if __name__ == "__main__":
    test()
