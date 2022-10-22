from fin_crawler import FinCrawler

if __name__ == '__main__':
    stock_price = FinCrawler.get('tw_stock_price_daily',{'date':'20220920'})
    print(stock_price)