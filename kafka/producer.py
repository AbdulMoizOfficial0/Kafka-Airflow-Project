import json
import time
import datetime
import pandas as pd
from kafka import KafkaProducer

tickers = ['BTC-USD', 'SHIB-USD', 'BNB-USD', 'ETH-USD', 'SOL-USD']
interval = '1d'

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    for ticker in tickers:
        period1 = int(time.mktime(datetime.datetime(2020, 8, 1, 23, 59).timetuple()))
        end_date = datetime.datetime.now()
        period2 = int(time.mktime(end_date.timetuple()))

        query_link = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'

        try:
            df = pd.read_csv(query_link)
            for index, row in df.iterrows():
                data = row.to_dict()
                data['ticker'] = ticker
                message = json.dumps(data).encode('utf-8')
                producer.send('-testingkafka', value=message)
        except Exception as e:
            print(f"Error fetching or processing data for {ticker}: {e}")
    producer.flush()
    time.sleep(3600)

producer.close()
