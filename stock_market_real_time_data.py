import yfinance as yf
from confluent_kafka import Producer

#kafka config

producer = Producer({
    'bootstrap.servers' : 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanism' : 'PLAIN',
    'security.protocol' : 'SASL_SSL',
    'sasl.username' : 'L2PHCI7CA2YHFFO6',
    'sasl.password' : 'QEDnmKiwOug0/gZBd/Ru7+ajAVx+1C/nD6KOJtKscZ7u6ofQSFQ4wruNXiZgpUuw',
})

def fetch_and_produce_stock_price(symbol):
    # try:
    stock = yf.Ticker(symbol)
    price = stock.history(period='1m')['Close'].iloc[-1]
    message = f'{symbol} : {price}'
    producer.produce('stock_price',value=message)
    producer.flush()
    print(f'sent:{symbol} - price:{price}')

    # except Exception as e:
        # print(f'Error sending date')

fetch_and_produce_stock_price('GOOG')    