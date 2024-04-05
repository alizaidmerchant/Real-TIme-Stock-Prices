from confluent_kafka import Consumer, KafkaError

#kafka config

consumer = Consumer({
    'bootstrap.servers' : 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanism' : 'PLAIN',
    'security.protocol' : 'SASL_SSL',
    'sasl.username' : 'L2PHCI7CA2YHFFO6',
    'sasl.password' : 'QEDnmKiwOug0/gZBd/Ru7+ajAVx+1C/nD6KOJtKscZ7u6ofQSFQ4wruNXiZgpUuw',
    'group.id' : 'stock_price_group',
    'auto.offset.reset' : 'latest',
})


consumer.subscribe(['stock_price'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f'Error while continuing : {msg.error()}')
        else:
            value = msg.value().decode('utf-8')
            symbol , price = value.split(':')
            print(f'Recieved {symbol} price:{price}')

except KeyboardInterrupt:
    pass
finally:
    consumer.close()


