# trigger kafka batch job.

# create API key and secret in confluent cloud once after creating a cluster.
# then attatch the API key and secret to the topic you create in confluent cloud.

# Install this libraries in databricks.
# confluent-kafka[avro,json,protobuf]>=1.4.2

# run this code in databricks.

#============================
# Writing to a kakfka topic =
#============================


from confluent_kafka import Producer


import socket, json

# https://confluent.cloud/environments/env-v8jrqz/clusters/lkc-mo88oq/settings/kafka
conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'OY5RFX5NYTEKIM5L',
        'sasl.password': 'cfltozF8wqRt2rxIP4U8RQ9fxIGtxSQL5f6OZo5on6yk3yta0yUTVMLvhiNWzWNg',
        'client.id': 'ccloud-python-client-0a866ea9-6bbd-4a3a-8b70-e4ba9bf6f647'}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Workspace/Users/gauravmishra7080@gmail.com/Drafts/order_input.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = 'topic_kafka_vs', key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()

# msg produced: <cimpl.Message object at 0x7ff57f32d8c0>
# msg produced key in binary is: b'5225' & msg produced value in binary is b'{"order_id":50,"customer_id":5225,"customer_fname":"Mary","customer_lname":"Smith","city":"Peabody","state":"MA","pincode":1960,"line_items":[{"order_item_id":124,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":125,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98}]}'

# msg produced key in string is: b'5225' & msg produced value in binary is b'{"order_id":50,"customer_id":5225,"customer_fname":"Mary","customer_lname":"Smith","city":"Peabody","state":"MA","pincode":1960,"line_items":[{"order_item_id":124,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":125,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98}]}'


#============================
# Reading from a kakfka topic =
#============================

# get these details from confluent kafka, search it on google.
confluentBootstrapServers = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
confluentApiKey = 'OY5RFX5NYTEKIM5L'
confluentSecret = 'cfltozF8wqRt2rxIP4U8RQ9fxIGtxSQL5f6OZo5on6yk3yta0yUTVMLvhiNWzWNg'
# we created the topic in confluent kafka.
confluentTopicName = 'topic_kafka_vs'

if __name__ == '__main__':
    
    orders_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers",confluentBootstrapServers) \
    .option("kafka.security.protocol","SASL_SSL") \
    .option("kafka.sasl.mechanism","PLAIN") \
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm","https") \
    .option("subscribe",confluentTopicName) \
    .load()

    print(orders_df.head(5), 'display the first 5 rows before casting')

    # [Row(key=bytearray(b'8827'), value=bytearray(b'{"order_id":4,"customer_id":8827,"customer_fname":"Brian","customer_lname":"Wilson","city":"San Antonio","state":"TX","pincode":78240,"line_items":[{"order_item_id":5,"order_item_product_id":897,"order_item_quantity":2,"order_item_product_price":24.99,"order_item_subtotal":49.98},{"order_item_id":6,"order_item_product_id":365,"order_item_quantity":5,"order_item_product_price":59.99,"order_item_subtotal":299.95},{"order_item_id":7,"order_item_product_id":502,"order_item_quantity":3,"order_item_product_price":50.0,"order_item_subtotal":150.0},{"order_item_id":8,"order_item_product_id":1014,"order_item_quantity":4,"order_item_product_price":49.98,"order_item_subtotal":199.92}]}\n'), topic='topic_kafka_vs', partition=4, offset=0, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 28, 761000), timestampType=0), 
     
    # Row(key=bytearray(b'11318'), value=bytearray(b'{"order_id":5,"customer_id":11318,"customer_fname":"Mary","customer_lname":"Henry","city":"Caguas","state":"PR","pincode":725,"line_items":[{"order_item_id":9,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98},{"order_item_id":10,"order_item_product_id":365,"order_item_quantity":5,"order_item_product_price":59.99,"order_item_subtotal":299.95},{"order_item_id":11,"order_item_product_id":1014,"order_item_quantity":2,"order_item_product_price":49.98,"order_item_subtotal":99.96},{"order_item_id":12,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98},{"order_item_id":13,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99}]}\n'), topic='topic_kafka_vs', partition=4, offset=1, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 30, 245000), timestampType=0),
    
    # Row(key=bytearray(b'9488'), value=bytearray(b'{"order_id":19,"customer_id":9488,"customer_fname":"Mary","customer_lname":"Smith","city":"Hialeah","state":"FL","pincode":33012,"line_items":[{"order_item_id":58,"order_item_product_id":1004,"order_item_quantity":1,"order_item_product_price":399.98,"order_item_subtotal":399.98},{"order_item_id":59,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98}]}\n'), topic='topic_kafka_vs', partition=4, offset=2, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 35, 896000), timestampType=0),
    
    # Row(key=bytearray(b'9198'), value=bytearray(b'{"order_id":20,"customer_id":9198,"customer_fname":"David","customer_lname":"Kerr","city":"Bowling Green","state":"KY","pincode":42101,"line_items":[{"order_item_id":60,"order_item_product_id":502,"order_item_quantity":5,"order_item_product_price":50.0,"order_item_subtotal":250.0},{"order_item_id":61,"order_item_product_id":1014,"order_item_quantity":4,"order_item_product_price":49.98,"order_item_subtotal":199.92},{"order_item_id":62,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":63,"order_item_product_id":365,"order_item_quantity":5,"order_item_product_price":59.99,"order_item_subtotal":299.95}]}\n'), topic='topic_kafka_vs', partition=4, offset=3, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 36, 112000), timestampType=0),
    
    # Row(key=bytearray(b'196'), value=bytearray(b'{"order_id":29,"customer_id":196,"customer_fname":"Thomas","customer_lname":"Watson","city":"Dearborn","state":"MI","pincode":48126,"line_items":[{"order_item_id":83,"order_item_product_id":1073,"order_item_quantity":1,"order_item_product_price":199.99,"order_item_subtotal":199.99},{"order_item_id":84,"order_item_product_id":1014,"order_item_quantity":5,"order_item_product_price":49.98,"order_item_subtotal":249.9},{"order_item_id":85,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":86,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":87,"order_item_product_id":1004,"order_item_quantity":1,"order_item_product_price":399.98,"order_item_subtotal":399.98}]}\n'), topic='topic_kafka_vs', partition=4, offset=4, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 37, 647000), timestampType=0)]  
    
    # display the first 5 rows before casting

    converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")

    print(converted_orders_df.head(5), "display the first 10 rows after casting")

    # [Row(key='8827', value='{"order_id":4,"customer_id":8827,"customer_fname":"Brian","customer_lname":"Wilson","city":"San Antonio","state":"TX","pincode":78240,"line_items":[{"order_item_id":5,"order_item_product_id":897,"order_item_quantity":2,"order_item_product_price":24.99,"order_item_subtotal":49.98},{"order_item_id":6,"order_item_product_id":365,"order_item_quantity":5,"order_item_product_price":59.99,"order_item_subtotal":299.95},{"order_item_id":7,"order_item_product_id":502,"order_item_quantity":3,"order_item_product_price":50.0,"order_item_subtotal":150.0},{"order_item_id":8,"order_item_product_id":1014,"order_item_quantity":4,"order_item_product_price":49.98,"order_item_subtotal":199.92}]}\n', topic='topic_kafka_vs', partition=4, offset=0, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 28, 761000), timestampType=0),
     
    # Row(key='11318', value='{"order_id":5,"customer_id":11318,"customer_fname":"Mary","customer_lname":"Henry","city":"Caguas","state":"PR","pincode":725,"line_items":[{"order_item_id":9,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98},{"order_item_id":10,"order_item_product_id":365,"order_item_quantity":5,"order_item_product_price":59.99,"order_item_subtotal":299.95},{"order_item_id":11,"order_item_product_id":1014,"order_item_quantity":2,"order_item_product_price":49.98,"order_item_subtotal":99.96},{"order_item_id":12,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98},{"order_item_id":13,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99}]}\n', topic='topic_kafka_vs', partition=4, offset=1, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 30, 245000), timestampType=0),
    
    
    # Row(key='9488', value='{"order_id":19,"customer_id":9488,"customer_fname":"Mary","customer_lname":"Smith","city":"Hialeah","state":"FL","pincode":33012,"line_items":[{"order_item_id":58,"order_item_product_id":1004,"order_item_quantity":1,"order_item_product_price":399.98,"order_item_subtotal":399.98},{"order_item_id":59,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98}]}\n', topic='topic_kafka_vs', partition=4, offset=2, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 35, 896000), timestampType=0),
    
    
    # Row(key='9198', value='{"order_id":20,"customer_id":9198,"customer_fname":"David","customer_lname":"Kerr","city":"Bowling Green","state":"KY","pincode":42101,"line_items":[{"order_item_id":60,"order_item_product_id":502,"order_item_quantity":5,"order_item_product_price":50.0,"order_item_subtotal":250.0},{"order_item_id":61,"order_item_product_id":1014,"order_item_quantity":4,"order_item_product_price":49.98,"order_item_subtotal":199.92},{"order_item_id":62,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":63,"order_item_product_id":365,"order_item_quantity":5,"order_item_product_price":59.99,"order_item_subtotal":299.95}]}\n', topic='topic_kafka_vs', partition=4, offset=3, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 36, 112000), timestampType=0),
    
    
    # Row(key='196', value='{"order_id":29,"customer_id":196,"customer_fname":"Thomas","customer_lname":"Watson","city":"Dearborn","state":"MI","pincode":48126,"line_items":[{"order_item_id":83,"order_item_product_id":1073,"order_item_quantity":1,"order_item_product_price":199.99,"order_item_subtotal":199.99},{"order_item_id":84,"order_item_product_id":1014,"order_item_quantity":5,"order_item_product_price":49.98,"order_item_subtotal":249.9},{"order_item_id":85,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":86,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99},{"order_item_id":87,"order_item_product_id":1004,"order_item_quantity":1,"order_item_product_price":399.98,"order_item_subtotal":399.98}]}\n', topic='topic_kafka_vs', partition=4, offset=4, timestamp=datetime.datetime(2025, 12, 31, 8, 15, 37, 647000), timestampType=0)] 
    
    # display the first 10 rows after casting
