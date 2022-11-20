from confluent_kafka import Consumer
from cassandra.cluster import Cluster
import cassandra_query
import json
################
c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

print('Available topics to consume: ', c.list_topics().topics)
c.subscribe(['myLogs'])
last_message=''
################
#cluster=Cluster(['127.0.0.1:9042'])
#session=cluster.connect('myLogs')
#session.execute('DESCRIBE tables')

def main():
    while True:
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        #cassandra_query.insert("test","test","test","test","test")
        print(data)
        jsonObject=json.loads(data)
        print(jsonObject["device_id"])
        cassandra_query.insert(jsonObject["device_id"],jsonObject["login_date"],jsonObject["device_ip"],jsonObject["device_name"],jsonObject["platform"])
        last_message=data
    c.close()



if __name__ == '__main__':
    main()
