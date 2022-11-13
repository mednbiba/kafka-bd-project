from confluent_kafka import Consumer
from cassandra.cluster import Cluster
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

        print(data)
        last_message=data
    c.close()



if __name__ == '__main__':
    main()
