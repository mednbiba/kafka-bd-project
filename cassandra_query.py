from cassandra.cluster import Cluster
def insert(date,device_id,device_ip,device_name,platform):
	cluster=Cluster(['127.0.0.1'],port=9042)
	session = cluster.connect('cycling')
	session.execute("INSERT INTO logs (id,device_id,date,device_ip,device_name,platform) VALUES (uuid(),'"+str(device_id)+"','"+str(date)+"','"+str(device_ip)+"','"+str(device_name)+"','"+str(platform)+"')")
	print('INSERTED VALUES')
#tables=session.execute("DESC TABLES")
#print(tables.ResultSet)
if __name__=="__main__":
	print("testing lol values")
	insert('lol','lol','lol','lol','lol')
