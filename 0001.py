from paho.mqtt import client as mqtt_client
import paho.mqtt.client as mqtt
import uuid
# Bibliotecas PostgreSql
import json
import psycopg2
  
HOST = ""
PORT = 1883
TOPIC = ""
CLIENT_ID = str(uuid.uuid1())
USER = ""
PASSWD = ""
DATABASE = ''
USERDB = ''
PASSDB = ''
HOSTDB = ''

print("Iniciando")
print("Connecting to broker with client ID '{}'...".format(CLIENT_ID))

def connect_mqtt() -> mqtt_client:
  def on_connect(client, userdata, flags, rc):
    if rc == 0:
      print("Connected to MQTT Broker!")
    else:
      print("Failed to connect!\n")
  client = mqtt_client.Client(CLIENT_ID, clean_session=False)
  client.username_pw_set(USER, PASSWD)
  client.on_connect = on_connect
  client.connect(HOST, PORT)
  return client

def subscribe(client: mqtt_client):
  def on_message(client, userdata, msg):
    print(f'Subscribed to {TOPIC}')
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic with QOS {msg.qos}")
    obj = json.loads(msg.payload.decode())
    nser = (obj['nser'])
    rssi = (obj['rssi'])
    temp = (obj['temp'])
    long = (obj['long'])
    lat = (obj['lat'])
    try:
      connection = psycopg2.connect(host=HOSTDB,
                                    database=DATABASE,
                                    user=USERDB,
                                    password=PASSDB)
      postgresql_insert_query = f"INSERT INTO temp (nser, rssi, temp, long, lat) VALUES ('{nser}', {rssi}, {temp}, {long}, {lat})"
      cursor = connection.cursor()
      cursor.execute(postgresql_insert_query)
      connection.commit()
      print(cursor.rowcount, "Data entered successfully")
      cursor.close()
      connection.close()
      print("Closed connection")
    except (Exception, psycopg2.DatabaseError) as error:
      print("Database failure".format(error))
  print('teste')
  client.subscribe(TOPIC)
  client.on_message = on_message

def run():
  client = connect_mqtt()
  subscribe(client)
  client.loop_forever()

if __name__ == '__main__':
  run()
  
