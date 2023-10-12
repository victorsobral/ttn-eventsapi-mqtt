from paho.mqtt import client as mqtt
import mqtt_secrets
import ttn_secrets
import data_format
import requests
import json


'''mqtt_init() initializes the parameters needed to make a connection with and publish data to the MQTT broker. These values are read from
    the mqtt_secrets file, so any changes have to go there. This is especially true for client ID, unless you are able to read the MAC address
    for your device once during the initialization phase, so you don't have to enter it for every unique device'''
def mqtt_init():
	global MQTT_TOPIC_NAME, MQTT_BROKER, MQTT_PORT, username, password, client_id
	MQTT_TOPIC_NAME = mqtt_secrets.MQTT_TOPIC_NAME
	MQTT_BROKER = mqtt_secrets.MQTT_BROKER
	MQTT_PORT = mqtt_secrets.MQTT_PORT
	username = mqtt_secrets.MQTT_USER
	password = mqtt_secrets.MQTT_PASSWORD
	client_id = mqtt_secrets.client_id


''' dict_init() initializes all data dicts to be streammed through MQTT'''
def dict_init():
	global data_dict_up_receive, data_dict_gs_stats
	data_dict_up_receive = data_format.data_dict_up_receive
	data_dict_gs_stats = data_format.data_dict_gs_stats


''' ttn_init() initializes all the TTN parameters'''
def ttn_init():
	global TTN_SERVER, TTN_KEY, GATEWAY_ID
	TTN_SERVER = ttn_secrets.TTN_SERVER
	TTN_KEY = ttn_secrets.TTN_KEY
	GATEWAY_ID = ttn_secrets.GATEWAY_ID


'''on_connect() is a built in paho-mqtt function that returns the status of the connection when a connect request is made'''
def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print('Connected to MQTT broker\n')
		pass
	else:
		print('Failed to connect to MQTT broker, reason is {}\n'.format(rc))
		pass


'''mqtt_connect() connects to the mqtt broker and returns the client object'''
def mqtt_connect():

	global username, password, MQTT_BROKER, MQTT_PORT, client_id
	client = mqtt.Client(client_id)
	client.username_pw_set(username, password)
	client.tls_set()
	client.on_connect = on_connect
	client.connect(MQTT_BROKER, MQTT_PORT, 60)
	return client
	

mqtt_init()                         # initializing the mqtt instance
dict_init()     					# initializing the dictionary for our mqtt publish
ttn_init()       					# initializing the ttn keys
mqtt_client = mqtt_connect()        # this creates the connection and we will refer to the returned client object as mqtt_client and perform operations 
                                    # like looping, disconnecting, reconnecting, etc. on this
mqtt_client.loop_start()            # all mqtt connections should be looped everytime you want to publish. 

while True:
	try:


		resp = requests.Session().request("POST",TTN_SERVER,
			headers={'Authorization': 'Bearer '+TTN_KEY , 'Accept': 'text/event-stream', 'Content-type': 'application/json; charset=utf-8'},
			data='{"identifiers":[{"gateway_ids":{"gateway_id":"'+GATEWAY_ID+'"}}]}', stream=True)


		for line in resp.iter_lines():
			if line:
				message = json.loads(line.decode('utf8'))
				print(message["result"]["name"])
				if message["result"]["name"] == "gs.gateway.connection.stats":
					data_dict_gs_stats.update({
						"uplink_count": int(message["result"]["data"]["uplink_count"]),
						"downlink_count": int(message["result"]["data"]["downlink_count"]),
						"tx_acknowledgment_count": int(message["result"]["data"]["tx_acknowledgment_count"]),
						"downlink_utilization_band1": float(message["result"]["data"]["sub_bands"][1]["downlink_utilization"]),

						"_meta": {"device_id": str(message["result"]["identifiers"][0]["gateway_ids"]["eui"]),
						          "platform": str(message["result"]["data"]["last_status"]["versions"]["platform"]),
						          "min_frequency_band1": int(message["result"]["data"]["sub_bands"][1]["min_frequency"]),
						          "max_frequency_band1": int(message["result"]["data"]["sub_bands"][1]["max_frequency"]),
						          "receiver": "ttn-eventsapi-mqtt"}
					})


					print(json.dumps(data_dict_gs_stats, indent=2))

				if message["result"]["name"] == "gs.up.receive":
					raw_payload = message["result"]["data"]["message"]["raw_payload"]
					raw_payload_size_bytes = (3-raw_payload[-4:].count('='))+(len(raw_payload[:-4])/4)*3  #calculate size in bytes from base64 format
					data_dict_up_receive.update({
						"raw_payload_size_bytes": int(raw_payload_size_bytes),
						"rssi_dbm": int(message["result"]["data"]["message"]["rx_metadata"][0]["rssi"]),
						"rssi": int(message["result"]["data"]["message"]["rx_metadata"][0]["rssi"]),
						"bandwidth_hz":int(message["result"]["data"]["message"]["settings"]["data_rate"]["lora"]["bandwidth"]),
						"spreading_factor":int(message["result"]["data"]["message"]["settings"]["data_rate"]["lora"]["spreading_factor"]),
						"frequency_hz":int(message["result"]["data"]["message"]["settings"]["frequency"]),
						"received_at":str(message["result"]["data"]["message"]["rx_metadata"][0]["received_at"]),
						"latitude_degrees": float(message["result"]["data"]["message"]["rx_metadata"][0]["location"]["latitude"]),
						"longitude_degrees": float(message["result"]["data"]["message"]["rx_metadata"][0]["location"]["longitude"]),
						"altitude_m": float(message["result"]["data"]["message"]["rx_metadata"][0]["location"]["altitude"])
		            })

					if "mac_payload" in message["result"]["data"]["message"]["payload"]:
						if "f_cnt" in message["result"]["data"]["message"]["payload"]["mac_payload"]["f_hdr"]:
							data_dict_up_receive.update({"lora_f_cnt":int(message["result"]["data"]["message"]["payload"]["mac_payload"]["f_hdr"]["f_cnt"])})
						else:
							if "lora_f_cnt" in data_dict_up_receive:
								del data_dict_up_receive["lora_f_cnt"]
						if "dev_addr" in message["result"]["data"]["message"]["payload"]["mac_payload"]["f_hdr"]:
							data_dict_up_receive.update({"lora_dev_addr":str(message["result"]["data"]["message"]["payload"]["mac_payload"]["f_hdr"]["dev_addr"])})
						else:
							if "lora_dev_addr" in data_dict_up_receive:
								del data_dict_up_receive["lora_dev_addr"]

					if "snr" in message["result"]["data"]["message"]["rx_metadata"][0]:
						data_dict_up_receive.update({"snr_db": int(message["result"]["data"]["message"]["rx_metadata"][0]["snr"]),
							"snr": int(message["result"]["data"]["message"]["rx_metadata"][0]["snr"])})
					else:
						if "snr_db" in data_dict_up_receive:
							del data_dict_up_receive["snr_db"]
							del data_dict_up_receive["snr"]

					data_dict_up_receive["_meta"].update({
		            	"device_id" : message["result"]["identifiers"][0]["gateway_ids"]["eui"], 
						"band_id" : message["result"]["data"]["band_id"],
						"receiver": "ttn-eventsapi-mqtt"
					})

					print(json.dumps(data_dict_up_receive, indent=2))

		# data_dict.update({"value": value})      # update our dictionary's value with the newly generated one
		# status = mqtt_client.publish(MQTT_TOPIC_NAME, json.dumps(data_dict))    # publish the data (we use json.dumps(dict) because published messages have to be strings)
        # This commented block below is used to check if the message was sent successfully. It is a one time operation, but you need to 
        # uncomment the verify variable at the top of the code. It is not mandatory. 
        # if verify:
		# 	print("{}, {}". format(value, json.dumps(data_dict)))
		# 	verify = 0
		# 	if status[0] == 0:
		# 		print("Sent")
		# 	else:
		# 		print("failed")
	except KeyboardInterrupt:
			print('User interrupted, exiting the loop now')
			break

mqtt_client.loop_stop()
mqtt_client.disconnect()
print('Client disconnected from the broker. No longer sending any data.')


mqtt_connect()
