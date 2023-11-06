#!/usr/bin/env python3

import asyncio
import httpx
import json
import paho.mqtt.client as mqtt

import data_format
import mqtt_secrets
import ttn_secrets



def on_connect(client, userdata, flags, rc):
        if rc == 0:
                print('Connected to MQTT broker\n')
                pass
        else:
                print('Failed to connect to MQTT broker, reason is {}\n'.format(rc))
                pass


async def gateway_info_stream(queue,TTN_SERVER,GATEWAY_ID,TTN_KEY):

    data_dict_up_receive = data_format.data_dict_up_receive
    data_dict_gs_stats = data_format.data_dict_gs_stats

    async with httpx.AsyncClient() as client:
        async with client.stream("POST",TTN_SERVER,
                                headers={'Authorization': 'Bearer '+TTN_KEY , 'Accept': 'text/event-stream', 'Content-type': 'application/json; charset=utf-8'},
                                data='{"identifiers":[{"gateway_ids":{"gateway_id":"'+GATEWAY_ID+'"}}]}', timeout=None) as r:

            async for line in r.aiter_text():
                try:
                    message = json.loads(line)
                    if message["result"]["name"] == "gs.gateway.connection.stats":
                            data_dict_gs_stats.update({
                                    "uplink_count": int(message["result"]["data"]["uplink_count"]),
                                    "downlink_count": int(message["result"]["data"]["downlink_count"]),
                                    "tx_acknowledgment_count": int(message["result"]["data"]["tx_acknowledgment_count"]),
                                    "downlink_utilization_band1": float(message["result"]["data"]["sub_bands"][1]["downlink_utilization"]),

                                    "_meta": {"received_time": str(message["result"]["time"]),
                                                      "device_id": str(message["result"]["identifiers"][0]["gateway_ids"]["eui"]),
                                              "platform": str(message["result"]["data"]["last_status"]["versions"]["platform"]),
                                              "min_frequency_band1": int(message["result"]["data"]["sub_bands"][1]["min_frequency"]),
                                              "max_frequency_band1": int(message["result"]["data"]["sub_bands"][1]["max_frequency"]),
                                              "receiver": "ttn-eventsapi-mqtt"}
                            })

                            await queue.put(data_dict_gs_stats)

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
                                    "received_time": str(message["result"]["data"]["message"]["rx_metadata"][0]["received_at"]),
                                    "device_id" : str(message["result"]["identifiers"][0]["gateway_ids"]["eui"]),
                                    "band_id" : str(message["result"]["data"]["band_id"]),
                                    "receiver": "ttn-eventsapi-mqtt"
                            })

                            await queue.put(data_dict_up_receive)

                except KeyboardInterrupt:
                        print('<< User stream interrupt >>')
                except:
                        print("<< error handling TTN message >>")


async def main():

    # Initialize mqtt client to push data to DB 
    MQTT_TOPIC_NAME = mqtt_secrets.MQTT_TOPIC_NAME
    MQTT_BROKER = mqtt_secrets.MQTT_BROKER
    MQTT_PORT = mqtt_secrets.MQTT_PORT
    username = mqtt_secrets.MQTT_USER
    password = mqtt_secrets.MQTT_PASSWORD
    client_id = mqtt_secrets.client_id

    # Connect to MQTT broker
    print('conecting to mqtt broker...')
    client = mqtt.Client(client_id)
    client.username_pw_set(username, password)
    client.tls_set()
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start() 

    # Load gateway information
    queue = asyncio.Queue()
    TTN_SERVER = ttn_secrets.TTN_SERVER
    GATEWAYS = ttn_secrets.GATEWAYS
    ID_list = [gateway['GATEWAY_ID'] for gateway in GATEWAYS]
    KEY_list = [gateway['TTN_KEY'] for gateway in GATEWAYS]

    # Start async tasks
    print('starting gateway data stream...')
    asyncio.gather(*[gateway_info_stream(queue,TTN_SERVER,ID,KEY) for (ID,KEY) in zip(ID_list,KEY_list)])

    # Start mqtt main loop
    while True:
        message = await queue.get()
        status = client.publish(MQTT_TOPIC_NAME, json.dumps(message))
        # print(json.dumps(message))
        queue.task_done()
        

asyncio.run(main())
