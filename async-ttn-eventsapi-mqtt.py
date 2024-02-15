#!/usr/bin/env python3

import asyncio
import configparser
import json
import sys

import httpx
import paho.mqtt.client as mqtt

# Data format for specific packet reception.
data_dict_up_receive = {
    "raw_payload_size_bytes": 0,
    "rssi_dbm": 0.0,
    "rssi": 0.0,
    "snr_db": 0.0,
    "snr": 0.0,
    "lora_f_cnt": 0,
    "bandwidth_hz": 0,
    "spreading_factor": 0,
    "frequency_hz": 0.0,
    "lora_dev_addr": "",
    "latitude_degrees": 30.1,
    "longitude_degrees": -70.1,
    "altitude_m": 2,
    "_meta": {
        "received_time": "",
        "device_id": "7",
        "platform": "cisco7",
        "band_id": "US_902_928",
        "receiver": "ttn-eventsapi-mqtt",
    },
}

# Data format for gateway statistics.
data_dict_gs_stats = {
    "uplink_count": 0,
    "downlink_count": 0,
    "tx_acknowledgment_count": 0,
    "downlink_utilization_band1": 0.0,
    "_meta": {
        "received_time": "",
        "device_id": "7",
        "platform": "cisco",
        "min_frequency_band1": "923300000",
        "max_frequency_band1": "927500000",
        "receiver": "ttn-eventsapi-mqtt",
    },
}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print("Failed to connect to MQTT broker, reason is {}".format(rc))


def create_pkt_stats(msg):
    pkt = {"_meta": {"receiver": "ttn-eventsapi-mqtt"}}

    # Data
    try:
        pkt["uplink_count"] = int(msg["result"]["data"]["uplink_count"])
    except:
        pass
    try:
        pkt["downlink_count"] = int(msg["result"]["data"]["downlink_count"])
    except:
        pass
    try:
        pkt["tx_acknowledgment_count"] = int(
            msg["result"]["data"]["tx_acknowledgment_count"]
        )
    except:
        pass
    try:
        pkt["downlink_utilization_band1"] = float(
            msg["result"]["data"]["sub_bands"][1]["downlink_utilization"]
        )
    except:
        pass

    # Metadata

    # `received_time` and `device_id` are required, so we are ok to fail if
    # these are missing.
    pkt["_meta"]["received_time"] = str(msg["result"]["time"])
    pkt["_meta"]["device_id"] = msg["result"]["identifiers"][0]["gateway_ids"]["eui"]
    try:
        pkt["_meta"]["platform"] = msg["result"]["data"]["last_status"]["versions"][
            "platform"
        ]
    except:
        pass
    try:
        pkt["_meta"]["min_frequency_band1"] = int(
            msg["result"]["data"]["sub_bands"][1]["min_frequency"]
        )
    except:
        pass
    try:
        pkt["_meta"]["max_frequency_band1"] = int(
            msg["result"]["data"]["sub_bands"][1]["max_frequency"]
        )
    except:
        pass

    return pkt


def create_pkt_receive(rst):
    pkt = {"_meta": {"receiver": "ttn-eventsapi-mqtt"}}

    msg = rst["result"]["data"]["message"]

    raw_payload = msg["raw_payload"]
    # Calculate size in bytes from base64 format
    raw_payload_size_bytes = (3 - raw_payload[-4:].count("=")) + (
        len(raw_payload[:-4]) / 4
    ) * 3

    pkt["raw_payload_size_bytes"] = int(raw_payload_size_bytes)
    try:
        pkt["rssi_dbm"] = int(msg["rx_metadata"][0]["rssi"])
    except:
        pass
    try:
        pkt["rssi"] = int(msg["rx_metadata"][0]["rssi"])
    except:
        pass
    try:
        pkt["bandwidth_hz"] = int(msg["settings"]["data_rate"]["lora"]["bandwidth"])
    except:
        pass
    try:
        pkt["spreading_factor"] = int(
            msg["settings"]["data_rate"]["lora"]["spreading_factor"]
        )
    except:
        pass
    try:
        pkt["frequency_hz"] = int(msg["settings"]["frequency"])
    except:
        pass
    try:
        pkt["latitude_degrees"] = float(msg["rx_metadata"][0]["location"]["latitude"])
    except:
        pass
    try:
        pkt["longitude_degrees"] = float(msg["rx_metadata"][0]["location"]["longitude"])
    except:
        pass
    try:
        pkt["altitude_m"] = float(msg["rx_metadata"][0]["location"]["altitude"])
    except:
        pass
    try:
        pkt["lora_f_cnt"] = int(msg["payload"]["mac_payload"]["f_hdr"]["f_cnt"])
    except:
        pass
    try:
        pkt["lora_dev_addr"] = msg["payload"]["mac_payload"]["f_hdr"]["dev_addr"]
    except:
        pass
    try:
        pkt["snr_db"] = int(msg["rx_metadata"][0]["snr"])
    except:
        pass
    try:
        pkt["snr"] = int(msg["rx_metadata"][0]["snr"])
    except:
        pass

    # Metadata

    pkt["_meta"]["received_time"] = msg["rx_metadata"][0]["received_at"]
    pkt["_meta"]["device_id"] = rst["result"]["identifiers"][0]["gateway_ids"]["eui"]
    try:
        pkt["_meta"]["band_id"] = rst["result"]["data"]["band_id"]
    except:
        pass

    return pkt


async def gateway_info_stream(queue, ttn_server, gateway_id, ttn_key):
    async with httpx.AsyncClient() as client:
        async with client.stream(
            "POST",
            ttn_server,
            headers={
                "Authorization": "Bearer " + ttn_key,
                "Accept": "text/event-stream",
                "Content-type": "application/json; charset=utf-8",
            },
            data='{{"identifiers":[{{"gateway_ids":{{"gateway_id":"{}"}}}}]}}'.format(
                gateway_id
            ),
            timeout=600.00,
        ) as r:
            async for line in r.aiter_text():
                try:
                    message = json.loads(line)
                    if message["result"]["name"] == "gs.gateway.connection.stats":
                        await queue.put(create_pkt_stats(message))

                    if message["result"]["name"] == "gs.up.receive":
                        await queue.put(create_pkt_receive(message))

                except KeyboardInterrupt:
                    print("<< User stream interrupt >>")
                except TimeoutError:
                    sys.exit("Timeout error - TTN events API was idle for longer than 10 minutes.") 
                except Exception as e:
                    print("<< error handling TTN message >>")
                    print(e)
                    print(message)


async def main():
    # First argument is the path to the config file.
    config_path = sys.argv[1]

    # Read in the INI file to get all of the configuration values.
    config = configparser.ConfigParser()
    config.read(config_path)

    # Group all gateway information from all [gateway X] sections.
    gateways = []
    for section in config.sections():
        if section.startswith("gateway"):
            gateways.append(config[section])
    for gateway in gateways:
        print("Using gateway: {} ({})".format(gateway["name"], gateway["id"]))

    # Connect to MQTT broker
    print("Connecting to MQTT broker...")
    client = mqtt.Client(config["mqtt"]["client_id"])
    client.username_pw_set(config["mqtt"]["username"], config["mqtt"]["password"])
    client.tls_set()
    client.on_connect = on_connect
    client.connect(config["mqtt"]["broker"], int(config["mqtt"]["port"]), 60)

    # Load gateway information
    ttn_server = config["ttn"]["server"]
    gateway_ids = [gateway["id"] for gateway in gateways]
    gateway_keys = [gateway["key"] for gateway in gateways]

    # Start async tasks
    print("Starting gateway data stream...")
    queue = asyncio.Queue()
    asyncio.gather(
        *[
            gateway_info_stream(queue, ttn_server, id, key)
            for (id, key) in zip(gateway_ids, gateway_keys)
        ]
    )

    # Start mqtt main loop
    while True:
        try:
            message = await asyncio.wait_for(queue.get(), 600)
            status = client.publish(config["mqtt"]["topic"], json.dumps(message))
            # print(json.dumps(message))
            queue.task_done()
        except TimeoutError:
            sys.exit("Timeout error - ttn packet queue empty for longer than 10 minutes.")




asyncio.run(main())
