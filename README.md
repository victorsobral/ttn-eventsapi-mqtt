ttn-eventsapi-mqtt
==================

Python application to connect to TTN events API and push data to MQTT.

Setup and Installation
----------------------

Setup virtual environment and install dependencies:

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Create a configuration .ini file using the format below.

For each gateway create a `[gateway X]` section. The `[section]` names can be
anything as long as they are unique and start with `gateway`.

For each gateway you need an API key from TTN. One the Add API key page you need
to select the following permissions:

- View gateway information
- View gateway status
- Read gateway traffic

Configuration file template:

```
[mqtt]
topic = <topic>
broker = <broker url>
port = 8883
username = <username>
password = <port>
client_id = gateway-info-stream

[ttn]
server = https://nam1.cloud.thethings.network/api/v3/events

[gateway 1]
name = <gateway-name>
id = <gateway-id>
key = <api key>

[gateway 2]
name = <gateway-name>
id = <gateway-id>
key = <api key>
```

Run the script from within the virtual environment:

```bash
./async-ttn-eventsapi-mqtt.py <path to configuration file>
```
