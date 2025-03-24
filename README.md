# ipfix-flow-sampling

## Installation
To create a Python virtual environment and install the required packages, run the following commands:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Setting up the Environment
Create a `.env` file in the root directory of your project and add the necessary environment variables:
```
SCHEMA_REGISTRY_URL=
AVRO_OUTPUT_SCHEMA_SUBJECT=

CONSUMER_BOOTSTRAP_SERVERS=
CONSUMER_SECURITY_PROTOCOL=
CONSUMER_SASL_MECHANISMS= # required if security protocol is SASL_SSL
CONSUMER_SASL_USERNAME= # required if security protocol is SASL_SSL
CONSUMER_SASL_PASSWORD= # required if security protocol is SASL_SSL
CONSUMER_SSL_CERTIFICATE_LOCATION= # required if security protocol is SSL
CONSUMER_SSL_KEY_LOCATION= # required if security protocol is SSL
CONSUMER_SSL_CA_LOCATION= # required if security protocol is SSL
CONSUMER_AUTO_OFFSET_RESET=latest
CONSUMER_ID=your_consumer_id

PRODUCER_BOOTSTRAP_SERVERS=
PRODUCER_SECURITY_PROTOCOL=PLAINTEXT
PRODUCER_SASL_MECHANISMS=
PRODUCER_SASL_USERNAME=
PRODUCER_SASL_PASSWORD=

INPUT_TOPIC=
OUTPUT_TOPIC=
```

## Running the Application
To run the application, use the following command inside the virtual environment:

```bash
python3 main.py
```
