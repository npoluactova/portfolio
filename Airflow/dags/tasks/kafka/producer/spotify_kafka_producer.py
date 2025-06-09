import os
import json
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from confluent_kafka import Producer

# to create universal counter to track how many messages were successfully sent and how mane failed
class KafkaDeliveryTracker:
    def __init__(self):
        self.success = 0
        self.failed = 0

    def kafka_custom_callback(self, err, msg):
        if err:
            print(f"Message delivery failed: {err}")
            self.failed += 1
        else:
            # print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
            self.success += 1

def get_params_from_yaml(params_file):
    try:
        with open(params_file, "r") as file:
            params = yaml.safe_load(file)
        return params
    except FileNotFoundError:
        print(f"Error: file {params_file} was not found")
    except yaml.YAMLError:
        print(f"Error: file {params_file} was incorrect")

def get_params_from_json(params_file):
    try:
        with open(params_file, "r") as file:
            params = json.load(file)
        return params
    except FileNotFoundError:
        print(f"Error: file {params_file} was not found")
    except json.JSONDecodeError:
        print(f"Error: file {params_file} was incorrect")

# build engine for any db connection (postgres, sql)
def build_engine(drivername, db_params, creds):
    DATABASE_URL = URL.create(
        drivername=drivername,
        username=creds.get('user'),
        password=creds.get('password'),
        host=db_params.get('host'),
        port=db_params.get('port'),
        database=db_params.get('db_name')
    )
    # DATABASE_URL = f"postgresql://{creds.get('DB_USER')}:{creds.get('DB_PASSWORD')}@{db_params.get('DB_HOST')}:{db_params.get('DB_PORT')}/{db_params.get('DB_NAME')}"
    engine = create_engine(DATABASE_URL)
    return engine

# read table from postgres with rows and columns names
def read_from_postgres(engine,query):
    result_data = []
    with engine.connect() as conn:
        result = conn.execute(text(query))
        # get columns
        columns = result.keys()
        # get every row in table
        for row in result:
            result_data.append(dict(zip(columns, row))) # zip - pairs rows and columns, dict - creates dict in Python
    return result_data

# method to produce data in kafka topic  row by row
def kafka_producer(config, data):
    tracker = KafkaDeliveryTracker()
    conf = {
        'bootstrap.servers': config['bootstrap_server']
    }
    topic = config['produce_topic']
    producer = Producer(conf)

    for record in data:
        payload = json.dumps(record) #make json value
        producer.produce(topic=topic,
                         key=record['id'],
                         value=payload,
                         callback=tracker.kafka_custom_callback)
        producer.poll(0)  #call callbacks after every message sent
    # after everything was sent additionally send flush to be sure all messages were delivered to kafka
    producer.flush()
    print(f"Kafka delivery report to topic {topic}")
    print(f"Sent messages: {len(data)}")
    print(f"Delivered messages: {tracker.success}")
    print(f"Failed messages:  {tracker.failed}")


def main():
    # get current dir, where creds and config are placed
    current_dir = os.path.dirname(os.path.abspath(__file__))
    creds_json_path = os.path.join(current_dir, "creds.json")
    config_yaml_path = os.path.join(current_dir, "config.yaml")

    # get creds from json
    creds = get_params_from_json(params_file=creds_json_path)
    # get database config from common config.yaml
    config = get_params_from_yaml(params_file=config_yaml_path)
    db_config = config['database']
    # get kafka config from common config.yaml
    kafka_config = config['kafka']

    # build engine for postgres connection
    postgres_engine = build_engine(drivername="postgresql",
                                   db_params=db_config,
                                   creds=creds['postgres'])
    # get table to read
    spotify_table = config['database']['scheme'] + '.' + config['database']['table_name']
    query = f"SELECT * FROM {spotify_table}"

    # read from postgres
    spotify_data = read_from_postgres(engine=postgres_engine,
                                      query=query)
    # produce data from postgres to kafka
    kafka_producer(config=kafka_config,
                   data=spotify_data)

if __name__ == "__main__":
    main()