from configparser import ConfigParser

config = ConfigParser()
configFilePath = 'secrets.cfg'

with open(configFilePath) as f:
    config.read_file(f)

# praw configuration
SUBREDDIT = config.get('reddit', 'subreddit')
CLIENT_ID = config.get('reddit', 'client_id')
CLIENT_SECRET = config.get('reddit', 'client_secret')

# kafka configuration
INPUT_TOPIC = config.get('kafka', 'input_topic')
OUTPUT_TOPIC = config.get('kafka', 'output_topic')
KAFKA_BOOTSTRAP_SERVER = config.get('kafka', 'bootstrap_servers')
