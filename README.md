# news-entity-counter

### Execution steps:

- Install all required libraries using:
```bash
pip install -r requirements.txt
```

- Install spacy requirements using:
```bash
python -m spacy download en_core_web_sm
```

update the environment variables in the `.env` file with your API key, Kafka broker address and topic names.

- Run the producer using:
```bash
python kafka_producer.py
```

- Run the consumer using:
```bash
spark-submit --master "local[3]" --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark_processor.py
```

- Run logstash using, use the logstash.conf file included in the repository:
```bash
bin/logstash -f path/to/logstash.conf
```

NOTE: logstash cmd needs to be executed from your logstash directory.
