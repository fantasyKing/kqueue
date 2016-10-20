const _ = require('lodash');

class Kproducer {
  constructor(zookeeper_addr, options = {}) {
    const kafka = require('kafka-node');
    const HighLevelProducer = kafka.HighLevelProducer;
    const client = new kafka.Client(zookeeper_addr);
    const producer = new HighLevelProducer(client, options);

    this.buffer = []; // use buffer to avoid kafka not ready case
    this.producer = producer;
    this.client = client;

    process.on('SIGINT', () => {
      console.log('exiting...');
      client.close(() => {
        console.log('exited');
        process.exit();
      });
    });

    producer.on('ready', () => {
      console.log('kafka producer ready.');
      if (this.buffer.length > 0) {
        this.producer.send(this.buffer, (err, data) => {
          console.log(data);
        });
        this.buffer = [];
      }
    });

    producer.on('error', (e) => {
      console.log('kafka producer error:');
      console.log(e);
    });
  }

  send(topic, messages) {
    if (this.producer.ready) {
      this.producer.send([{ topic, messages, attributes: 2 }], (err, data) => {
        if (err) {
          console.log(err);
          console.log(`${topic} Connect error,request push to memory buffer and wait for reconnect.`);

          const exist_topic = _.find(this.buffer, { topic });
          if (exist_topic) {
            exist_topic.messages = _.concat(exist_topic.messages, messages);
          } else {
            this.buffer.push({ topic, messages, attributes: 2 });
          }
        } else {
          console.log(data);
        }
      });
    } else {
      const exist_topic = _.find(this.buffer, { topic });
      if (exist_topic) {
        exist_topic.messages = _.concat(exist_topic.messages, messages);
      } else {
        this.buffer.push({ topic, messages, attributes: 2 });
      }
      // attributes controls compression of the message set. It supports the following values:

      // 0: No compression
      // 1: Compress using GZip
      // 2: Compress using snappy
    }
  }

  kpub(class_name, function_name, arg) {
    this.send(`${class_name}.${function_name}`, JSON.stringify(arg));
  }

}

export default Kproducer;
