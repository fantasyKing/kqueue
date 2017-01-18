const _ = require('lodash');
const async = require('async');

class KproducerLoad {
  constructor(zookeeper_addr, options = {}) {
    const kafka = require('kafka-node');
    const HighLevelProducer = kafka.HighLevelProducer;
    const client = new kafka.Client(zookeeper_addr);
    const producer = new HighLevelProducer(client, {
      requireAcks: 1,
      ackTimeoutMs: 100,
      partitionerType: 2
    });

    this.buffer = []; // use buffer to avoid kafka not ready case
    this.producer = producer;
    this.client = client;

    function handlerSend(date) {
      const self = this;
      return function (cb){
        producer.send(date, (err, value) => {
          if (err){
            cb(err, null);
          }

          cb(null, value);
        });

      }
    }

    process.on('SIGINT', () => {
      console.log('exiting...');
      client.close(() => {
        console.log('exited');
        process.exit();
      });
    });

    producer.on('ready', () => {
      console.log('kafka producer ready.');

      const sendArr = [];
      this.buffer.forEach(v => {
        sendArr.push(handlerSend(v));
      });

      async.series(sendArr, function (err, result) {
        if (err) {
          console.log('kafka producer send error: ' + JSON.stringify(error));
        }
        console.log('kafka producer send data: ' + JSON.stringify(result));
      });

      this.buffer = [];
    });

    producer.on('error', (e) => {
      console.log('kafka producer error:');
      console.log(e);
    });
  }

  send(topic, messages) {
    if (this.producer.ready) {
      this.producer.send([{ topic, messages, attributes: 0 }], (err, data) => {
        if (err) {
          console.log(`topic:${topic} messages:${messages} Connect error,request push to memory buffer and wait for reconnect.`);

          const topicMessage = [];
          topicMessage.push({ topic, messages, attributes: 0 });
          this.buffer.push(topicMessage);
        } else {
          console.log(data);
        }
      });
    } else {
      const topicMessage = [];
      topicMessage.push({ topic, messages, attributes: 0 });
      this.buffer.push(topicMessage);
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

export default KproducerLoad;

