class Kproducer {
  constructor(zookeeper_addr, options={}) {
    var kafka = require('kafka-node'),
        HighLevelProducer = kafka.HighLevelProducer,
        client = new kafka.Client(zookeeper_addr),
        producer = new HighLevelProducer(client,options);

    this.buffer = []; // use buffer to avoid kafka not ready case
    this.producer = producer;
    this.client = client;

    process.on('SIGINT', function() {
      console.log("exiting...");
      client.close(function(){
          console.log("exited");
          process.exit();
      });
    });

    producer.on('ready', () => {
      console.log("kafka producer ready.");
      if (this.buffer.length > 0) {
        this.producer.send(this.buffer, function (err, data) {
            console.log(data);
        });
        this.buffer = [];
      }
    });
  }

  send(topic, messages) {
    if (this.producer.ready) {
      this.producer.send([{"topic": topic, "messages": messages, attributes:2}],function (err, data) {
          console.log(data);
      });
    } else {
      this.buffer.push({"topic": topic, "messages": messages, attributes:2});
      // attributes controls compression of the message set. It supports the following values:

      // 0: No compression
      // 1: Compress using GZip
      // 2: Compress using snappy
    }
  }

  kpub(class_name, function_name, arg)  {
    this.send(`${class_name}.${function_name}`, JSON.stringify(arg));
  }

}

export default Kproducer;