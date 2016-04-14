class Kconsumer {
  constructor(zookeeper_addr, options = {}) {
    var kafka = require('kafka-node'),
        HighLevelConsumer = kafka.HighLevelConsumer,
        client = new kafka.Client(zookeeper_addr),
        consumer = new HighLevelConsumer(client, options);
    this.consumer = consumer;

    process.on('SIGINT', function() {
      console.log("exiting...");
      consumer.close(true, function(){
        client.close(function(){
          console.log("exited");
          process.exit();
        });
      });
    });

    consumer.on('message', (message) => {
      console.log(message);
      this.onMessage(message);
    });
  }

  onMessage(message) {
    console.log('an message comes'+message);
  }
}

export default Kconsumer;