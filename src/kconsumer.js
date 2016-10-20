class Kconsumer {
  constructor(zookeeper_addr, options = {}) {
    const kafka = require('kafka-node');
    const HighLevelConsumer = kafka.HighLevelConsumer;
    const client = new kafka.Client(zookeeper_addr);
    const consumer = new HighLevelConsumer(client, options);
    this.consumer = consumer;

    process.on('SIGINT', () => {
      console.log('exiting...');
      consumer.close(true, () => {
        client.close(() => {
          console.log('exited');
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
    console.log(`an message comes ${message}`);
  }
}

export default Kconsumer;
