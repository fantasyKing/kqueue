var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var client = new kafka.Client('192.168.200.150:2181/kafka/q-rax306gz');
var producer = new HighLevelProducer(client, {
  requireAcks: 1,
  ackTimeoutMs: 100,
  partitionerType: 2
});
var topic = 'Q10000Sub.RebalanceTopic';
var count = 10;
var rets = 0;
var producer = new HighLevelProducer(client);


producer.on('ready', function () {
  setInterval(send, 1000);
});

producer.on('error', function (err) {
  console.log('error', err);
});

function send () {
  producer.send([
    {topic: topic, messages: JSON.stringify({
      root: "5751249b20316a7222269a26",
      data: {
        action: 2,
        type: 1
      } })}
  ], function (err, data) {
    if (err) console.log(err);
    else console.log('send %d messages', ++rets);
    if (rets === count) process.exit();
  });
}
