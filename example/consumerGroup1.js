var async = require('async');
var ConsumerGroup = require('kafka-node').ConsumerGroup;

var consumerOptions = {
  host: '192.168.200.150:2181/kafka/q-rax306gz',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

var topics = ['Q10000Sub.RebalanceTopic', 'Q10000Sub.RebalanceTest'];

var consumerGroup2 = new ConsumerGroup(Object.assign({id: 'c2'}, consumerOptions), topics);
consumerGroup2.on('error', onError);
consumerGroup2.on('message', onMessage);

//var consumerGroup = new ConsumerGroup(Object.assign({id: 'c2'}, consumerOptions), topics);
//consumerGroup.on('error', onError);
//consumerGroup.on('message', onMessage);

//consumerGroup2.on('connect', function () {
//  setTimeout(function () {
//    consumerGroup2.close(true, function (error) {
//      console.log('consumer2 closed', error);
//    });
//  }, 25000);
//});

//var consumerGroup3 = new ConsumerGroup(Object.assign({id: 'c3'}, consumerOptions), topics);
//consumerGroup3.on('error', onError);
//consumerGroup3.on('message', onMessage);

function onError (error) {
  console.error(error);
  console.error(error.stack);
}

function onMessage (message) {
  console.log(message);
  console.log('%s read msg Topic="%s" Partition=%s Offset=%d', this.client.clientId, message.topic, message.partition, message.offset);
}

process.once('SIGINT', function () {
  async.each([consumerGroup2], function (consumer, callback) {
    consumer.close(true, callback);
  });
});

