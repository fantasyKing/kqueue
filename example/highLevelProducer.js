var async = require('async');
var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var client = new kafka.Client('192.168.200.150:2181/kafka/q-rax306gz');
var producer = new HighLevelProducer(client, {
  requireAcks: 1,
  ackTimeoutMs: 100,
  partitionerType: 3
});
var topic = 'P10000Sub.inviteNotification';
var count = 10;
var rets = 0;
var producer = new HighLevelProducer(client);


producer.on('ready', function () {
  setInterval(send, 10);

  //var arr = [];
  //for (var i= 0; i< 10; i++){
  //  arr.push(sendfun());
  //}
  //
  //async.series(arr, function (error, result) {
  //  console.log('error: ' + JSON.stringify(error));
  //  console.log('result: ' + result);
  //});
});

producer.on('error', function (err) {
  console.log('error', err);
});


function sendfun () {
  return function (cb) {
    send (cb);
  }
}
function send (cb) {
  producer.send([
    {topic: topic, messages: JSON.stringify({
      room: ["585b48200fd48b0001b1c694","57ec8afb7dcfa50001756757"],
      data: "585b48210dd4110001156e79"
    })}
  ], function (err, data) {
    console.log(err);
    if (err) cb(err, null);
    else console.log('send %d messages', ++rets);
    if (rets === count) process.exit();
    console.log(data);
    cb(null, data);
  });
}

//function send (date, cb) {
//  producer.send([
//    {topic: topic, messages: JSON.stringify({
//      root: "5751249b20316a7222269a26",
//      data: {
//        action: 2,
//        type: 1
//      } })}
//  ], function (err, data) {
//    if (err) console.log(err);
//    else console.log('send %d messages', ++rets);
//    if (rets === count) process.exit();
//    console.log(data);
//    producer.send([
//      {topic: topic, messages: JSON.stringify({
//        root: "5751249b20316a7222269a26",
//        data: {
//          action: 2,
//          type: 1
//        } })}
//    ], function (err, data) {
//      if (err) console.log(err);
//      else console.log('send %d messages', ++rets);
//      if (rets === count) process.exit();
//      console.log(data);
//      producer.send([
//        {topic: topic, messages: JSON.stringify({
//          root: "5751249b20316a7222269a26",
//          data: {
//            action: 2,
//            type: 1
//          } })}
//      ], function (err, data) {
//        if (err) console.log(err);
//        else console.log('send %d messages', ++rets);
//        if (rets === count) process.exit();
//        console.log(data);
//        producer.send([
//          {topic: topic, messages: JSON.stringify({
//            root: "5751249b20316a7222269a26",
//            data: {
//              action: 2,
//              type: 1
//            } })}
//        ], function (err, data) {
//          if (err) console.log(err);
//          else console.log('send %d messages', ++rets);
//          if (rets === count) process.exit();
//          console.log(data);
//
//        });
//      });
//    });
//  });
//
//}


//,
//{topic: topic, messages: JSON.stringify({
//  root: "5751249b20316a7222269a26",
//  data: {
//    action: 2,
//    type: 1
//  } })},
//{topic: topic, messages: JSON.stringify({
//  root: "5751249b20316a7222269a26",
//  data: {
//    action: 2,
//    type: 1
//  } })},
//{topic: topic, messages: JSON.stringify({
//  root: "5751249b20316a7222269a26",
//  data: {
//    action: 2,
//    type: 1
//  } })},
//{topic: topic, messages: JSON.stringify({
//  root: "5751249b20316a7222269a26",
//  data: {
//    action: 2,
//    type: 1
//  } })}