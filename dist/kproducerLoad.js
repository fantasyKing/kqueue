'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _ = require('lodash');
var async = require('async');

var KproducerLoad = function () {
  function KproducerLoad(zookeeper_addr) {
    var _this = this;

    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    _classCallCheck(this, KproducerLoad);

    var kafka = require('kafka-node');
    var HighLevelProducer = kafka.HighLevelProducer;
    var client = new kafka.Client(zookeeper_addr);
    var producer = new HighLevelProducer(client, {
      requireAcks: 1,
      ackTimeoutMs: 100,
      partitionerType: 2
    });

    this.buffer = []; // use buffer to avoid kafka not ready case
    this.producer = producer;
    this.client = client;

    function handlerSend(date) {
      var self = this;
      return function (cb) {
        producer.send(date, function (err, value) {
          if (err) {
            cb(err, null);
          }

          cb(null, value);
        });
      };
    }

    process.on('SIGINT', function () {
      console.log('exiting...');
      client.close(function () {
        console.log('exited');
        process.exit();
      });
    });

    producer.on('ready', function () {
      console.log('kafka producer ready.');

      var sendArr = [];
      _this.buffer.forEach(function (v) {
        sendArr.push(handlerSend(v));
      });

      async.series(sendArr, function (err, result) {
        if (err) {
          console.log('kafka producer send error: ' + JSON.stringify(error));
        }
        console.log('kafka producer send data: ' + JSON.stringify(result));
      });

      _this.buffer = [];
    });

    producer.on('error', function (e) {
      console.log('kafka producer error:');
      console.log(e);
    });
  }

  _createClass(KproducerLoad, [{
    key: 'send',
    value: function send(topic, messages) {
      var _this2 = this;

      if (this.producer.ready) {
        this.producer.send([{ topic: topic, messages: messages, attributes: 0 }], function (err, data) {
          if (err) {
            console.log('topic:' + topic + ' messages:' + messages + ' Connect error,request push to memory buffer and wait for reconnect.');

            var topicMessage = [];
            topicMessage.push({ topic: topic, messages: messages, attributes: 0 });
            _this2.buffer.push(topicMessage);
          } else {
            console.log(data);
          }
        });
      } else {
        var topicMessage = [];
        topicMessage.push({ topic: topic, messages: messages, attributes: 0 });
        this.buffer.push(topicMessage);
        // attributes controls compression of the message set. It supports the following values:

        // 0: No compression
        // 1: Compress using GZip
        // 2: Compress using snappy
      }
    }
  }, {
    key: 'kpub',
    value: function kpub(class_name, function_name, arg) {
      this.send(class_name + '.' + function_name, JSON.stringify(arg));
    }
  }]);

  return KproducerLoad;
}();

exports.default = KproducerLoad;