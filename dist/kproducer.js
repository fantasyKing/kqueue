'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _ = require('lodash');

var Kproducer = function () {
  function Kproducer(zookeeper_addr) {
    var _this = this;

    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    _classCallCheck(this, Kproducer);

    var kafka = require('kafka-node');
    var HighLevelProducer = kafka.HighLevelProducer;
    var client = new kafka.Client(zookeeper_addr);
    var producer = new HighLevelProducer(client, options);

    this.buffer = []; // use buffer to avoid kafka not ready case
    this.producer = producer;
    this.client = client;

    process.on('SIGINT', function () {
      console.log('exiting...');
      client.close(function () {
        console.log('exited');
        process.exit();
      });
    });

    producer.on('ready', function () {
      console.log('kafka producer ready.');
      if (_this.buffer.length > 0) {
        _this.producer.send(_this.buffer, function (err, data) {
          console.log(data);
        });
        _this.buffer = [];
      }
    });

    producer.on('error', function (e) {
      console.log('kafka producer error:');
      console.log(e);
    });
  }

  _createClass(Kproducer, [{
    key: 'send',
    value: function send(topic, messages) {
      var _this2 = this;

      if (this.producer.ready) {
        this.producer.send([{ topic: topic, messages: messages, attributes: 2 }], function (err, data) {
          if (err) {
            console.log(err);
            console.log(topic + ' Connect error,request push to memory buffer and wait for reconnect.');

            var exist_topic = _.find(_this2.buffer, { topic: topic });
            if (exist_topic) {
              exist_topic.messages = _.concat(exist_topic.messages, messages);
            } else {
              _this2.buffer.push({ topic: topic, messages: messages, attributes: 2 });
            }
          } else {
            console.log(data);
          }
        });
      } else {
        var exist_topic = _.find(this.buffer, { topic: topic });
        if (exist_topic) {
          exist_topic.messages = _.concat(exist_topic.messages, messages);
        } else {
          this.buffer.push({ topic: topic, messages: messages, attributes: 2 });
        }
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

  return Kproducer;
}();

exports.default = Kproducer;