'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Kproductor = function () {
  function Kproductor(zookeeper_addr) {
    var _this = this;

    var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

    _classCallCheck(this, Kproductor);

    var kafka = require('kafka-node'),
        HighLevelProducer = kafka.HighLevelProducer,
        client = new kafka.Client(zookeeper_addr),
        producer = new HighLevelProducer(client, options);

    this.buffer = []; // use buffer to avoid kafka not ready case
    this.producer = producer;
    this.client = client;

    process.on('SIGINT', function () {
      console.log("exiting...");
      client.close(function () {
        console.log("exited");
        process.exit();
      });
    });

    producer.on('ready', function () {
      console.log("kafka producer ready.");
      if (_this.buffer.length > 0) {
        _this.producer.send(_this.buffer, function (err, data) {
          console.log(data);
        });
        _this.buffer = [];
      }
    });
  }

  _createClass(Kproductor, [{
    key: 'send',
    value: function send(topic, messages) {
      if (this.producer.ready) {
        this.producer.send([{ "topic": topic, "messages": messages, attributes: 2 }], function (err, data) {
          console.log(data);
        });
      } else {
        this.buffer.push({ "topic": topic, "messages": messages, attributes: 2 });
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

  return Kproductor;
}();

exports.default = Kproductor;