'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Kconsumer = function () {
  function Kconsumer(zookeeper_addr) {
    var _this = this;

    var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

    _classCallCheck(this, Kconsumer);

    var kafka = require('kafka-node'),
        HighLevelConsumer = kafka.HighLevelConsumer,
        client = new kafka.Client(zookeeper_addr),
        consumer = new HighLevelConsumer(client, options);
    this.consumer = consumer;

    process.on('SIGINT', function () {
      console.log("exiting...");
      consumer.close(true, function () {
        client.close(function () {
          console.log("exited");
          process.exit();
        });
      });
    });

    consumer.on('message', function (message) {
      console.log(message);
      _this.onMessage(message);
    });
  }

  _createClass(Kconsumer, [{
    key: 'onMessage',
    value: function onMessage(message) {
      console.log('an message comes' + message);
    }
  }]);

  return Kconsumer;
}();

exports.default = Kconsumer;