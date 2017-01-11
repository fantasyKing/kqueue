'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _async = require('async');

var _async2 = _interopRequireDefault(_async);

var _kafkaNode = require('kafka-node');

var _kafkaNode2 = _interopRequireDefault(_kafkaNode);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var ConsumerGroup = _kafkaNode2.default.ConsumerGroup;

var consumerOptions = {
  groupId: 'kafka-node-group',
  id: 'consumer',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

var KconsumerGroup = function KconsumerGroup(zookeeper_addr, jobs) {
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};

  _classCallCheck(this, KconsumerGroup);

  this.host = zookeeper_addr;
  var opt = Object.assign(consumerOptions, options);
  opt.host = this.host;

  // 获取方法名函数
  function getAllMethods(object) {
    var obj = Object.getPrototypeOf(object);
    return Object.getOwnPropertyNames(obj).filter(function (property) {
      return typeof object[property] === 'function' && property !== 'constructor';
    });
  }

  // 实例化对象
  var objs = {};
  for (var key in jobs) {
    objs[key] = new jobs[key]();
  }

  // 构造 对象名-对象可用方法数组 对象
  var avliable_obj_arr = {};
  for (var _key in objs) {
    avliable_obj_arr[_key] = getAllMethods(objs[_key]);
  }

  // 生成可用监听Topic 列表
  var topics_avliable = [];
  for (var _key2 in avliable_obj_arr) {
    for (var subkey in avliable_obj_arr[_key2]) {
      topics_avliable.push(_key2 + '.' + avliable_obj_arr[_key2][subkey]);
    }
  }

  var topics_enable = topics_avliable;
  // 存在 only 配置时的情况
  if (options['only']) {
    topics_enable = topics_enable.filter(function (v) {
      return options['only'].indexOf(v) > -1;
    });
  }

  var topic_config = [];
  for (var _key3 in topics_enable) {
    topic_config.push(topics_enable[_key3]);
  }

  console.log(topic_config);

  var consumerGroup = new ConsumerGroup(opt, topic_config);

  consumerGroup.on('error', function (error) {
    console.error(error);
    console.error(error.stack);
  });

  consumerGroup.on('message', function (message) {
    var topic_arr = message['topic'].split('.');

    if (!topic_arr[0] || !topic_arr[1]) {
      console.error('not valid topic name');
      return;
    }

    if (typeof objs[topic_arr[0]][topic_arr[1]] !== 'function') {
      console.error('not valid topic name');
      return;
    }

    // 调用具体方法
    var args = null;
    try {
      args = JSON.parse(message['value']);
    } catch (e) {
      console.error('not valid message');
      return;
    }
    objs[topic_arr[0]][topic_arr[1]](args);
  });
};

exports.default = KconsumerGroup;