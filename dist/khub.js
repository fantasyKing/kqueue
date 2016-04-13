"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Khub = function Khub(zookeeper_addr, jobs) {
  var _this = this;

  var options = arguments.length <= 2 || arguments[2] === undefined ? {} : arguments[2];

  _classCallCheck(this, Khub);

  // 获取方法名函数
  function getAllMethods(object) {
    var obj = Object.getPrototypeOf(object);
    return Object.getOwnPropertyNames(obj).filter(function (property) {
      return typeof object[property] == 'function' && property != "constructor";
    });
  }

  // 实例化对象
  var objs = {};
  for (var key in jobs) {
    objs[key] = new jobs[key]();
  }

  // 构造 对象名-对象可用方法数组 对象
  var avliable_obj_arr = {};
  for (var key in objs) {
    avliable_obj_arr[key] = getAllMethods(objs[key]);
  }

  // 生成可用监听Topic 列表
  var topics_avliable = [];
  for (var key in avliable_obj_arr) {
    for (var subkey in avliable_obj_arr[key]) {
      topics_avliable.push(key + "." + avliable_obj_arr[key][subkey]);
    }
  }

  var topics_enable = topics_avliable;
  // 存在 only 配置时的情况
  if (options["only"]) {
    topics_enable = topics_enable.filter(function (v) {
      return options["only"].indexOf(v) > -1;
    });
  }

  var topic_config = [];
  for (var key in topics_enable) {
    topic_config.push({ topic: topics_enable[key] });
  }

  var kafka = require('kafka-node'),
      HighLevelConsumer = kafka.HighLevelConsumer,
      client = new kafka.Client(zookeeper_addr),
      Producer = kafka.HighLevelProducer,
      producer = new Producer(client);

  producer.on("ready", function () {
    // 自动创建 Topics
    producer.createTopics(topics_enable, false, function (err, data) {
      console.log(err || data);
    });

    var consumer = new HighLevelConsumer(client, topic_config);
    _this.consumer = consumer;

    console.log("now listening topic:" + JSON.stringify(topics_enable));

    consumer.on('message', function (message) {
      var topic_arr = message["topic"].split(".");

      if (!topic_arr[0] || !topic_arr[1]) {
        console.log("not valid topic name");
        return;
      }

      if (typeof objs[topic_arr[0]][topic_arr[1]] != 'function') {
        console.log("not valid topic name");
        return;
      }

      // 调用具体方法
      var args = null;
      try {
        args = JSON.parse(message["value"]);
      } catch (e) {
        console.log("not valid message");
        return;
      }
      objs[topic_arr[0]][topic_arr[1]](args);
    });
  });
};

exports.default = Khub;