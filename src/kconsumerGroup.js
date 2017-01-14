import async from 'async';
import kafka from 'kafka-node';

const ConsumerGroup = kafka.ConsumerGroup;

const consumerOptions = {
  groupId: 'push-online-group',
  id: 'consumer',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

function KconsumerGroup(zookeeper_addr, jobs, options = {}) {
  this.host = zookeeper_addr;
  let opt = Object.assign(consumerOptions, options);
  opt.host = this.host;

  // 获取方法名函数
  function getAllMethods(object) {
    const obj = Object.getPrototypeOf(object);
    return Object.getOwnPropertyNames(obj).filter((property) =>
      typeof object[property] === 'function' && property !== 'constructor'
    );
  }

  // 实例化对象
  const objs = {};
  for (const key in jobs) {
    objs[key] = new jobs[key];
  }

  // 构造 对象名-对象可用方法数组 对象
  const avliable_obj_arr = {};
  for (const key in objs) {
    avliable_obj_arr[key] = getAllMethods(objs[key]);
  }

  // 生成可用监听Topic 列表
  const topics_avliable = [];
  for (const key in avliable_obj_arr) {
    for (const subkey in avliable_obj_arr[key]) {
      topics_avliable.push(`${key}.${avliable_obj_arr[key][subkey]}`);
    }
  }

  let topics_enable = topics_avliable;
  // 存在 only 配置时的情况
  if (options['only']) {
    topics_enable = topics_enable.filter((v) => options['only'].indexOf(v) > -1);
  }

  const topic_config = [];
  for (const key in topics_enable) {
    topic_config.push(topics_enable[key]);
  }

  console.log(topic_config);

  var consumerGroup = new ConsumerGroup(opt, topic_config);
  consumerGroup.on('error', function (error) {
    console.error(error);
    console.error(error.stack);
  });


  consumerGroup.on('message', function (message) {
    console.log('%s read msg Topic="%s" Partition=%s Offset=%d', this.client.clientId, message.topic, message.partition, message.offset);
    const topic_arr = message['topic'].split('.');

    if (!topic_arr[0] || !topic_arr[1]) {
      console.error('not valid topic name');
      return;
    }

    if (typeof objs[topic_arr[0]][topic_arr[1]] !== 'function') {
      console.error('not valid topic name');
      return;
    }

    // 调用具体方法
    let args = null;
    try {
      args = JSON.parse(message['value']);
    } catch (e) {
      console.error('not valid message');
      return;
    }
    objs[topic_arr[0]][topic_arr[1]](args);
  });

  process.once('SIGINT', function () {
    async.each([consumerGroup], function (consumer, callback) {
      consumer.close(true, callback);
    });
  });
}

module.exports = KconsumerGroup;

