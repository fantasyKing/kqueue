class Khub {
  constructor(zookeeper_addr, jobs, options = {}) {
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
      topic_config.push({ topic: topics_enable[key] });
    }

    const kafka = require('kafka-node');
    const HighLevelConsumer = kafka.HighLevelConsumer;
    const client = new kafka.Client(zookeeper_addr);
    const Producer = kafka.HighLevelProducer;
    const producer = new Producer(client);


    producer.on('ready', () => {
      // 自动创建 Topics
      producer.createTopics(topics_enable, false, (err, data) => {
        console.log(err || data);
      });


      const consumer = new HighLevelConsumer(client, topic_config);
      this.consumer = consumer;

      console.log(`now listening topic: ${JSON.stringify(topics_enable)}`);

      process.on('SIGINT', () => {
        console.log('exiting...');
        consumer.close(true, () => {
          client.close(() => {
            console.log('exited');
            process.exit();
          });
        });
      });

      consumer.on('message', (message) => {
        const topic_arr = message['topic'].split('.');

        if (!topic_arr[0] || !topic_arr[1]) {
          console.log('not valid topic name');
          return;
        }

        if (typeof objs[topic_arr[0]][topic_arr[1]] !== 'function') {
          console.log('not valid topic name');
          return;
        }

        // 调用具体方法
        let args = null;
        try {
          args = JSON.parse(message['value']);
        } catch (e) {
          console.log('not valid message');
          return;
        }
        objs[topic_arr[0]][topic_arr[1]](args);
      });
    });
  }
}

export default Khub;
