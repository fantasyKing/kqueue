import { KproducerLoad } from '../src';
import config from './config';

const pro = new KproducerLoad(config.zookeeper_addr);

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 1
  }
});

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 2
  }
});

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 2
  }
});

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 2
  }
});

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 2
  }
});

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 2
  }
});

pro.kpub('P10000Sub', 'inviteNotification', {
  room: "5751249b20316a7222269a26",
  data: {
    action: 2,
    type: 2
  }
});
