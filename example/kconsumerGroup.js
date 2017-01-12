import { KconsumerGroup } from '../src';
import config from './config';


class P10000Sub {
  inviteNotification(msg) {
    console.log(`P10000Sub.inviteNotification happend ${JSON.stringify(msg)}`);
  }
}

console.log(P10000Sub);
// let khub = new Khub(config.zookeeper_addr,{Ti1,Ti2},{only: "Ti2.bar"});
const khub = new KconsumerGroup(config.zookeeper_addr, { P10000Sub }, {id: "consumer1"});
console.log(khub);