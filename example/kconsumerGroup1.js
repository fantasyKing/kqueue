import { KconsumerGroup } from '../src';
import config from './config';


class Q10000Sub {
  RebalanceTopic(msg) {
    console.log(`Q10000Sub.RebalanceTopic happend ${JSON.stringify(msg)}`);
  }
}

console.log(Q10000Sub);
// let khub = new Khub(config.zookeeper_addr,{Ti1,Ti2},{only: "Ti2.bar"});
const khub = new KconsumerGroup(config.zookeeper_addr, { Q10000Sub }, {id: "consumer2"});
console.log(khub);