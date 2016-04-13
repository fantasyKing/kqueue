import Kconsumer from '../dist/kconsumer';
import config from './config';


var consumer = new Kconsumer(config.zookeeper_addr, [{topic: 'test2'}]);

consumer.onMessage = function(message) {
  console.log(message);
}