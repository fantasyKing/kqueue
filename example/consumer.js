import { Kconsumer } from '../dist';
import config from './config';


const consumer = new Kconsumer(config.zookeeper_addr, [{ topic: 'test2' }]);

consumer.onMessage = (message) => {
  console.log(message);
};
