import Kproducer from '../dist/kproducer';
import config from './config';


var producer = new Kproducer(config.zookeeper_addr);

producer.send('test2','helloworld2');

setTimeout(function(){
    producer.send('test2','helloworld3');
    producer.send('test2','helloworld4');
}, 2000);

producer.kpub('Ti1','foo','well done');
producer.kpub('Ti2','bar',{"somekey":"hello world"});