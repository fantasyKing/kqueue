import Kproductor from '../dist/kproductor';
import config from './config';


var productor = new Kproductor(config.zookeeper_addr);

productor.send('test2','helloworld2');

setTimeout(function(){
    productor.send('test2','helloworld3');
    productor.send('test2','helloworld4');
}, 2000);

productor.kpub('Ti1','foo','well done');
productor.kpub('Ti2','bar',{"somekey":"hello world"});