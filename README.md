# Kqueue
 Easy queue on kafka with node.js
## How to use
### producer
    import {Kproducer} from '../dist';
    var producer = new Kproducer(config.zookeeper_addr);
    producer.send('you_topic','helloworld2');
## consumer
    import {Kconsumer} from '../dist';
    import config from './config';
    var consumer = new Kconsumer(config.zookeeper_addr, [{topic: 'your_topic'}]);
    consumer.onMessage = function(message) {
      console.log(message);
    } 
## use worker hub
### producer
    import {Kproducer} from '../dist';
    var producer = new Kproducer(config.zookeeper_addr);
    producer.kpub('Ti1','foo','well done');
## consumer
    import {Khub} from '../dist';
    import config from './config';
    class Ti1{
      foo(msg){
        console.log("Ti1.foo happend"+msg);
      }
      bar(msg){
        console.log("Ti1.bar happend"+msg);
      }
    }
    class Ti2{
      foo2(msg){
        console.log("Ti2.foo2 happend"+msg);
      }
      bar(msg){
        console.log("Ti2.bar happend"+msg["somekey"]);
      }
    }
    var khub = new Khub(config.zookeeper_addr,{Ti1,Ti2},{only: "Ti2.bar"});


For more example, see the example folder.
## run example
    npm start
    cd out
    node ./ksub.js
