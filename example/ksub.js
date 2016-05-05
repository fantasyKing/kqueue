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

//let khub = new Khub(config.zookeeper_addr,{Ti1,Ti2},{only: "Ti2.bar"});
let khub = new Khub(config.zookeeper_addr,{Ti1,Ti2});