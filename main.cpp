#include <iostream>
#include "skiplist.h"
#include "sstable.h"
#include <fstream>
#include <direct.h>
#include <io.h>
#include <fstream>
#include <direct.h>
#include <iostream>
#include <cstring>
#include "level.h"
#include "kvstore.h"
int main() {

//    Skiplist m;
//    for(uint64_t i=0;i<10;i++){
//        m.put(i, to_string(i));
//    }
//    m.translate();
    //创建文件
//    if(access("test",0)!=0){
//        mkdir("test");
//    }
//    if(access("test\\0",0!=0)){
//        mkdir("test\\0");
//    }
//    string filename="test\\0\\test.sst";
//    ofstream out(filename,ios::out|ios::binary);
//    string str="abc";
//    char *tmp;
//    tmp=(char*)&str;
//    out.write((char*)&(str),sizeof(str));
//    string str2="cde";
//    out.write((char*)&(str2),sizeof (str2));
//    out.close();
//    ifstream in(filename,ios::in|ios::binary);
//    string m;
//    in.read((char*)&m,sizeof(str) );
//    cout<<m<<endl;

//分割线
//    Skiplist m;
//    for(uint64_t i=0;i<100;i++){
//        m.put(i, "value"+to_string(i));
//    }
//    for(uint64_t i=50;i<65;i++){
//        m.del(i);
//    }
//    Sstable* sstable=m.translate();
//    string result;
//    for(uint64_t i=0;i<100;i++){
//        result=sstable->get(i);
//        cout<<result<<endl;
//    }
//分割线
    Level level(0);
    Skiplist skiplist;
    for(uint64_t i=0;i<100;i++){
        skiplist.put(i,"value1:"+ to_string(i));
    }
    level.make_sstable(skiplist);
    for(uint64_t i=0;i<100;i++){
        skiplist.put(i,"value2:"+ to_string(i));
    }
    level.make_sstable(skiplist);
    Level level1(1);
    skiplist.reset();
    for(uint64_t i=0;i<100;i++){
        skiplist.put(i,"value3:"+ to_string(i));
    }
    level1.make_sstable(skiplist);
    for(uint64_t i=0;i<100;i++){
        skiplist.put(i,"value4:"+ to_string(i));
    }
    level1.make_sstable(skiplist);
    Level level2(2);
    for(uint64_t i=0;i<100;i++){
        skiplist.put(i,"value5:"+ to_string(i));
    }
    level2.make_sstable(skiplist);
    KVStore kvStore("DATA");


    return 0;
}
