//
// Created by 周昱宏 on 2021/4/22.
//

#ifndef LSM_SSTABLE_H
#define LSM_SSTABLE_H



#include <stdint.h>
#include<vector>
#include <string>
static unsigned int time=1;//时间戳
using namespace std;
struct Header
{
    uint64_t time=0;
    uint64_t num=0;
    uint64_t min_key = UINT64_MAX;
    uint64_t max_key = 0;
    void updatehead(uint64_t input){
      if(input>max_key) max_key=input;
      if(input<min_key) min_key=input;
      num++;
    }
};
struct Searcher {
    uint64_t key=0;
    uint32_t offset=0;
    bool del = false;
    Searcher(uint64_t key,uint32_t offset,bool del=false){
        this->key=key;
        this->offset=offset;
        this->del=del;
    }
    Searcher(uint64_t key,bool del){
        this->key=key;
        this->del=del;
    }
    Searcher();
};
struct Bloomfilter {
    char bitarray[10240];
   //char* bitarray;
    void add_to_bitarray( uint32_t num);
    bool is_in_bitarray( uint32_t num);
    void clear_bitarray(uint32_t num);
    Bloomfilter();
    ~Bloomfilter();
};
class Sstable {
public:
    Header header;
    Bloomfilter bloomfilter;
    vector<Searcher> searcharray;
    //将hash得到的128位划分成4个unsigned
    vector<unsigned int> gethash(uint64_t num);

    //找键值为key的准备工作,第二个参数标明索引下标，第三个参数标明是否删除，通过返回值和if_del区分删除、找到、未找到三种情况
    bool getoffset(uint64_t key,int & index,bool & if_del);
    //返回键值为key的value,未找到返回空的string,第二个参数为打开的文件名，第三个参数标记是否删除
    std::string get(uint64_t key,string filename,bool & if_del);
    //设置bloom filter的位
    void setfilter(uint64_t key);

    //生成Sstable的辅助函数，对应的value先暂时放在skiplist的内存中，有两者下标的对应关系
    void addkey(uint64_t key,bool del=false);

    Sstable(){
        this->header.time=time;
        time++;
    }
    //从.sst文件初始化一个Sstable
    Sstable(std::string filename);

};



#endif //LSM_SSTABLE_H
