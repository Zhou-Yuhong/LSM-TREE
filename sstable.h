//
// Created by 周昱宏 on 2021/4/22.
//

#ifndef LSM_SSTABLE_H
#define LSM_SSTABLE_H



#include <stdint.h>
#include<vector>
#include <string>
//注意读入时要把时间戳改成最大的加一
static unsigned int time=1;//时间戳
static void settime(int time_read){
    if(time_read>time) time=time_read;
}
using namespace std;
//归并时用的结点
struct comp_node{
    //存储key、value和建立的时间
    uint64_t key;
    string val;
    unsigned int time;
    comp_node(uint64_t key,string val,unsigned int time){
        this->key=key;
        this->val=val;
        this->time=time;
    }
};
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
    Searcher(uint64_t key,uint32_t offset){
        this->key=key;
        this->offset=offset;
    }
    Searcher(uint64_t key){
        this->key=key;
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
    //存储哈希得到的四个数
    unsigned int hash_result[4]={0};
    Header header;
    Bloomfilter bloomfilter;
    vector<Searcher> searcharray;
    //将hash得到的128位划分成4个unsigned
    //vector<unsigned int> gethash(uint64_t num);
    void  gethash(uint64_t num);
    //找键值为key的准备工作,第二个参数标明索引下标，第三个参数标明是否删除，通过返回值和if_del区分删除、找到、未找到三种情况
    bool getoffset(uint64_t key,int & index);
    //返回键值为key的value,未找到返回空的string,第二个参数为打开的文件名，第三个参数标记是否删除
    std::string get(uint64_t key,string filename);
    //设置bloom filter的位
    void setfilter(uint64_t key);

    //生成Sstable的辅助函数，对应的value先暂时放在skiplist的内存中，有两者下标的对应关系
    void addkey(uint64_t key);
    //取得所有信息，参数为文件路径
    vector<comp_node*> get_all_node(string filename);
    Sstable(){
        this->header.time=time;
        time++;
    }
    //从.sst文件初始化一个Sstable
    Sstable(std::string filename);

};



#endif //LSM_SSTABLE_H
