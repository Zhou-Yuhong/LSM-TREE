//
// Created by 周昱宏 on 2021/4/24.
//

#ifndef LSM_LEVEL_H
#define LSM_LEVEL_H
#include "skiplist.h"
#include "sstable.h"
//建立sstable* 和sstable文件的联系
struct Sstable_Wrap{
    Sstable * sstable;
    //sstable文件的最后数字
    int seq_num;
    Sstable_Wrap(Sstable * sstable,int seq_num){
        this->sstable=sstable;
        this->seq_num=seq_num;
    }
    ~Sstable_Wrap(){
        delete sstable;
    }
};
class Level{
public:
    int level_id;
    //.sst文件后缀,新建的时候用
    int sstable_id=0;
    int cap=0; //level中存储了多少sstable
    bool *is_create;
    vector<Sstable_Wrap*> file;
    Level(int id){
        this->level_id=id;
        is_create=new bool[((1<<(level_id+1))+1)];
        for(int i=0;i<((1<<(level_id+1))+1);i++){
            is_create[i]=false;
            file.push_back(nullptr);
        }
    }
    //由Skiplist生成sstable并存入内存
    void make_sstable(Skiplist &skiplist);
    ~Level(){
        delete is_create;
        for(int i=0;i<this->file.size();i++){
            if(this->file[i]!= nullptr){
                delete this->file[i];
            }
        }
    }
    //kvstore调用，参数为sstable*以及.sst的后缀数字，注意时间戳越大越往后插入
    void add_sstable(Sstable * sstable,int sstable_seq_num);
    //判断一层是否满了
    bool if_full();
    //找到最新的value
    std::string get(uint64_t key);

};


#endif //LSM_LEVEL_H
