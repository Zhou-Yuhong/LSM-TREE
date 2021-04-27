//
// Created by 周昱宏 on 2021/4/24.
//

#ifndef LSM_LEVEL_H
#define LSM_LEVEL_H
#include "skiplist.h"
#include "sstable.h"

class Level{
public:
    int level_id;
    bool *is_create;
    vector<Sstable*> file;
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
    void add_sstable(Sstable * sstable);
    //判断一层是否满了
    bool if_full();
    //找到最新的value
    std::string get(uint64_t key,bool& if_del);
};


#endif //LSM_LEVEL_H
