//
// Created by 周昱宏 on 2021/4/22.
//

#ifndef LSM_KVSTORE_H
#define LSM_KVSTORE_H

#pragma once

#include "kvstore_api.h"
#include "level.h"
#include <list>
//存储sst文件的层次以及路径信息
struct  sstable_path{
    int level;
    string path;
    sstable_path(int level,string path){
        this->level=level;
        this->path=path;
    }
};
class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    std::vector<Level*> file_level;
    Skiplist* skiplist;

public:
    //由path取出所有的.sst文件
    //void getFiles( string path, vector<string>& files );
    KVStore(const std::string &dir);
    //不带参数的kvstore
    //KVStore();
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;
    //扩容level
    void addlevel(int newsize);
    //得到string中的所有数字
    vector<int> GetStringNum(string);
};



#endif //LSM_KVSTORE_H
