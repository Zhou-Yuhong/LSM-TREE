//
// Created by 周昱宏 on 2021/4/22.
//

#ifndef LSM_SKIPLIST_H
#define LSM_SKIPLIST_H

#include "sstable.h"
#include <vector>
#include <string>

struct Node {
    Node* right, * down;   //向右向下足矣
    uint64_t key;
    std::string val;
    Node(Node* right, Node* down, uint64_t key, std::string val) : right(right), down(down), key(key), val(val) {}
    Node() : right(nullptr), down(nullptr) {}
    bool del=false;
};
class Skiplist {
public:
    unsigned int cap;
    Node* head;
    // long size=0;
    Skiplist() {
        head = new Node();
        //初始化cap的大小,bloom filter和头部所占字节
        cap=10240+32;
    }
    ~Skiplist();
    void put(uint64_t key,std::string val,bool if_del=false);
    void showtree();
    std::string* get(const uint64_t & key);
    bool del(const uint64_t & key);
    unsigned int getcap();
    void reset();
    //返回键值为key的左上结点，不论是否delete
    Node* fetch(const uint64_t & key);
    //生成Sstable,同时在硬盘也做对应操作
    Sstable* translate();
};



#endif //LSM_SKIPLIST_H
