//
// Created by 周昱宏 on 2021/4/22.
//

#include "skiplist.h"
#include <fstream>
#include <direct.h>
#include <iostream>
#include <io.h>
using namespace std;
void Skiplist::put( uint64_t key,  std::string val)
{   //已经存在该key的情况
    Node* tr=fetch(key);
    if(tr!= nullptr){
            while(tr){
                tr->val=val;
                tr=tr->down;
            }
            return;
    }
    //不存在该key，插入该key
    vector<Node*> pathList;    //从上至下记录搜索路径
    Node* p = head;
    while (p) {
        while (p->right && p->right->key < key) {
            p = p->right;
        }
        pathList.push_back(p);
        p = p->down;
    }

    bool insertUp = true;
    Node* downNode = nullptr;
    while (insertUp && pathList.size() > 0) {   //从下至上搜索路径回溯，50%概率
        Node* insert = pathList.back();
        pathList.pop_back();
        insert->right = new Node(insert->right, downNode, key, val); //add新结点
        downNode = insert->right;    //把新结点赋值为downNode
        insertUp = (rand() & 1);   //50%概率
    }
    if (insertUp) {  //插入新的头结点，加层
        Node* oldHead = head;
        head = new Node();
        head->right = new Node(NULL, downNode, key, val);
        head->down = oldHead;
    }
    //增加一个新的元素后cap的增加量
    this->cap+=(12+val.length()+1);
}

std::string* Skiplist::get(const uint64_t & key)
{
    Node* p = head;
    bool flag = false;
    while (p) {
        while (p->right && p->right->key < key) {
            p = p->right;
        }
        if (p->right && p->right->key == key) {
            flag = true;
            break;
        }
        p = p->down;
    }
    if (flag) {
        return &(p->right->val);
    }
    else
    return nullptr;
}

bool Skiplist::del(const uint64_t & key)
{
    Node* p = fetch(key);
    Node* tmp;
    if (p == nullptr) return false;
    while (p) {
//        Node* q = p->right;
//        p->right = q->right;
//        tmp = q->down;
//        delete q;
//        p = p->down;
//        if (p == nullptr)  break;
//        while (p->right && p->right != tmp) {
//            p = p->right;
//        }
      p->val="~DELETED~";
      p=p->down;
    }
    return true;
}

void Skiplist::reset()   //删除所有，只留下一个head
{   //重置大小
    this->cap=10240+32;
    Node* p = head;
    Node* q;
    Node* tmp;
    while (p->down) {
        q = p->right;
        while (q) {
            tmp = q->right;
            delete q;
            q = tmp;
        }
        tmp = p->down;
        delete p;
        p = tmp;
    }
    q = p->right;
    while (q) {
        tmp = q->right;
        delete q;
        q = tmp;
    }
    head = p;
    head->right = nullptr;
    head->down = nullptr;
}

Node* Skiplist::fetch(const uint64_t & key)
{
    Node* p = head;
    bool flag = false;
    while (p) {
        while (p->right && p->right->key < key){
            p = p->right;
        }
        if (p->right && p->right->key == key) {
            flag = true;
            break;
        }
        p = p->down;
    }
    if (flag) {
        return p->right;
    }
    return nullptr;
}

Sstable *Skiplist::translate() {
    Node* p=head;
    while(p->down) p=p->down;
    p=p->right;
    //创建一个Sstable
    Sstable* sstable=new Sstable();
    //创建一个Vector保存所有的value
    vector<string> valueset;
    while(p){
        //sstable插入key
         sstable->addkey(p->key);
         //先把value放在vector里
         valueset.push_back(p->val);
         p=p->right;
    }
    //创建文件
    if(access("data",0)!=0){
        mkdir("data");
    }
    if(access("data\\0",0)!=0){
        mkdir("data\\0");
    }
    string filename="data\\0\\test.sst";
    ofstream out(filename,ios::out|ios::binary);
    //起始偏移量
    unsigned int origin_offset=32+10240+sstable->header.num*96;
    //写头部
    out.write((char *)&(sstable->header.time), sizeof(uint64_t));
    out.write((char *)&(sstable->header.num),sizeof (uint64_t));
    out.write((char *)&(sstable->header.min_key),sizeof (uint64_t));
    out.write((char *)&(sstable->header.max_key),sizeof (uint64_t));
    //写bloomfilter
    out.write((char*)&(sstable->bloomfilter),sizeof(char)*10240);

    for(int i=0;i<sstable->header.num;i++){
        //写索引
        out.write((char*)&(sstable->searcharray[i].key),sizeof(uint64_t));
        out.write((char*)&(sstable->searcharray[i].offset),sizeof (uint32_t));
    }
    //用于记录所有偏移量
    vector<unsigned int> offset_array;
    //循环中存储偏移量
    unsigned int pos;
    //unsigned int size;
    //存储value，并保存偏移量
    for(int i=0;i<sstable->header.num;i++){
       pos=out.tellp();
       //记录所有偏移量
       offset_array.push_back(pos);
       //把string转成char*来存
       const char* input=valueset[i].c_str();
       //这里size多留一个位置用于存字符串结束符
       out.write(input,valueset[i].size());
    }
    //测试
//    out.close();
//    ifstream in(filename,ios::in|ios::binary);
//    in.seekg(offset_array[0]);  //第一处字符串
//    char a[100];
//    in.read(a,100);
//    in.close();
    //在索引区写回偏移量,内存和硬盘都要写
    pos=32+10240+8;
    for(int i=0;i<sstable->header.num;i++){
        sstable->searcharray[i].offset=offset_array[i];
        out.seekp(pos);
        out.write((char*)&(offset_array[i]),sizeof(unsigned int));
        pos+=12;
    }
    return sstable;
}

//void Skiplist::showtree() {
//    Node* p=head;
//    Node* q;
//    while(p){
//        q=p->right;
//        while(q){
//            if(q->del){
//                cout<<"del ";
//            }
//            else cout<<q->key<<" ";
//            q=q->right;
//        }
//        cout<<endl;
//        p=p->down;
//    }
//}

unsigned int Skiplist::getcap() {
    return this->cap;
}

Skiplist::~Skiplist() {
    this->reset();
}

