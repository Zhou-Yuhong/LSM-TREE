//
// Created by 周昱宏 on 2021/4/22.
//

#include "skiplist.h"
//#include <iostream>
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

