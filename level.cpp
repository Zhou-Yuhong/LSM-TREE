//
// Created by 周昱宏 on 2021/4/24.
//

#include "level.h"
#include "io.h"
#include <fstream>
void Level::make_sstable(Skiplist &skiplist) {
    //先在内存中生成一个sstable
    Node* p=skiplist.head;
    while(p->down) p=p->down;
    p=p->right;
    //创建一个Sstable
    Sstable* sstable=new Sstable();
    //创建一个Vector保存所有的value
    vector<string> valueset;
    while(p){
        //sstable插入key,这个过程处理了挺多东西
        sstable->addkey(p->key);
        //先把value放在vector里
        valueset.push_back(p->val);
        p=p->right;
    }
    //创建文件
    if(access("./DATA",0)!=0){
        mkdir("./DATA");
    }
    std::string filename="./DATA\\level_" + to_string(this->level_id);
    const char* tmp=filename.c_str();
    if(access(tmp,0)!=0){
        mkdir(tmp);
    }
    int i=0;
    while(this->is_create[i]){
        i++;
    }
    filename=filename+"\\sstable_"+ to_string(i);
    ofstream out(filename,ios::out|ios::binary);
    //写头部
    out.write((char *)&(sstable->header.time), sizeof(uint64_t));
    out.write((char *)&(sstable->header.num),sizeof (uint64_t));
    out.write((char *)&(sstable->header.min_key),sizeof (uint64_t));
    out.write((char *)&(sstable->header.max_key),sizeof (uint64_t));
    //写bloomfilter
    out.write((char*)&(sstable->bloomfilter),sizeof(char)*10240);
    for(int j=0;j<sstable->header.num;j++){
        //写索引
        out.write((char*)&(sstable->searcharray[j].key),sizeof(uint64_t));
        out.write((char*)&(sstable->searcharray[j].offset),sizeof (uint32_t));
    }
    //用于记录所有偏移量
    vector<unsigned int> offset_array;
    //循环中存储偏移量
    unsigned int pos;
    //unsigned int size;
    //存储value，并保存偏移量
    for(int j=0;j<sstable->header.num;j++){
        pos=out.tellp();
        //记录所有偏移量
        offset_array.push_back(pos);
        //把string转成char*来存
        const char* input=valueset[j].c_str();
        //写val
        out.write(input,valueset[j].size());
    }
        //在索引区写回偏移量,内存和硬盘都要写
    pos=32+10240+8;
    for(int j=0;j<sstable->header.num;j++){
        sstable->searcharray[j].offset=offset_array[j];
        out.seekp(pos);
        out.write((char*)&(offset_array[j]),sizeof(unsigned int));
        pos+=12;
    }
    //把生成的sstable加入到level中的vector<Sstable*>中
    this->file[i]=sstable;
    this->is_create[i]=true;
}

std::string Level::get(uint64_t key) {
    //若为空
    if(this->is_create[0]== false){
        return "";
    }
    //先找到最新的sstable
    int i=0;
    while(this->is_create[i]){
        i++;
    }
    std::string filename;
    std::string result;
    Sstable * sstable;
    //按新旧顺序找
    for(int index=i-1;index>=0;index--){
        //判断key是否在这个sstable中
        sstable=file[index];
        if(key<=sstable->header.max_key&&key>=sstable->header.min_key) {
            filename = "./DATA\\level_" + to_string(this->level_id) + "\\sstable_" + to_string(index);
            result = this->file[index]->get(key, filename);
            //如果在一个sst中未查找到且并非被删除，则继续查找
            if (result == "") continue;
            else {
                //否则返回（这里返回可能是"~DELETED~"或者具体的值），反正是最新的
                 return result;
            }
        }
        else{
            continue;
        }
    }
}

void Level::add_sstable(Sstable *sstable,int index) {

    this->file[index]=sstable;
    this->is_create[index]=true;
}

bool Level::if_full() {
    return this->is_create[this->file.size()-1];
}


