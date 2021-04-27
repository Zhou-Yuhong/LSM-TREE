//
// Created by 周昱宏 on 2021/4/22.
//

#include "sstable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include "MurmurHash3.h"
#define SHIFT 3
#define MASK 0x7
#define BLOOM_FILTER_LENGTH 81920;
void Bloomfilter::add_to_bitarray(uint32_t num)
{
    num=num%BLOOM_FILTER_LENGTH;
    bitarray[num >> SHIFT] |= (1 << (num & MASK));
}

bool Bloomfilter::is_in_bitarray(uint32_t num)
{
//    num=num%BLOOM_FILTER_LENGTH;
//    uint32_t shiftnum=num >> SHIFT;
//    return bitarray[shiftnum] & (1 << (num & MASK));
      num=num%BLOOM_FILTER_LENGTH;
      uint32_t  shiftnum=num>>SHIFT;
      char tmp=bitarray[shiftnum];
      return tmp&(1 << (num & MASK));
}

void Bloomfilter::clear_bitarray(uint32_t num)
{   num=num%BLOOM_FILTER_LENGTH;
    bitarray[num >> SHIFT] &= ~(1 << (num & MASK));
}

Bloomfilter::Bloomfilter()
{
    //bitarray=new char[10240];
    memset(bitarray, 0, 10240);
}

Bloomfilter::~Bloomfilter()
{
    //delete bitarray;
}
//如果未找到返回false,找到的话，返回true
bool Sstable::getoffset(uint64_t key, int &index)
{
    if (key > this->header.max_key || key < this->header.min_key) return false;
    //用bloomfilter判断key是否存在
    //vector<unsigned int>hash_numgroup= gethash(key);
    gethash(key);
    for(int i=0;i<4;i++){
        if(!bloomfilter.is_in_bitarray(this->hash_result[i])) return false;
    }
    //用二分法找到对应索引,没有则返回0
    int left=0;
    int right=this->searcharray.size()-1;
    while(left<=right){
        int mid=(left+right)/2;
        if(searcharray[mid].key==key){
                index=mid;
                return true;
        }
        else if(searcharray[mid].key<key){
            left=mid+1;
        }
        else{
            right=mid-1;
        }
    }
    //二分法没有查找到
    return false;
}

//vector<unsigned int>Sstable::gethash(uint64_t num) {
//    unsigned int hashresult[4] = {0};
//    void* key=&num;
//    MurmurHash3_x64_128(key,8,1,hashresult);
//    vector<unsigned int> result;
//    for(int i=0;i<4;i++){
//        result.push_back(hashresult[i]);
//    }
//    return result;
//}
    void Sstable::gethash(uint64_t num) {
    void* key=&num;
    MurmurHash3_x64_128(key,8,1,this->hash_result);

}

void Sstable::setfilter(uint64_t key) {
     //vector<unsigned int> result= gethash(key);
      gethash(key);
     for(int i=0;i<4;i++){
         this->bloomfilter.add_to_bitarray(this->hash_result[i]);
     }
     return;
}
//跳表生成Sstable的辅助函数
void Sstable::addkey(uint64_t key) {
   //更新头部
   this->header.updatehead(key);
   //布隆过滤器
   this->setfilter(key);
   //索引区加一
   Searcher search(key);
   this->searcharray.push_back(search);
}

std::string Sstable::get(uint64_t key,string filename) {
//    string filename="data\\0\\test.sst";
    int index=0;
    //读的大小
    unsigned int size;
    if(this->getoffset(key,index)){
        //如果存在该key
        ifstream in(filename,ios::in|ios::binary);
        unsigned int offset=this->searcharray[index].offset;
        //如果要读的是最后一个value
        if(index==this->searcharray.size()-1){
            in.seekg(0,ios_base::end);
            unsigned length=in.tellg();
         size=length-offset;
         char* c=new char[size+1];
         c[size]='\0';
         in.seekg(offset);
         in.read(c,size);
         std::string result=c;
         delete c;
         return result;
        }
        //若不是
        else{
            size=this->searcharray[index+1].offset-offset;
            char* c=new char[size+1];
            c[size]='\0';
            in.seekg(offset);
            in.read(c,size);
            std::string result=c;
            delete c;
            return result;
        }
    }
    else{
        //不存在返回空
        return "";
    }
}

Sstable::Sstable(std::string filename) {
    ifstream in(filename,ios::in|ios::binary);
    //读入头部
    uint64_t numinput;
    uint32_t numinput_32;
    in.read((char *)&numinput, sizeof(uint64_t));
    this->header.time=numinput;
    in.read((char *)&numinput, sizeof(uint64_t));
    this->header.num=numinput;
    in.read((char *)&numinput, sizeof(uint64_t));
    this->header.min_key=numinput;
    in.read((char *)&numinput, sizeof(uint64_t));
    this->header.max_key=numinput;
    //读入bloom filter
    in.read(this->bloomfilter.bitarray,10240);
    //读入searcharray
    uint64_t totlenum=this->header.num;
    for(unsigned int i=0;i<totlenum;i++){
        in.read((char *)&numinput, sizeof(uint64_t));
        in.read((char *)&numinput_32, sizeof(uint32_t));
        Searcher tmp=Searcher(numinput,numinput_32);
        this->searcharray.push_back(tmp);
    }
}





