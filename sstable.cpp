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
//如果未找到返回false,//似乎只需要返回下标就行
bool Sstable::getoffset(uint64_t key, int &index,bool & if_del)
{
  //  if (key > this->header.max_key || key < this->header.min_key) return 0;
    //用bloomfilter判断key是否存在
    vector<unsigned int>hash_numgroup= gethash(key);
    for(int i=0;i<4;i++){
        if(!bloomfilter.is_in_bitarray(hash_numgroup[i])) return false;
    }
    //用二分法找到对应索引,没有则返回0
    int left=0;
    int right=this->searcharray.size()-1;
    while(left<=right){
        int mid=(left+right)/2;
        if(searcharray[mid].key==key){
            if(searcharray[mid].del) {
                if_del=true;
                return false;//被删除了
            }
            else {
                index=mid;
                return true;
            };
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

vector<unsigned int>Sstable::gethash(uint64_t num) {
    unsigned int hashresult[4] = {0};
    void* key=&num;
    MurmurHash3_x64_128(key,8,1,hashresult);
    vector<unsigned int> result;
    for(int i=0;i<4;i++){
        result.push_back(hashresult[i]);
    }
    return result;
}

void Sstable::setfilter(uint64_t key) {
     vector<unsigned int> result= gethash(key);
     for(int i=0;i<4;i++){
         this->bloomfilter.add_to_bitarray(result[i]);
     }
     return;
}
//跳表生成Sstable的辅助函数
void Sstable::addkey(uint64_t key,bool del) {
   //更新头部
   this->header.updatehead(key);
   //布隆过滤器
   this->setfilter(key);
   //索引区加一
   Searcher search(key,del);
   this->searcharray.push_back(search);
}

std::string Sstable::get(uint64_t key,string filename,bool & if_del) {
//    string filename="data\\0\\test.sst";
    int index=0;
    //读的大小
    unsigned int size;
    if(this->getoffset(key,index,if_del)){
        ifstream in(filename,ios::in|ios::binary);
        in.seekg(0,ios_base::end);
        unsigned length=in.tellg();
        //如果要读的是最后一个value
        if(index==this->searcharray.size()-1){
         size=length-this->searcharray[index].offset;
         char* c=new char[size];
         in.seekg(this->searcharray[index].offset);
         in.read(c,size);
         std::string result=c;
         delete c;
         return result;
        }
        else{
            size=this->searcharray[index+1].offset-this->searcharray[index].offset;
            char c[100];
            in.seekg(this->searcharray[index].offset);
            in.read(c,100);
            std::string result=c;
            return result;
        }
    }
    else{
        return "";
    }
}

Sstable::Sstable(std::string filename) {
    ifstream in(filename,ios::in|ios::binary);
    //读入头部
    uint64_t numinput;
    uint32_t numinput_32;
    bool del;
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
    for(unsigned int i=0;i<(this->header.num);i++){
        in.read((char *)&numinput, sizeof(uint64_t));
        in.read((char *)&numinput_32, sizeof(uint32_t));
        in.read((char *)&del, sizeof(bool));
        Searcher tmp=Searcher(numinput,numinput_32,del);
        this->searcharray.push_back(tmp);
    }


}





