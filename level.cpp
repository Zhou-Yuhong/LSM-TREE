//
// Created by 周昱宏 on 2021/4/24.
//

#include "level.h"
#include "utils.h"
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
    if(!utils::dirExists("./DATA")){
        utils::_mkdir("./DATA");
    }
    std::string filename="./DATA\\level_" + to_string(this->level_id);
    const char* tmp=filename.c_str();
    if(!utils::dirExists(tmp)){
       utils::_mkdir(tmp);
    }
    int i=0;
    //找到最近的空下标
    while(this->is_create[i]){
        i++;
    }
    filename=filename+"\\sstable_"+ to_string(this->sstable_id);
    ofstream out(filename,ios::out|ios::binary);
    //写头部
    out.write((char *)&(sstable->header.time), sizeof(uint64_t));
    out.write((char *)&(sstable->header.num),sizeof (uint64_t));
    out.write((char *)&(sstable->header.min_key),sizeof (uint64_t));
    out.write((char *)&(sstable->header.max_key),sizeof (uint64_t));
    //写bloomfilter
    out.write(sstable->bloomfilter.bitarray,sizeof(char)*10240);
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
    //把生成的sstable,进行包裹，再加入到level中的vector<Sstable_Wrap*>中,同时sstable_id+1
    Sstable_Wrap* sstable_wrap=new Sstable_Wrap(sstable,this->sstable_id);
    this->file[i]=sstable_wrap;
    this->is_create[i]=true;
    this->sstable_id++;
    //this->cap++;
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
        sstable=file[index]->sstable;
        if(key<=sstable->header.max_key&&key>=sstable->header.min_key) {
            filename = "./DATA\\level_" + to_string(this->level_id) + "\\sstable_" + to_string(file[index]->seq_num);
            result = sstable->get(key, filename);
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

void Level::add_sstable(Sstable *sstable,int sstable_seq_num) {

   // this->file[index]=sstable;
    //this->is_create[index]=true;
    //先找到合适的插入位置（时间戳以及是否为空）
    int index=0;
    while(this->is_create[index]&&(this->file[index]->sstable->header.time<sstable->header.time)){
        index++;
    }
    //如果是空的，直接插入
    if(this->is_create[index]==false){
    Sstable_Wrap *sstable_wrap=new Sstable_Wrap(sstable,sstable_seq_num);
    this->file[index]=sstable_wrap;
    this->is_create[index]=true;
    this->sstable_id=(sstable_seq_num>sstable_id) ? sstable_seq_num+1:sstable_id;
    //this->cap++;
    return;
    }
    //否则，不是空的但是应该插入该位置
    else{
        Sstable_Wrap *sstable_wrap=new Sstable_Wrap(sstable,sstable_seq_num);
        int cap=0;
        while(this->is_create[cap]){
            cap++;
        }
        //把index到最后的sstable_wrap*都往后移一位
        this->is_create[cap]=true;
        for(int j=cap;j>index;j--){
            this->file[j]=this->file[j-1];
        }
        //把新的插入到index的位置上
        this->file[index]=sstable_wrap;
        this->sstable_id=(sstable_seq_num>sstable_id) ? sstable_seq_num+1:sstable_id;
        return;
    }
}

bool Level::if_full() {
    return this->is_create[this->file.size()-1];
}

void Level::GetLevelRange(uint64_t &minkey,uint64_t& maxkey) {
     int i=0;
     while(this->is_create[i]){
         minkey= this->file[i]->getminkey()>minkey ?  minkey:this->file[i]->getminkey();
         maxkey= this->file[i]->getmaxkey()>maxkey ?  this->file[i]->getmaxkey():maxkey;
         i++;
     }
     return;
}
//由vector<comp_node*> 生成vector<Sstable_wrap*>,并要求在文件中写
vector<Sstable_Wrap *> Level::translate(vector<comp_node *>A) {
    vector<Sstable_Wrap*>result;
    Sstable* sstable=new Sstable();
    int cap=32+10240;
    unsigned int offset_tmp=0;
    vector<string> valueGroup;
    vector<uint64_t> keyGroup;
    vector<unsigned int> offsetGroup;
    for(int i=0;i<A.size();i++){
        sstable->addkey(A[i]->key);
        keyGroup.push_back(A[i]->key);
        valueGroup.push_back(A[i]->val);
        cap+=(A[i]->val).length()+12;
        //如果满了，或者读完了，则写入文件
        if(cap>=2048*1000||i==A.size()-1){
            string filename="./DATA\\level_"+ to_string(this->level_id)+"\\sstable_"+ to_string(this->sstable_id);
            ofstream out(filename,ios::out|ios::binary);
            //写头部
            out.write((char *)&sstable->header.time,sizeof(uint64_t));
            out.write((char *)&sstable->header.num,sizeof(uint64_t));
            out.write((char*)&sstable->header.min_key,sizeof (uint64_t));
            out.write((char*)&sstable->header.max_key,sizeof (uint64_t));
            //写bloomfilter
            out.write((char *)&sstable->bloomfilter.bitarray,sizeof(char)*10240);
            //写索引，偏移量先填0
            for(int j=0;j<sstable->header.num;j++){
                out.write((char *)&(keyGroup[j]),sizeof (uint64_t));
                out.write((char*)&offset_tmp,sizeof (uint32_t));
            }
            //写value，存储偏移量
            for(int j=0;j<sstable->header.num;j++){
                offset_tmp=out.tellp();
                offsetGroup.push_back(offset_tmp);
                //把string转成char*写
                const char* input=valueGroup[j].c_str();
                out.write(input,valueGroup[j].size());
            }
            //回写偏移量
            offset_tmp=32+10240+8;
            for(int j=0;j<sstable->header.num;j++){
                sstable->searcharray[j].offset=offsetGroup[j];
                out.seekp(offset_tmp);
                out.write((char*)&offsetGroup[j], sizeof(unsigned int));
                offset_tmp+=12;
            }
                        //把生成的sstable,进行包裹,生成Sstable_wrap
            Sstable_Wrap* sstable_wrap=new Sstable_Wrap(sstable,this->sstable_id);
            this->sstable_id++;
            result.push_back(sstable_wrap);
            //清空valueGroup和keyGroup,cap,offsetGroup,建立一个新的sstable
            cap=32+10240;
            keyGroup.clear();
            valueGroup.clear();
            offsetGroup.clear();
            sstable=new Sstable();
        }
    }
    return result;
}
//由多路vector<com_node*> 归并后加入该层，返回要加入下层的vector<com_node*>
vector<vector<comp_node *>> Level::merge(vector<vector<comp_node *>> A) {
    //save the result
    vector<vector<comp_node*>> result;
    //把所有的路归并到一路
    vector<comp_node*> totle= kMergeSort(A,0,A.size()-1);
    //转化成一组sstable_wrap
    vector<Sstable_Wrap *> translate_result=this->translate(totle);
    //得到当前存储的sstablewrap数量
    int cap=0;
    while(this->is_create[cap]){
        cap++;
    }
    //要加入下层的数量
    int extraNum=cap+translate_result.size()-this->file.size();
    //没有超出，直接加到末尾就行
    if(extraNum<=0){
        for(int i=0;i<translate_result.size();i++){
            this->is_create[cap]=true;
            this->file[cap]=translate_result[i];
            cap++;
        }
    }
    //有超出
    else{
       //translate the extra sstable_wrap* into vector<comp_node*>
       for(int i=0;i<extraNum;i++){
           vector<comp_node*> tmp= translate(this->file[i]);
           result.push_back(tmp);
           //delete the file,set the relate thing
           string filename="./DATA\\level_"+to_string(this->level_id)+"\\sstable_"+ to_string(this->file[i]->seq_num);
           utils::rmfile(filename.c_str());
           this->is_create[i]= false;
           this->file[i]= nullptr;
       }
       //move the other sstable_wrap* foward
       int move_begin_index=extraNum;
       while(this->is_create[move_begin_index]){
           this->file[move_begin_index-extraNum]=this->file[move_begin_index];
           this->is_create[move_begin_index]=false;
           this->is_create[move_begin_index-extraNum]=true;
           this->file[move_begin_index]=nullptr;
           move_begin_index++;
       }
       //add the new-made sstable_wrap*
       int add_begin_index=0;
       while(this->is_create[add_begin_index]){
           add_begin_index++;
       }
       for(int i=0;i<translate_result.size();i++){
           this->file[add_begin_index]=translate_result[i];
           this->is_create[add_begin_index]=true;
           add_begin_index++;
       }
    }
    return result;
}
//k路归并
vector<comp_node *> Level::kMergeSort(vector<vector<comp_node *>> A, int start, int end) {
    if(start >=end){
        return A[start];
    }
    int mid = start + (end-start)/2;
    vector<comp_node*>Left= kMergeSort(A,start,mid);
    vector<comp_node*>Right= kMergeSort(A,mid+1,end);
    return mergeTwoArrays(Left,Right);
}
//二路归并
vector<comp_node *> Level::mergeTwoArrays(vector<comp_node *>A,vector<comp_node *>B) {
    vector<comp_node *> temp;
    int i = 0, j = 0;
    while (i < A.size() && j < B.size()) {
        if (A[i]->key < B[j]->key) {
            temp.push_back(A[i++]);
            continue;
        }
        if (A[i]->key > B[j]->key) {
            temp.push_back(B[j++]);
            continue;
        }
        if (A[i]->key == B[j]->key) {
            if (A[i]->time > B[j]->time) {
                temp.push_back(A[i++]);
                j++;
                continue;
            } else {
                temp.push_back(B[j++]);
                i++;
                continue;
            }
        }
    }
    while (i < A.size()) {
        temp.push_back(A[i++]);
    }
    while (j < B.size()) {
        temp.push_back(B[j++]);
    }
    return temp;
}

vector<comp_node *> Level::translate(Sstable_Wrap * sstableWrap) {
    Sstable *sstable=sstableWrap->sstable;
    string filename="./DATA\\level_"+ to_string(this->level_id)+"\\sstable_"+ to_string(sstableWrap->seq_num);
    return sstable->get_all_node(filename);
}



