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
        //sstable插入key,这个过程处理了挺多东西，key和value通过下标对应
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
    //在循环中存储偏移量
    unsigned int pos;
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
}

std::string Level::get(uint64_t key) {
    //若为空
    if(this->is_create[0]== false){
        return "";
    }
    //先找到最新的sstable
    int i=0;
    while(i<this->file.size()&&this->is_create[i]){
        i++;
    }
    std::string filename;
    std::string result="";
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
    return result;
}

void Level::add_sstable(Sstable *sstable,int sstable_seq_num) {

   // this->file[index]=sstable;
    //this->is_create[index]=true;
    //先找到合适的插入位置（通过时间戳以及是否为空判断）
    int index=0;
    while(this->is_create[index]&&(this->file[index]->sstable->header.time<sstable->header.time)){
        index++;
    }
    //如果是空的，直接插入
    if(this->is_create[index]==false){
    Sstable_Wrap *sstable_wrap=new Sstable_Wrap(sstable,sstable_seq_num);
    this->file[index]=sstable_wrap;
    this->is_create[index]=true;
    //更新可用的不会重名的sstable_id
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
     while(i<this->file.size()&&this->is_create[i]){
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
    //vector<int> lengthGroup;
    //int begin_index=0;
    vector<uint64_t> keyGroup;
    vector<unsigned int> offsetGroup;
    int totle_size=A.size();
    for(int i=0;i<totle_size;i++){
        //i=4445
        uint64_t key=A[i]->key;
        sstable->addkey(A[i]->key);
        keyGroup.push_back(A[i]->key);
//        string val=A[i]->val;
        valueGroup.push_back(A[i]->val);
        cap+=A[i]->val.length()+12;
        //lengthGroup.push_back(A[i]->val.length());
        //如果满了，或者读完了，则写入文件
        if(cap>=2048*1000||i==totle_size-1){
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
               // const char* input=valueGroup[j].c_str();
                out.write(valueGroup[j].c_str(),valueGroup[j].length());
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
            //lengthGroup.clear();
            offsetGroup.clear();
            sstable=new Sstable();
        }
    }
    return result;
}
//由多路vector<com_node*> 归并后加入该层，返回要加入下层的vector<com_node*>
vector<vector<comp_node *>> Level::merge( vector<vector<comp_node *>> &A) {
    //要返回的值
    vector<vector<comp_node*>> result;
    //把所有的路归并到一路来获得范围
    vector<comp_node*> get_range= kMergeSort(A,0,A.size()-1);
    uint64_t min=UINT64_MAX;
    uint64_t max=0;
    //获得totle的最大最小键值
    this->get_road_range(get_range,min,max);
    //从该层找出键值有交叉的，为了防止内存泄漏，都加到A里
    int i=0;
    while(i<this->file.size()&&this->is_create[i]){
         uint64_t sstable_min=this->file[i]->sstable->header.min_key;
         uint64_t sstable_max=this->file[i]->sstable->header.max_key;
         if(this->if_cross(sstable_min,sstable_max,min,max)){
             //加入要归并的路
             A.push_back(this->translate(this->file[i]));
             //删除文件
             string filename="./DATA\\level_"+ to_string(this->level_id)+"\\sstable_"+ to_string(this->file[i]->seq_num);
             utils::rmfile(filename.c_str());
             //修改内存
             delete this->file[i];
             this->file[i]= nullptr;
             this->is_create[i]=false;
         }
         i++;
     }
    vector<comp_node*> totle= kMergeSort(A,0,A.size()-1);
    //转化成一组sstable_wrap
    vector<Sstable_Wrap *> translate_result=this->translate(totle);

    //得到当前存储的sstablewrap数量
    int cap=0;
    int cc=0;
    while(cc<this->file.size()){
        if(this->is_create[cc]){
            cap++;
        }
        cc++;
    }
    //要加入下层的数量
    int extraNum=cap+translate_result.size()-this->file.size();
    if(extraNum<0) extraNum=0;
    //push all the sstable_wrap* into a tmp vector
    vector<Sstable_Wrap*> tmp;
    //add the exist sstable_wrap*
    //先把空位补上
    this->move_forword();
    for(int i=0;i<cap;i++){
        tmp.push_back(this->file[i]);
    }
    //add the new made sstable_wrap*
    for(int i=0;i<translate_result.size();i++){
        tmp.push_back(translate_result[i]);
    }
    //有超出
    if(extraNum>0){
          for(int i=0;i<extraNum;i++){
           vector<comp_node*> comp_tmp= translate(tmp[i]);
           result.push_back(comp_tmp);
           //delete the file,set the relate thing
           string filename="./DATA\\level_"+to_string(this->level_id)+"\\sstable_"+ to_string(tmp[i]->seq_num);
           utils::rmfile(filename.c_str());
       }
    }
    //copy the remain sstable_wrap* to this level
    int z=0;

    for(int i=extraNum;i<tmp.size();i++){
        this->file[z]=tmp[i];
        this->is_create[z]=true;
        z++;
    }

    return result;
}
//k路归并
vector<comp_node *> Level::kMergeSort( vector<vector<comp_node *>> A, int start, int end) {
    if(start >=end){
        return A[start];
    }
    int mid = start + (end-start)/2;
    vector<comp_node*>Left= kMergeSort(A,start,mid);
    vector<comp_node*>Right= kMergeSort(A,mid+1,end);
    return mergeTwoArrays(Left,Right);
}
//二路归并
vector<comp_node *> Level::mergeTwoArrays(vector<comp_node *>&A,vector<comp_node *>&B) {
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

void Level::move_forword() {
     vector<Sstable_Wrap*>tmp;
     int i=0;
     //遍历，把所有Sstable_wrap都加入到tmp中
     while(i<this->file.size()){

         if(this->is_create[i]){
             //加入Sstable_wrap，注意不能delete，因为分配的空间有用
             tmp.push_back(this->file[i]);
             this->is_create[i]= false;
             this->file[i]= nullptr;
         }
         i++;
     }
     //把tmp分配回file
     for(int j=0;j<tmp.size();j++){
         this->is_create[j]=true;
         this->file[j]=tmp[j];
     }
}

void Level::clearVector(vector<vector<comp_node *>> &A) {
    for(int i=0;i<A.size();i++){
        for(int j=0;j<A[i].size();j++){
            delete A[i][j];
            A[i][j]= nullptr;
        }
        A[i].clear();
    }
    A.clear();
}

void Level::get_road_range(vector<comp_node*>&A,uint64_t& min,uint64_t &max) {
    for(int i=0;i<A.size();i++){
        if(A[i]->key>max) max=A[i]->key;
        if(A[i]->key<min) min=A[i]->key;
    }
}

void Level::Remove_delete() {
     vector<vector<comp_node*>> A;
     int i=0;
     while(i<this->file.size()&&this->is_create[i]){
         //加入路
         A.push_back(this->translate(this->file[i]));
         //删除文件
         string filename = "./DATA\\level_" + to_string(this->level_id) + "\\sstable_" + to_string(file[i]->seq_num);
         utils::rmfile(filename.c_str());
         //修改内存
         delete this->file[i];
         this->file[i]= nullptr;
         this->is_create[i]= false;
         i++;
     }
     //转成一路
     vector<comp_node*> totle=this->kMergeSort(A,0,A.size()-1);
     //去掉totle中的”~DELETE“
     for(int j=totle.size()-1;j>=0;j--){
         if(totle[j]->val=="~DELETED~")
         {   delete totle[j];
             totle.erase(totle.begin()+j);}
     }
     //转成一组sstable_wrap
     vector<Sstable_Wrap*> translate_result=this->translate(totle);
     for(int z=0;z<translate_result.size();z++){
         this->file[z]=translate_result[z];
         this->is_create[z]=true;
     }
     //释放内存空间
     this->clearVector(A);
}



