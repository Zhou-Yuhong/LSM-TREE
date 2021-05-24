//
// Created by 周昱宏 on 2021/4/22.
//

#include "kvstore.h"
#include <string>
#include <cwchar>
#include <io.h>
#include <cstring>
#include "utils.h"
//#include <iostream>
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    //先创建1个level
        Level* level=new Level(0);
        this->file_level.push_back(level);
        //加
        this->addlevel(2);
    //从dir中读出所有sstable文件
    std::vector<string> first_level_files;
    std::string first_filename;
    utils::scanDir(dir,first_level_files);
    std::vector<string> second_level_files;
    std::vector<string> fullname_files;
    for(int i=0;i<first_level_files.size();i++){
        first_filename=first_level_files[i];
        utils::scanDir(dir+"\\"+first_filename,second_level_files);
        for(int j=0;j<second_level_files.size();j++) {
            fullname_files.push_back(dir+"\\" + first_filename + "\\" + second_level_files[j]);
        }
        second_level_files.clear();
    }
    //存储从string中得到的信息
    vector<int> numset;
    for(int i=0;i<fullname_files.size();i++){
        //如果读出的level超出内存中的level层数，加层
//        sstable_num= files[i][files[i].length()-1]-'0';
        numset=this->GetStringNum(fullname_files[i]);
        if((numset[0]+1)>=this->file_level.size()){
            this->addlevel(numset[0]+1);
        }
        //初始化,加入新的sstable
        Sstable *sstable=new Sstable(fullname_files[i]);
        //设置时间
        settime(sstable->header.time);
        this->file_level[numset[0]]->add_sstable(sstable,numset[1]);
    }

    //初始化跳表
    this->skiplist=new Skiplist();
}

KVStore::~KVStore()
{
    for(int i=0;i<this->file_level.size();i++){
        delete file_level[i];
    }
    delete skiplist;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    this->skiplist->put(key,s);
    //满了的话生成sstable放到第一层
    //std::cout<<this->skiplist->getcap()<<std::endl;
//    if(this->skiplist->getcap()==1673608){
//        std::cout<<"stop here";
//    }

//
//    if(this->skiplist->getcap()>2048*1000) {
//        this->addlevel(15);
//        this->file_level[14]->make_sstable(*this->skiplist);
//        //reset Skiplist
//        skiplist->reset();
//    }
     //满了的话加入第一层
     if(this->skiplist->getcap()>2048*1000){
         this->file_level[0]->make_sstable(*this->skiplist);
         //reset Skiplist
         skiplist->reset();
     }
    //如果第一层满了，进行合并操作
    if(this->file_level[0]->if_full()){
        this->Compa_level0();
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{   //先在Skiplist里寻找
    Node* target=this->skiplist->fetch(key);
    std::string result;
    //在Skiplist里找到且未被删除
    if(target!= nullptr&&(target->val!="~DELETED~")){
        return target->val;
    }
    else{
        if(target&&target->val=="~DELETED~"){
            //如果被删除了
            return "";
        }
        //在Skiplist 里没找到，到下层去找
        else{
            int level_size=this->file_level.size();
            for(int i=0;i<level_size;i++){
                result=this->file_level[i]->get(key);
                //如果未找到，则继续
                if(result=="") continue;
                else{
                    //被删除
                    if(result=="~DELETED~") return "";
                    else return result;
                }
            }
            //全没找到
            return "";
        }
    }
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
//    if((this->skiplist->fetch(key)&&this->skiplist->fetch(key)->del))
//    return false;
//    this->skiplist->del(key);
//    return true;
    //如果在skiplist里找到了
    if(this->skiplist->fetch(key)){
        //找到但是已经被删除
        if(this->skiplist->fetch(key)->val=="~DELETED~") return false;
        //找到且未被删除
        else{
            this->skiplist->del(key);
            return true;
        }
    }
    //未在skiplist里找到，增加一条键值为key，标记为删除的记录
    else{
        this->skiplist->put(key,"~DELETED~");
        if(this->skiplist->getcap()>2048*1000) {
            //暂时都放到14层
            this->file_level[14]->make_sstable(*this->skiplist);
            //reset Skiplist
            skiplist->reset();
        }
        return true;
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
}


//把level的层数增加到newsize
void KVStore::addlevel(int newsize) {
    int oldsize=this->file_level.size();
    for(int i=oldsize;i<newsize;i++){
        //创建文件夹
        if(!utils::dirExists("./DATA")){
            utils::_mkdir("./DATA");
        }
        std::string filename="./DATA\\level_" + to_string(i);
        const char* tmp=filename.c_str();
        if(!utils::dirExists(tmp)){
            utils::_mkdir(tmp);
        }
        Level *level=new Level(i);
        this->file_level.push_back(level);
    }
}

vector<int> KVStore::GetStringNum(string str) {

        static char numarr[] = {'0','1','2','3','4','5','6','7','8','9',};
        vector<int> numlist;
        int startIndex = 0;

        while (startIndex != -1)
        {
            vector<char> tempnum;

            startIndex = -1;

            for (size_t i = 0; i < str.length(); i++)
            {
                for (size_t j = 0; j < (sizeof(numarr) / sizeof(numarr[0])); j++)
                {
                    if (str[i] == numarr[j])
                    {
                        startIndex = i;
                        break;
                    }
                }

                if (startIndex != -1)
                {
                    tempnum.push_back(str[startIndex]);
                    int tempindex = 0;

                    //向下查找数据
                    char tempchar = str[startIndex + (tempindex += 1)];

                    //表示为数字
                    while (int(tempchar - 48) >= 0 && int(tempchar - 48) <= 9)
                    {
                        tempnum.push_back(tempchar);
                        tempchar = str[startIndex + (tempindex += 1)];
                    }

                    //删除查询到的数据
                    str.erase(startIndex, tempindex);
                    break;
                }
            }

            if (!tempnum.empty()) {
                //cout << "tempnum : " << string(tempnum.begin(), tempnum.end()) << endl;
                numlist.push_back(stoi(string(tempnum.begin(), tempnum.end())));
            }
        }

        return numlist;

}

void KVStore::Compa_level0() {
    uint64_t min_key=UINT64_MAX;
    uint64_t max_key=0;
    //得到第一层的key范围
    this->file_level[0]->GetLevelRange(min_key,max_key);
    //存储第一层键值有交叉的sstsble下标
    vector<int> indexSet;
    //临时存储sstable的minkey和maxkey
    uint64_t sstable_min=0;
    uint64_t sstable_max=0;
    Level* level1=this->file_level[1];
    int i=0;
    //搜寻所有键值有交集的sstable
    while(i<level1->file.size()&&level1->is_create[i]){
        sstable_min=level1->file[i]->getminkey();
        sstable_max=level1->file[i]->getmaxkey();
        if(if_cross(sstable_min,sstable_max,min_key,max_key)){
            indexSet.push_back(i);
        }
        i++;
    }
    //存储所有要归并的sstable中的value
    vector<vector<comp_node*>>  wayGroup;
    //先把level0中的路放进去
    i=0;
    string filename;
    while(i<this->file_level[0]->file.size()&&this->file_level[0]->is_create[i]){
        //取文件
        filename = "./DATA\\level_" + to_string(0) + "\\sstable_" + to_string(this->file_level[0]->file[i]->seq_num);
        vector<comp_node*> waytmp=this->file_level[0]->file[i]->sstable->get_all_node(filename);
        wayGroup.push_back(waytmp);
        //删除文件夹中的文件,把内存中的is_create和file清空
        this->file_level[0]->is_create[i]=false;
        delete this->file_level[0]->file[i];
        this->file_level[0]->file[i]= nullptr;
        utils::rmfile(filename.c_str());
        i++;
    }
    //把level0中的sst起始下标置0
    this->file_level[0]->sstable_id=0;
    //把level1中的放进去,前提是indexSet非空
    if(!indexSet.empty()) {
        for (i = 0; i < indexSet.size(); i++) {
            //取走同时把,is_create设空，file设空
            filename = "./DATA\\level_" + to_string(1) + "\\sstable_" + to_string(level1->file[indexSet[i]]->seq_num);
            vector<comp_node *> waytmp = level1->file[indexSet[i]]->sstable->get_all_node(filename);
            wayGroup.push_back(waytmp);
            //删除文件夹中的文件，把内存中的is_create和file清空
            level1->is_create[indexSet[i]] = false;
            delete level1->file[indexSet[i]];
            level1->file[indexSet[i]] = nullptr;
            utils::rmfile(filename.c_str());
            continue;
        }
        //前移，覆盖去掉的空位置
        level1->move_forword();
    }
    vector<vector<comp_node*>> next_wayGroup;
    //进行归并以及写入,从第二层开始
    int level_num=1;
    while(true){
        if(level_num>=this->file_level.size()){
            this->addlevel(level_num+1);
        }
        if(!utils::dirExists("./DATA")){
            utils::_mkdir("./DATA");
        }
        std::string filename="./DATA\\level_" + to_string(level_num);
        const char* tmp=filename.c_str();
        if(!utils::dirExists(tmp)){
            utils::_mkdir(tmp);
        }
        next_wayGroup=this->file_level[level_num]->merge(wayGroup);
        if(next_wayGroup.empty()) {
            this->clearVector(wayGroup);
            break;
        }
        //释放资源
        this->clearVector(wayGroup);
        wayGroup=next_wayGroup;
        level_num++;
    }

}

bool KVStore::if_cross(int min1,int max1,int min2,int max2) {
    int distance= (max2-min1)>0 ? max2-min1:max1-min2;
    int length=max1-min1+max2-min2;
    if(distance>length) return false;
    else return true;
}

void KVStore::clearVector(vector<vector<comp_node*>> &A) {
  for(int i=0;i<A.size();i++){
      for(int j=0;j<A[i].size();j++){
          delete A[i][j];
          A[i][j]= nullptr;
      }
      A[i].clear();
  }
  A.clear();
}

//KVStore::KVStore() {
//    //先创建10个level
//    for(int i=0;i<10;i++){
//        Level* level=new Level(i);
//        this->file_level.push_back(level);
//    }
//
//}

