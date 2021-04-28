//
// Created by 周昱宏 on 2021/4/22.
//

#include "kvstore.h"
#include <string>
#include <cwchar>
#include <io.h>
#include <cstring>
//#include <iostream>
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    //先创建15个level
    for(int i=0;i<15;i++){
        Level* level=new Level(i);
        this->file_level.push_back(level);
    }
    //从dir中读出所有sstable文件
    std::vector<string> files;
    this->getFiles(dir,files);
    //存储从string中得到的信息
    vector<int> numset;
    for(int i=0;i<files.size();i++){
        //如果读出的level超出内存中的level层数，加层
//        sstable_num= files[i][files[i].length()-1]-'0';
        numset=this->GetStringNum(files[i]);
        if((numset[0]+1)>=this->file_level.size()){
            this->addlevel(numset[0]+1);
        }
        //初始化,加入新的sstable
        Sstable *sstable=new Sstable(files[i]);
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
    if(this->skiplist->getcap()>2048*1000) {
        this->file_level[14]->make_sstable(*this->skiplist);
        //reset Skiplist
        skiplist->reset();
    }

    //this->skiplist->reset();
    //如果第一层满了，进行合并操作

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

void KVStore::getFiles(string path, vector<string> &files) {
    //文件句柄
    long   hFile   =   0;
    //文件信息
    struct _finddata_t fileinfo;
    string p;
    if((hFile = _findfirst(p.assign(path).append("\\*").c_str(),&fileinfo)) !=  -1)
    {
        do
        {
            //如果是目录,迭代之
            //如果不是,加入列表
            if((fileinfo.attrib &  _A_SUBDIR))
            {
                if(strcmp(fileinfo.name,".") != 0  &&  strcmp(fileinfo.name,"..") != 0) {
                    getFiles(p.assign(path).append("\\").append(fileinfo.name), files);
                }
            }
            else
            {
                files.push_back(p.assign(path).append("\\").append(fileinfo.name));
            }
        }while(_findnext(hFile, &fileinfo)  == 0);
        _findclose(hFile);
    }

}
//把level的层数增加到newsize
void KVStore::addlevel(int newsize) {
    int oldsize=this->file_level.size();
    for(int i=oldsize;i<newsize;i++){
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

//KVStore::KVStore() {
//    //先创建10个level
//    for(int i=0;i<10;i++){
//        Level* level=new Level(i);
//        this->file_level.push_back(level);
//    }
//
//}

