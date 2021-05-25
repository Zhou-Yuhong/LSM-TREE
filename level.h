//
// Created by 周昱宏 on 2021/4/24.
//

#ifndef LSM_LEVEL_H
#define LSM_LEVEL_H
#include "skiplist.h"
#include "sstable.h"
//#include "LeakDetector.h"
//建立sstable* 和sstable文件的联系
struct Sstable_Wrap{
    Sstable * sstable;
    //sstable文件的最后数字
    int seq_num;
    Sstable_Wrap(Sstable * sstable,int seq_num){
        this->sstable=sstable;
        this->seq_num=seq_num;
    }
    ~Sstable_Wrap(){
        delete sstable;
    }
    void getRange(uint64_t & minkey,uint64_t &maxkey){
        minkey=this->sstable->header.min_key;
        maxkey=this->sstable->header.max_key;
    };
    uint64_t getminkey(){
        return this->sstable->header.min_key;
    };
    uint64_t getmaxkey(){
        return this->sstable->header.max_key;
    };
};
class Level{
public:
    //标明哪层
    int level_id;
    //.sst文件后缀,新建的时候用,防止重名
    int sstable_id=0;
    //is_create和file一一对应
    bool *is_create;
    //时间戳越大的越后面
    vector<Sstable_Wrap*> file;
    Level(int id){
        this->level_id=id;
        is_create=new bool[((1<<(level_id+1))+1)];
        for(int i=0;i<((1<<(level_id+1))+1);i++){
            is_create[i]=false;
            file.push_back(nullptr);
        }
    }
    //由Skiplist生成sstable并存入内存
    void make_sstable(Skiplist &skiplist);
    ~Level(){
        delete is_create;
        for(int i=0;i<this->file.size();i++){
            if(this->file[i]!= nullptr){
                delete this->file[i];
            }
        }
    }
    //接口供kvstore调用，参数为sstable*以及.sst的后缀数字，注意时间戳越大越往后插入
    void add_sstable(Sstable * sstable,int sstable_seq_num);
    //判断一层是否满了
    bool if_full();
    //处理函数，把Sstable_wrap*往前移动，覆盖空的位置
    void move_forword();
    //找到最新的value
    std::string get(uint64_t key);
    void clearVector(vector<vector<comp_node*>> &A);
    //找到这层的range
    void GetLevelRange(uint64_t &minkey,uint64_t& maxkey);
    //由多路vector<com_node*> 归并后加入该层，返回要加入下层的vector<com_node*>
    vector<vector<comp_node*>> merge( vector<vector<comp_node*>>&);
    //多路归并，递归实现
    vector<comp_node*> kMergeSort(vector<vector<comp_node*>>&,int start,int end);
    //两路归并
    vector<comp_node*> mergeTwoArrays(vector<comp_node*>&A,vector<comp_node*> &B);
    //把vector<comp_node*>转成 vector<Sstable_Wrap *>，并在文件中写
    vector<Sstable_Wrap *> translate(vector<comp_node*> A);
    //把一个Sstable_wrap* 转成vector<comp_node*>
    vector<comp_node *> translate(Sstable_Wrap * sstableWrap);
    void get_road_range(vector<comp_node*>&A,uint64_t &min,uint64_t &max);
    bool if_cross(int min1,int max1,int min2,int max2) {
        int distance= (max2-min1)>0 ? max2-min1:max1-min2;
        int length=max1-min1+max2-min2;
        if(distance>length) return false;
        else return true;
    }
    //去除该层所有的”~DELETE~“
    void Remove_delete();
};


#endif //LSM_LEVEL_H
