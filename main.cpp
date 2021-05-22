#include <iostream>
#include "skiplist.h"
#include "sstable.h"
#include <fstream>
#include <direct.h>
#include <io.h>
#include <fstream>
#include <direct.h>
#include <iostream>
#include <cstring>
#include "level.h"
#include "kvstore.h"
#include <string>
using namespace std;
int main() {
    std::string source[14]={"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ccccccccccccccccccccccccccc",
                            "dddddddddddddddddddddddddddddddddd","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","ffffffffffffffffffffffffffffff",
                            "ggggggggggggggggggggggggggggggggggg","hhhhhhhhhhhhhhhhhhhhhhhhhhhhhh","iiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
                            "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj","kkkkkkkkkkkkkkkkkkkkkkkkkkkkkk","ooooooooooooooooooooooooooooooo",
                            "pppppppppppppppppppppppppppppppppppppppp","qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"};
    KVStore kvstore("./DATA");
    kvstore.Compa_level0();
//    for(uint64_t i=0;i<150000;i++){
//        kvstore.put(i,source[i%14]);
//    }
//    for(uint64_t i=0;i<100000;i++){
//        string result=kvstore.get(i);
//        if(result!=source[i%14]){
//            cout<<"expect "+source[i%14]+"  get "+result<<endl;
//        }
//    }

    return 0;
}
