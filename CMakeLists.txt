cmake_minimum_required(VERSION 3.19)
project(LSM)

set(CMAKE_CXX_STANDARD 14)
file(GLOB SOURCES
#        main.cpp
        kvstore.cpp
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
        skiplist.cpp
        skiplist.h
        sstable.cpp
        sstable.h
        level.cpp
        level.h
        utils.h
        test.h
        # correctness.cc
        persistence.cc
        # main.cpp
)

add_executable(LSM persistence.cc ${SOURCE_FILES}${SOURCES})