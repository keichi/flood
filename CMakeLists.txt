cmake_minimum_required(VERSION 3.14)

PROJECT(flood)

include(FetchContent)
FetchContent_Declare(argparse GIT_REPOSITORY https://github.com/p-ranav/argparse.git)
FetchContent_MakeAvailable(argparse)

add_executable(flood main.cpp)
target_link_libraries(flood argparse)
