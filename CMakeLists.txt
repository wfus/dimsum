cmake_minimum_required(VERSION 2.8.9)
project(DIMSUM)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set( CMAKE_CXX_FLAGS "-Wall -O3 " )

set(SOURCES src/prng.cc src/countmin.cc src/alosum.cc src/dimsumpp.cc src/alosumpp.cc)

add_executable(wfu src/wfu.cc ${SOURCES})
add_executable(hh src/hh.cc ${SOURCES})

add_executable(dimsum src/dimsum_demo.cc ${SOURCES})
