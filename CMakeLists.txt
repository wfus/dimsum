cmake_minimum_required(VERSION 2.8.9)
project(DIMSUM)

set(SOURCES src/prng.cc src/countmin.cc src/alosum.cc src/alosumpp.cc)

add_executable(wfu src/wfu.cc ${SOURCES})
add_executable(hh src/hh.cc ${SOURCES})