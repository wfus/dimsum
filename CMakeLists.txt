cmake_minimum_required(VERSION 2.8.9)
project(DIMSUM)

set(SOURCES prng.cc countmin.cc alosum.cc alosumpp.cc)

add_executable(wfu wfu.cc ${SOURCES})
add_executable(hh hh.cc ${SOURCES})