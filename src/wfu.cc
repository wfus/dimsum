/**
 * Prototype test file for testing implementation of DIM-SUM and
 * DIM-SUM++ with a C++ class implementation.
 */
#include <iostream>
#include "alosumpp.h"


int main(int argc, char** argv) {
    std::cout << "Hello good morning" << std::endl;
    ALS als(0.001, 4);
    als.show_heap();
    return 0;
}