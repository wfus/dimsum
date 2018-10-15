/**
 * Prototype test file for testing implementation of DIM-SUM and
 * DIM-SUM++ with a C++ class implementation.
 */
#include <iostream>
#include "alosumpp.h"


int main(int argc, char** argv) {
    std::cout << "Hello good morning" << std::endl;
    ALS als(0.1, 1);
    als.add_item(1, 1);
    als.add_item(2, 10);
    als.add_item(3, 11);
    als.add_item(4, 12);
    als.add_item(5, 5);
    als.show_hash();
    return 0;
}