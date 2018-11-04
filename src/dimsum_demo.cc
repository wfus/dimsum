/**
 * Prototype test file for testing implementation of new
 * DIM-SUM++ algorithm.
 */
#include <iostream>
#include "dimsumpp.h"

int main(int argc, char** argv) {
    // epsilon = 1/20
    // gamma = 2
    // should expect 20 in large passive table
    std::cout << "Hello world" << std::endl;
    DIMSUM dimsum(0.05, 0.2);
    // dimsum.show_table();
    std::cout << "Adding things" << std::endl;
    dimsum.add_item(69, 4);
    dimsum.add_item(70, 5);
    dimsum.show_table();
     
    return 0;
}