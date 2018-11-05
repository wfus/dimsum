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
    DIMSUMpp dimsum(0.05, 0.4);
    // dimsum.show_table();
    std::cout << "Adding things" << std::endl;
    for (int i = 1; i < 100000; i++) {
        dimsum.update(i + 1000, i);
    }

    std::cout << "Ehh good morning" << std::endl;
    dimsum.show_table();


    return 0;
}