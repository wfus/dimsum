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
    DIMSUMpp dimsum(0.02, 0.5);
    // dimsum.show_table();
    std::cout << "Adding things" << std::endl;
    for (int i = 1; i < 10000000; i++) {
        dimsum.update(hash31(1, 2, i) % 1000, hash31(444, 442, i) % 1000);
    }

    std::cout << "Ehh good morning" << std::endl;
    dimsum.show_table();

    dimsum.show_hash();


    return 0;
}