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
    dimsum.update(69, 4);
    dimsum.update(70, 5);
    dimsum.update(71, 6);
    dimsum.update(72, 7);
    dimsum.show_table();

    dimsum.update(73, 8);
    dimsum.update(74, 9);
    dimsum.update(75, 10);
    dimsum.update(76, 11);
    dimsum.show_table();
    dimsum.update(77, 12);
    dimsum.show_table();

    dimsum.swap_small_large_passive(1, 0);
    dimsum.show_table();
    dimsum.swap_small_large_passive(2, 1);
    dimsum.show_table();
    dimsum.swap_small_large_passive(0, 1);
    dimsum.show_table();

    //dimsum.show_hash();

    return 0;
}