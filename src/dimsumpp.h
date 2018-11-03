/*
 * Implementation of the DIM-SUM++ Algorithm in C++.
 * This is a deamortized heavy hitter algorithm that
 * uses half the space of the regular DIM-SUM algorithm.
 */
#include "prng.h"
#define DIMweight_t int
#define DIMitem_t uint32_t
#define GAMMA 1.0
#define DIM_HASHMULT 3
#ifdef DIM_SIZE
#define DIM_SPACE (DIM_HASHMULT * DIM_SIZE)
#endif

typedef struct DIMcounter_t DIMCounter;
struct DIMcounter_t {
    DIMitem_t item; // item identifier
    int hash; // item hash value
    DIMweight_t count; // (upper bound on) count fothe item
    DIMcounter_t *prev, *next;  // doubly linked list for hashtable
};


class DIMSUM {

    int hasha, hashb, hashsize;
    int countersize, maxMaintenanceTime;
    int nActive, nPassive, extra, movedFromPassive;

    int* buffer;
    int quantile;
    float epsilon;
    float gamma;
    void* handle;

    DIMCounter* activeCounters;
    DIMCounter* passiveCounters;
    DIMCounter** activeHashtable;
    DIMCounter** passiveHashtable;

public:
    DIMSUM(float, float);
    ~DIMSUM();
    void update(DIMitem_t, DIMweight_t);
    int size();
    int point_est(DIMitem_t);
    int point_err();
    std::map<uint32_t, uint32_t> output(uint64_t);
    int in_place_find_kth(int*, int, int, int, int);

    // query functions
    DIMCounter* find_item(DIMitem_t);

    // what the user calls 
    void add_item(DIMitem_t, DIMweight_t);

    // debugging for days
    void show_hash();
    void show_heap();
    void check_hash(int, int);

private:
    void init_passive();
    void destroy_passive();

    // internal query functions
    DIMCounter* find_item_in_active(DIMitem_t);
    DIMCounter* find_item_in_passive(DIMitem_t);
};
