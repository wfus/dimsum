/*
 * Implementation of the DIM-SUM Algorithm in C++. This is a deamortized heavy
 * hitter algorithm that uses a maintenance thread to pivot the passive table
 * while recording down flows in our active table.
 */
#include "prng.h"
#include <mutex>
#include <thread>
#include <algorithm>

#define DIMweight_t int
#define DIMitem_t uint32_t
#define GAMMA 1.0
#define DIM_HASHMULT 3
#ifdef DIM_SIZE
#define DIM_SPACE (DIM_HASHMULT * DIM_SIZE)
#endif

#define STEPS_AT_A_TIME 1
#define BLOCK_SIZE 1

typedef struct DIMcounter_t DIMCounter;
struct DIMcounter_t {
    DIMitem_t item; // item identifier
    int hash; // item hash value
    DIMweight_t count; // (upper bound on) count fothe item
    DIMcounter_t *prev, *next;  // doubly linked list for hashtable
};


class DIMSUM {

    DIMweight_t n;

    int hasha, hashb, hashsize;
    int countersize, maxMaintenanceTime;
    int nActive, nPassive, extra, movedFromPassive;

    int* buffer;
    DIMweight_t quantile;
    float epsilon;
    float gamma;
    void* handle;

    int passiveSize, activeSize;
    int activeHashSize, passiveHashSize;

    DIMCounter* activeCounters;
    DIMCounter* passiveCounters;
    DIMCounter** activeHashtable;
    DIMCounter** passiveHashtable;
    
    // locks for maintenance steps and maintanace info
    std::mutex maintenance_step_mutex, finish_update_mutex;
    int blocksLeft, blocksLeftThisUpdate;
    int left2move, copied2buffer;
    int stepsLeft, movedFromPassive, clearedFromPassive;
    bool finishedMedian;

    // cleanup code for maintenance
    bool all_done;

public:
    DIMSUM(float, float);
    ~DIMSUM();

    // user methods
    void update(DIMitem_t, DIMweight_t);
    int size();
    std::map<uint32_t, uint32_t> output(uint64_t);

    // query functions
    DIMCounter* find_item(DIMitem_t);

    // internal query methods
    void add_item(DIMitem_t, DIMweight_t);
    void add_item_to_location(DIMitem_t, DIMweight_t, DIMCounter**);
    DIMweight_t point_est(DIMitem_t);
    DIMweight_t point_err();

    // debugging for days
    void show_hash();
    void show_heap();
    void check_hash(int, int);
    void show_active_table();
    void show_large_passive_table();
    void show_small_passive_table();
    void show_passive_table();
    void show_table();
    
private:

    // allocation and deallocation
    void init_passive();
    void destroy_passive();
    void init_active();
    void destroy_active();
    
    // maintenance threads stuff
    int maintenance();
    void restart_maintenance();
    inline void finish_step();
    void do_some_clearing();
    void do_some_moving();


    // internal editing functions for adding/updating
    void add_item_to_location(DIMitem_t, DIMweight_t, DIMCounter**);
    int in_place_find_kth(int*, int, int, int, int);

    // internal query functions
    DIMCounter* find_item_in_active(DIMitem_t);
    DIMCounter* find_item_in_passive(DIMitem_t);
    DIMCounter* find_item_in_location(DIMitem_t, DIMCounter**);
};
