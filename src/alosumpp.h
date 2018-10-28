/*
 * Implementation of the IM-SUM Algorithm using
 * C++ classes.
 */
#pragma once
#include "prng.h"

/////////////////////////////////////////////////////////
#define ALSweight_t int
//#define ALS_SIZE 101 // size of k, for the summary
// if not defined, then it is dynamically allocated based on user parameter
////////////////////////////////////////////////////////
#define ALSitem_t uint32_t
#define GAMMA 1.0
#define ALS_HASHMULT 3  // how big to make the hashtable of elements:
#ifdef ALS_SIZE
#define ALS_SPACE (ALS_HASHMULT * ALS_SIZE)
#endif

typedef struct ALScounter_t ALSCounter;
struct ALScounter_t {
	ALSitem_t item; // item identifier
	int hash; // its hash value
	ALSweight_t count; // (upper bound on) count for the item
	ALSCounter *prev, *next; // pointers in doubly linked list for hashtable
};

class ALS {
    ALSweight_t n;
    int hasha, hashb, hashsize;
	int countersize, maxMaintenanceTime;
	int nActive, nPassive, extra, movedFromPassive;
	int* buffer;
	int quantile;  // error term representing count uncertainty
	float epsilon;  // Use 1/epsilon base space for each hashtable - gives guarantees
	float gamma;  // amount of extra space used more than 1/epsilon
	void* handle;
	ALSCounter* activeCounters;  // Counters inside our active hashtable
	ALSCounter* passiveCounters;  // Counters inside our passive hashtable
	ALSCounter** activeHashtable; // array of pointers to items in 'counters'
	ALSCounter** passiveHashtable; // array of pointers to items in 'counters'

public:
    ALS(float, float);
    ~ALS();
    void update();
    int size();
    int point_est(ALSitem_t);
    int point_err();
    std::map<uint32_t, uint32_t> output(uint64_t);
    int in_place_find_kth(int*, int, int, int, int);

    // query functions
    ALSCounter* find_item(ALSitem_t);

    // modification
    void add_item(ALSitem_t, ALSweight_t);
    
    // debugging functions
    void show_hash();
    void show_heap();
    void check_hash(int, int);

private:
    void init_passive();
    void destroy_passive();
    
    // internal query functions
    ALSCounter* find_item_in_active(ALSitem_t);
    ALSCounter* find_item_in_passive(ALSitem_t);
};
