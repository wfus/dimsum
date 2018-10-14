#include "alosumpp.h"

#define ALS_NULLITEM 0x7FFFFFFF
#define swap(x,y) do {int t=x; x=y; y=t;} while(0)

/**
 * Constructs a DIM-SUM 2 Table Implementation with parameters
 *      gamma = gamma (extra overhead space)
 *      fPhi = epsilon (size of table for accuracy guarantees)
 */
ALS::ALS(float fPhi, float gamma) {
    int i;
	// needs to be odd so that the heap always has either both children or 
	// no children present in the data structure
    int k = 1 + (int) 1.0 / fPhi;

    epsilon = fPhi;
	countersize = int(ceil(gamma / fPhi) + ceil(1 / fPhi) - 1);
	maxMaintenanceTime = int(ceil(gamma / fPhi));
	hashsize = ALS_HASHMULT * countersize;
	
    // hard coded constants for the hash table,
	// should really generate these randomly
    hasha = 151261303;
	hashb = 6722461; 

    n = (ALSweight_t) 0;
}

ALS::~ALS() {
    free(activeHashtable);
    free(activeCounters);
    free(buffer);
    destroy_passive();
}

void ALS::update() {

};

/*
 * Initializes the passive hashtable and counters to NULL
 */
void ALS::init_passive() {
    passiveCounters = (ALSCounter*) calloc(countersize, sizeof(ALSCounter));
    passiveHashtable = (ALSCounter**) calloc(hashsize, sizeof(ALSCounter*));
    for (int i = 0; i < hashsize; i++) {
        passiveHashtable[i] = NULL;
    }
    for (int i = 0; i < countersize; i++) {
		// initialize items and counters to zero
		passiveCounters[i].next = NULL;
		passiveCounters[i].prev = NULL;
		passiveCounters[i].item = ALS_NULLITEM;
	}
	nPassive = 0;
}

ALS::destroy_passive() {
    free(passiveHashtable);
    free(passiveCounters);
}

int ALS::size() {
    return sizeof(ALS) + countersize * sizeof(int)    // size of median buffer
		+ 2 * (hashsize * sizeof(ALSCounter*))        // two hash tables
		+ 2 * (countersize * sizeof(ALSCounter));     // two counter arrays
};

int ALS::point_est() {

};

int ALS::point_err() {

};

void ALS::check_hash() {

};

/**
 * Returns the Elephant Flows - All flows that have been estimated to
 * be greater than value "thresh".
 */
std::map<uint32_t, uint32_t> ALS::output(uint64_t thresh) {
	std::map<uint32_t, uint32_t> res;
    // Iterate through active block. Then, we iterate through 
};

int ALS::in_place_find_kth() {

};