#include "dimsumpp.h"

#define DIM_NULLITEM 0x7FFFFFF

DIMSUM::DIMSUM(float ep, float g) {
    epsilon = ep;
    gamma = g;
    int k = 1 + (int) 1.0 / epsilon;    
    
    // Initialize the active and passive
    nActive = 0; 
    nLargePassive = 0;
    nSmallPassive = 0;

    /* Our setup kind of looks like this 
     *  |----------------------------------|---|---|-------|
     *  |       PASSIVE (e^{-1})           |AT |INP|PASSIVE|
     *  |----------------------------------|---|---|-------|
     * where everytime we pivot, we swap the locations of AT and
     * the second passive table, so it could also look like
     *  |----------------------------------|-------|---|---|
     *  |       PASSIVE (e^{-1})           |PASSIVE|AT |INP|
     *  |----------------------------------|-------|---|---| 
     * The first passive section will be e^{-1}
     * The small passive section will be g/2e
     * The small active table and the input table will be g/4e each
     */
    largePassiveSize = (int) (ceil(1.0 / epsilon));
    smallPassiveSize = (int) (ceil(0.5 * gamma / epsilon));
    activeSize = (int) (ceil(0.5 * gamma / epsilon));

    // Need to decide on maintainance time for
    // rebalancing the active and passive tables
    // Need to do this if active table fills up at least.
    maxMaintenanceTime = activeSize; 
    
    // hard coded constants for the hash table, should really generate these
	// randomly later. Currently hardcoded for paper reproduciblity.
    hasha = 151261303;
	hashb = 6722461; 
    n = (DIMweight_t) 0;

    init_active();
    init_passive();

    // Allocate the number of spaces to our buffer for
    // finding the topk and quantile stuff
    quantile = 0;
}


DIMSUM::~DIMSUM() {
    std::cout << "Destroying" << std::endl;
    destroy_passive();
    destroy_active();
}


/**
 * Adds an item to to our system. Can be executed while FindItem is running.
 */
void DIMSUM::add_item(DIMitem_t item, DIMweight_t value) {
	int hashval = static_cast<int>(hash31(hasha, hashb, item) % activeHashSize);
	DIMCounter* hashptr = activeHashtable[hashval];

	// Coherence checks - we need to make sure we always have room to
	// put in more network flows.
	if (nActive >= activeSize) {
		std::cerr << "Error! Not enough room in table." << std::endl;
		std::cerr << "Size:"<< countersize << " Active: " << nActive 
            << " LargePassive:" << nLargePassive
            << " SmallPassive:" << nSmallPassive 
			<< std::endl;
	}
	assert(nActive < activeSize);
	// put the new item into the hashtable at the beginning.
	DIMCounter* counter = &(activeCounters[nActive]);
    nActive++;
	// slot new item into hashtable
	// counter goes to the beginning of the list.
	// The current head of the list becomes the second item in the list.
	counter->next = hashptr;
	// If the list was not empty, 
	if (hashptr)
		// point the second item's previous item to the new counter.
		hashptr->prev = counter;
	// Now put the counter as the new head of the list.
	activeHashtable[hashval] = counter;
	// The head of the list has no previous item
	counter->prev = NULL;
	// save the current item
	counter->item = item;
	// save the current hash
	counter->hash = hashval; 
	// update the upper bound on the items frequency
	counter->count = value; 	
}


/**
 * Returns the size of ALL datastructures used, including the stuff
 * allocated onto the heap.
 */
int DIMSUM::size() {
    // TODO:
    return 0;
}


/*************************************************************************
 * INTERNAL QUERYING 
 *************************************************************************/
/**
 * Returns a list of items and counts that are greater than a certain threshold.
 */
std::map<uint32_t, uint32_t> DIMSUM::output(uint64_t thresh) {
    std::map<uint32_t, uint32_t> res;
    //TODO: Implement this.
    return res;
}

/*************************************************************************
 * INTERNAL QUERYING 
 *************************************************************************/
DIMCounter* DIMSUM::find_item(DIMitem_t item) {
	DIMCounter* hashptr;
	hashptr = find_item_in_active(item);
	if (!hashptr) {
		hashptr = find_item_in_passive(item);
	}
	return hashptr;
}

DIMCounter* DIMSUM::find_item_in_active(DIMitem_t item) {
	DIMCounter* hashptr;
	int hashval;
	hashval = static_cast<int>(hash31(hasha, hashb, item) % activeHashSize);
	hashptr = activeHashtable[hashval];
	// Continue to look for the item through the LL in the passive Hashtable
	while (hashptr) {
		if (hashptr->item == item) break;
		else hashptr = hashptr->next;
	}
	return hashptr;
}

DIMCounter* DIMSUM::find_item_in_passive(DIMitem_t item) {
	DIMCounter* hashptr;
	int hashval;
	hashval = static_cast<int>(hash31(hasha, hashb, item) % largePassiveHashSize);
	hashptr = largePassiveHashtable[hashval];

	// Continue to look for the item through the LL in the passive Hashtable
	while (hashptr) {
		if (hashptr->item == item) break;
		else hashptr = hashptr->next;
	}

    //TODO: Make this also search through small passive hash table
	return hashptr;
}

/*************************************************************************
 * DEBUGGING (I enjoy debugging in a very deep level.)
 *************************************************************************/
void DIMSUM::show_large_passive_table() {
    for (int i = 0; i < largePassiveSize; i++) {
        std::cout << "|" << largePassiveCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUM::show_small_passive_table() {
    for (int i = 0; i < smallPassiveSize; i++) {
        std::cout << "|" << smallPassiveCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUM::show_passive_table() {
    std::cout << "LARGE PASSIVE TABLE" << std::endl;
    show_large_passive_table();
    std::cout << "SMALL PASSIVE TABLE" << std::endl;
    show_small_passive_table();
    std::cout << std::endl;
}

void DIMSUM::show_active_table() {
    for (int i = 0; i < activeSize; i++) {
        std::cout << "|" << activeCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUM::show_table() {
    std::cout << "LARGE PASSIVE TABLE" << std::endl;
    show_large_passive_table();
    std::cout << "SMALL PASSIVE TABLE" << std::endl;
    show_small_passive_table();
    std::cout << "ACTIVE TABLE" << std::endl;
    show_active_table();
}

/*************************************************************************
 * Helper Allocation and Deallocation functions 
 *************************************************************************/
void DIMSUM::init_passive() {
    // Allocate the large hash table. 
    largePassiveHashSize = DIM_HASHMULT * largePassiveSize;
    largePassiveCounters = (DIMCounter *) calloc(largePassiveSize, sizeof(DIMCounter));
    largePassiveHashtable = (DIMCounter**) calloc(largePassiveHashSize, sizeof(DIMCounter*));
    for (int i = 0; i < largePassiveHashSize; i++) {
        largePassiveHashtable[i] = NULL;
    }
    for (int i = 0; i < countersize; i++) {
		// initialize items and counters to zero
		largePassiveCounters[i].next = NULL;
		largePassiveCounters[i].prev = NULL;
		largePassiveCounters[i].item = DIM_NULLITEM;
	}
	nLargePassive = 0;

    // Allocate the small hash table
    smallPassiveHashSize = DIM_HASHMULT * smallPassiveSize;
    smallPassiveCounters = (DIMCounter *) calloc(smallPassiveSize, sizeof(DIMCounter));
    smallPassiveHashtable = (DIMCounter**) calloc(smallPassiveHashSize, sizeof(DIMCounter*));
    for (int i = 0; i < smallPassiveHashSize; i++) {
        smallPassiveHashtable[i] = NULL;
    }
    for (int i = 0; i < countersize; i++) {
		smallPassiveCounters[i].next = NULL;
		smallPassiveCounters[i].prev = NULL;
		smallPassiveCounters[i].item = DIM_NULLITEM;
	}
	nSmallPassive = 0;
}

void DIMSUM::destroy_passive() {
    free(smallPassiveHashtable);
    free(smallPassiveCounters);
    free(largePassiveHashtable);
    free(largePassiveCounters);
}

void DIMSUM::init_active() {
    // Allocate the large hash table. 
    activeHashSize = DIM_HASHMULT * activeSize;
    activeCounters = (DIMCounter *) calloc(activeSize, sizeof(DIMCounter));
    activeHashtable = (DIMCounter**) calloc(activeHashSize, sizeof(DIMCounter*));
    for (int i = 0; i < activeHashSize; i++) {
        activeHashtable[i] = NULL;
    }
    for (int i = 0; i < activeSize; i++) {
        // initialize items and counters to zero
        activeCounters[i].next = NULL;
        activeCounters[i].prev = NULL;
        activeCounters[i].item = DIM_NULLITEM;
    }
    nActive = 0;
}

void DIMSUM::destroy_active() {
    free(activeCounters);
    free(activeHashtable);
}
