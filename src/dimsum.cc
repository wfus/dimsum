#include "dimsum.h"

#define DIM_NULLITEM 0x7FFFFFF

DIMSUM::DIMSUM(float ep, float g) {
    epsilon = ep;
    gamma = g;
    int k = 1 + (int) 1.0 / epsilon;    
    
    // Initialize the active and passive
    nActive = 0; 
    nPassive = 0;

    //TODO: Need to figure out why has to be odd.

    passiveSize = (int) (ceil(gamma / epsilon) + ceil(1 / epsilon) - 1);
    activeSize = (int) (ceil(gamma / epsilon) + ceil(1 / epsilon) - 1);
    activeHashSize = DIM_HASHMULT * activeSize;
    passiveHashSize = DIM_HASHMULT * passiveSize;
    // TODO: Understand this random constant lmao
    maxMaintenanceTime = 24 * activeSize + activeHashSize + 1;

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
    buffer = (int*) calloc(passiveSize, sizeof(int));
    blocksLeft = 0;
    left2move = 0;
    finishedMedian = false;
    stepsLeft = 0;
    movedFromPassive = 0;
    clearedFromPassive = passiveHashSize;
    copied2buffer = 0;

    // Make the maintenance thread and shit
    all_done = false;
    std::thread maintenance_thread(&DIMSUM::maintenance, this);
    maintenance_thread.detach();
}


DIMSUM::~DIMSUM() {
    std::cout << "Destroying" << std::endl;
    maintenance_step_mutex.unlock();
    finish_update_mutex.unlock();
    destroy_passive();
    destroy_active();
    free(buffer);
}



/**
 * Returns the size of ALL datastructures used, including the stuff
 * allocated onto the heap.
 */
int DIMSUM::size() {
    // TODO:
    return 0;
}


void DIMSUM::update(DIMitem_t item, DIMweight_t value) {

    // Wait for maintenance to finish running!

}

/*************************************************************************
 * MAINTENANCE THREAD STUFF 
 *************************************************************************/
int DIMSUM::maintenance() {
    // We want to run the maintenance thread forever, but only try doing the
    // maintenance if we can acquire the lock in some way.

    //TODO; finish this!
    for (;;) {
        maintenance_step_mutex.lock();
        std::cerr << "In maintenance right now!" << std::endl;
        if (all_done) {
            std::cerr << "Object getting destroyed... goodbye from maintenance thread!";
            std::cerr << std::endl;
            return 0;
        }
        std::cerr << "Getting out of maintenance..." << std::endl;
        finish_update_mutex.unlock();
    }
    return 0;
}

void DIMSUM::restart_maintenance() {
    // switch counter arrays and zero out the active array
    std::swap(activeCounters, passiveCounters);
    std::swap(activeHashtable, passiveHashtable);
    nPassive = nActive;
    nActive = 0;

    blocksLeft = (passiveHashSize + 24 * nPassive) / STEPS_AT_A_TIME + 1;

    int tmp = nPassive;
    left2move = (int) (std::min(tmp, (int) (floor(1 / epsilon))));
    finishedMedian = false;
    clearedFromPassive = 0;
    movedFromPassive = 0;
    copied2buffer = 0;
}


/*************************************************************************
 * INTERNAL UPDATING 
 *************************************************************************/


void DIMSUM::do_some_moving() {

}

void DIMSUM::do_some_clearing() {
    int updatesLeft = activeSize - nActive;
    assert(movedFromPassive == nPassive);
	assert(left2Move == 0);
	assert(updatesLeft >= 0);
}

/**
 * Adds an item to to our system. Can be executed while FindItem is running.
 * or if another add_item is running
 */
void DIMSUM::add_item(DIMitem_t item, DIMweight_t value) {
	int hashval = static_cast<int>(hash31(hasha, hashb, item) % activeHashSize);
	// Function should not have been called if there is not enough room in table to insert the item
	// This applies both to if it's called from maintenance thread and update.
	assert(nActive < activeSize);
	// put the new item into the hashtable at the beginning.
	DIMCounter* counter = &(activeCounters[nActive]);
    nActive++;
    // slot new item into hashtable
	// counter goes to the beginning of the list.
	// save the current item
	counter->item = item;
	// save the current hash
	counter->count = value;
	// If we add another item to the same chain we could have a problem here.
	DIMCounter* hashptr = activeHashtable[hashval];
	// The current head of the list becomes the second item in the list.
	counter->next = hashptr;
	// Now put the counter as the new head of the list.
	activeHashtable[hashval] = counter;
}

/**
 * Adds an item to a specified location in our hash table. Can be executed
 * when find_item or add_item is still running.
 * Only adds the item to the active table.
 */
void DIMSUM::add_item_to_location(DIMitem_t item, DIMweight_t value, DIMCounter** location) {
    // Function should not have been called if there is not enough room in table to insert the item
	// This applies both to if it's called from maintenance thread and update.
	assert(nActive < activeSize);
	DIMCounter* counter = &(activeCounters[nActive]);
    nActive++;
	// slot new item into hashtable
	// counter goes to the beginning of the list.
	// save the current item
	counter->item = item;
	// save the current hash
	counter->count = value;
	// If we add another item to the same chain we could have a problem here.
	DIMCounter* hashptr = *location;
	// The current head of the list becomes the second item in the list.
	counter->next = hashptr;
	// Now put the counter as the new head of the list.
	*location = counter;
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
	hashval = static_cast<int>(hash31(hasha, hashb, item) % passiveHashSize);
	hashptr = passiveHashtable[hashval];

	// Continue to look for the item through the LL in the passive Hashtable
	while (hashptr) {
		if (hashptr->item == item) break;
		else hashptr = hashptr->next;
	}
	return hashptr;
}

DIMCounter* DIMSUM::find_item_in_location(DIMitem_t item, DIMCounter** location) {
    DIMCounter* hashptr;
	hashptr = *location;
	// compute the hash value of the item, and begin to look for it in 
	// the hash table
	while (hashptr) {
		if (hashptr->item == item)
			break;
		else hashptr = hashptr->next;
	}
	return hashptr;
}

int DIMSUM::in_place_find_kth(int *v, int n, int k, int jump=1, int pivot=0) {
	assert(k < n);
	if ((n == 1) && (k == 0)) return v[0];
	else if (n == 2) {
		return (v[k*jump] < v[(1 - k)*jump]) ? v[k*jump] : v[(1 - k)*jump];
	}
	if (pivot == 0) {
		int m = (n + 4) / 5; // number of medians
								//allocate space for medians.
		for (int i = 0; i < m; i++) {
			finish_step();
			// if quintet is full
			int to_sort = (n - 5 * i < 3)? (n - 5 * i): 3;
			int quintet_size = (n - 5 * i < 5) ? (n - 5 * i) : 5;
			int *w = &v[5 * i * jump];
			// find 3 smallest items
			for (int j0 = 0; j0 < to_sort; j0++) {
				int jmin = j0;
				for (int j = j0 + 1; j < quintet_size; j++) {
					if (w[j*jump] < w[jmin*jump]) jmin = j;
				}
				std::swap(w[j0*jump], w[jmin*jump]);
			}
		
		}
		pivot = in_place_find_kth(v + 2*jump, (n+2)/5, (n + 2) / 5 / 2, jump * 5, 0);
	}
	// put smaller items in the beginning
	int store = 0;
	for (int i = 0; i < n; i++) {
		finish_step();
		if (v[i*jump] < pivot) {
			std::swap(v[i*jump], v[store*jump]);
			store++;
		}
	}
	// put pivots next
	int store2 = store;
	for (int i = store; i < n; i++) {
		finish_step();
		if (v[i*jump] == pivot) {
			std::swap(v[i*jump], v[store2*jump]);
			store2++;
		}
	}
	// if k is small, search for it in the beginning.
	if (store > k) {
		//std::cerr << "Beginning:" << std::endl;
		return in_place_find_kth(v, store, k, jump, 0);
	}
	// if k is large, search for it at the end.
	else if (k >= store2){
		//std::cerr << "End:" << std::endl;
		return in_place_find_kth(v + store2*jump, n - store2,
			k - store2, jump, 0);
	}
	else {
		return pivot;
	}
}

/*************************************************************************
 * QUERYING 
 *************************************************************************/
/**
 * Returns a list of items and counts that are greater than a certain threshold.
 */
std::map<uint32_t, uint32_t> DIMSUM::output(uint64_t thresh) {
    std::map<uint32_t, uint32_t> res;

    // TODO: Have to scan through the small passive table
    return res;
}

DIMweight_t DIMSUM::point_err() {
    return quantile;
}

DIMweight_t DIMSUM::point_est(DIMitem_t item) {
    DIMCounter* a;
    a = find_item(item);
    return a ? a->count : quantile;
}

/*************************************************************************
 * DEBUGGING (I enjoy debugging in a very deep level.)
 *************************************************************************/
void DIMSUM::show_passive_table() {
    std::cout << std::endl;
}

void DIMSUM::show_active_table() {
    for (int i = 0; i < activeSize; i++) {
        std::cout << "|" << activeCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUM::show_table() {
    show_passive_table();
    show_active_table();
}

/*************************************************************************
 * Helper Allocation and Deallocation functions 
 *************************************************************************/
inline void DIMSUM::finish_step() {
    blocksLeft--;
    blocksLeftThisUpdate--;
    if (blocksLeftThisUpdate == 0) {
        finish_update_mutex.unlock();
    }
}

void DIMSUM::init_passive() {
    passiveCounters = (DIMCounter*) calloc(passiveSize, sizeof(DIMCounter));
    passiveHashtable = (DIMCounter**) calloc(passiveHashSize, sizeof(DIMCounter*));
    for (int i = 0; i < passiveHashSize; i++) {
        finish_step();
        passiveHashtable[i] = NULL;
    }
    nPassive = 0;
}

void DIMSUM::destroy_passive() {
    //TODO
    all_done = true;
    maintenance_step_mutex.unlock();
    free(passiveHashtable);
    free(passiveCounters);
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
