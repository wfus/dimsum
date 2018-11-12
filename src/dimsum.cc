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
	return sizeof(this) +  // size of this data structure
		sizeof(int) * passiveSize +  // size of median buffer
		sizeof(DIMCounter*) * passiveHashSize * 2  +  // size of hash tables
		sizeof(DIMCounter) * passiveSize * 2;  // size of counters
}

/**
 * Main user-facing function. User should call this when adding a new flow
 * to the data structure
 */
void DIMSUM::update(DIMitem_t item, DIMweight_t value) {
	int updatesLeft = activeSize - nActive - left2move;
	if (updatesLeft <= 0) {
		// No more free spots in the active table, we MUST finish up the
		// maintenance. The iteration should be finished by now
		assert(stepsLeft == 0);
		// check that clearing the passive table is done
		assert(movedFromPassive == nPassive);
		if (clearedFromPassive != passiveHashSize) {
			std::cerr << "Spotted a potential error: "
				<< "clearedFromPassive: " << clearedFromPassive
				<< "passiveHashSize: " << passiveHashSize
				<< std::endl;
		}
		assert(clearedFromPassive == passiveHashSize);
		restart_maintenance();
		updatesLeft = activeSize - nActive - left2move;
	}
    
	// Now we assume that if we needed to, maintenance has been restarted
	// and we have spots left in the active table for new entries
	// We will do some updates here.
	int bltu = blocksLeft / updatesLeft;
	if (bltu > 0 && bltu < BLOCK_SIZE) {
		bltu = BLOCK_SIZE;
	}
	blocksLeftThisUpdate = bltu;
	
	// do the actual update step
	do_update(item, value);

	// Wait for maintenance to finish running!
	// wait to get the finish update mutex back!
	// Wait for maintenance to finish running if needed
	if (!finishedMedian) {
		if (copied2buffer < nPassive) {
			do_some_copying();
		}
		else if (blocksLeftThisUpdate > 0) {
			// block until update is finished.
			finish_update_mutex.lock();
			finish_update_mutex.unlock();
		}
	}
	else {
		if (movedFromPassive < nPassive) {
			do_some_moving();
		}
		else {
			do_some_clearing();
		}
	}
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
        #if DIMSUM_VERBOSE
            std::cerr << "In maintenance right now!" << std::endl;
        #endif
        if (all_done) {
            std::cerr << "Object getting destroyed... goodbye from maintenance thread!";
            std::cerr << std::endl;
            return 0;
        }

        int k = nPassive - ceil(1 / epsilon);
        if (k >= 0) {
			int median = in_place_find_kth(buffer, nPassive, k, 1, quantile + 1);
			quantile = std::max(median, quantile);
		}
		else {
			blocksLeft = (passiveHashSize + nPassive) / STEPS_AT_A_TIME + 1;
		}
		// Copy passive to active
		//std::cerr << "Copying P to A..." << std::endl;
		assert(blocksLeft >= (passiveHashSize + nPassive) / STEPS_AT_A_TIME + 1);
		blocksLeft = (passiveHashSize + nPassive) / STEPS_AT_A_TIME + 1;
		
		finishedMedian = true;
		// Release update if it is waiting
		blocksLeftThisUpdate = 0;

        #if DIMSUM_VERBOSE
            std::cerr << "Getting out of maintenance..." << std::endl;
        #endif
        finish_update_mutex.unlock();
    }
    return 0;
}

void DIMSUM::restart_maintenance() {
    // switch counter arrays and zero out the active array
    #if DIMSUM_VERBOSE
        std::cout << "Restarting the maintenance." << std::endl;
    #endif
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

void DIMSUM::do_update(DIMitem_t item, DIMweight_t value) {
	DIMCounter* hashptr;
	// find whether new item is already stored, if so store it and add one
	// update heap property if necessary
	n += value;  // update the total flow that went through this datastructure
	int hashval = (int)hash31(hasha, hashb, item) % hashsize;
	DIMCounter** location = &(activeHashtable[hashval]);
	hashptr = find_item_in_location(item, location);
	if (hashptr) {
		hashptr->count += value;  // increment the count of the item
	}
	else {
		// if control reaches here, then we have failed to find the item in the active table.
		// so, search for it in the passive table
		hashptr = find_item_in_passive(item);
		if (hashptr) {
			value += hashptr->count;
		}
		else {
			value += quantile;
		}
		// Now add the item to the active hash table.
		// This could race with adding the item from the passive table.
		// What if first we check for the item in active and it's missing, 
		// then the item is copied from the passive table
		// and then the item is added here?
		add_item_to_location(item, value, location);
	}
}

void DIMSUM::do_some_copying() {
	int updatesLeft = activeSize - nActive;
	assert(movedFromPassive == 0);
	assert(updatesLeft >= 0);
	stepsLeft = (passiveHashSize + 24 * nPassive) + 1 - copied2buffer;
	int stepsLeftThisUpdate = stepsLeft / (updatesLeft + 1);
	int k = nPassive - ceil(1 / epsilon);
	if (k >= 0) {
		for (int i = 0; i < stepsLeftThisUpdate; i++) {
			if (copied2buffer >= nPassive) {
				break;
			}
			blocksLeft--;
			buffer[copied2buffer] = passiveCounters[copied2buffer].count;
			copied2buffer++;
		}
	}
	else {
		copied2buffer = nPassive;
	}
	if (copied2buffer == nPassive) {
		blocksLeft = (passiveHashSize + 23 * nPassive) / STEPS_AT_A_TIME + 1;
		maintenance_step_mutex.unlock();
	}
}


/**
 * Moving records from the passive tables that are above the quantile
 * back to the active table, so they don't get wiped. Anything that is below
 * the quantile will get cleared out, not get moved to the active table, and
 * get overwritten later.
 */
void DIMSUM::do_some_moving() {
	int updatesLeft = activeSize - nActive - left2move;
	stepsLeft = passiveSize + nPassive - movedFromPassive;
	int steps_left_this_update = stepsLeft / (updatesLeft+1);
	int largerThanQuantile = 0;
	for (int i = 0; i < steps_left_this_update; i++) {
		if (passiveCounters[movedFromPassive].count > quantile) {
            // If our passive counter is larger than quantile, it's safe and
            // won't get wiped. However, we should see if its already in the
            // active table so we can merge it in. When an element is already
            // in the active table, the passive counter is already added to it
            // so we don't have to actually merge in the counts.
			DIMCounter* c = find_item_in_active(passiveCounters[movedFromPassive].item);
			if (!c) {
                // if it's not in the active table, move it to the active table
				add_item(passiveCounters[movedFromPassive].item,
					passiveCounters[movedFromPassive].count);
			}
			--left2move;
			++largerThanQuantile;
		}
		movedFromPassive++;
		if (movedFromPassive >= nPassive) {
            // if we've already moved same or more from passive table than the
            // number of actual records stored in passive, we can stop.
			left2move = 0;
			clearedFromPassive = 0;
			break;
		}
	}
	if (nPassive == movedFromPassive) {
		// If finished moving
		left2move = 0;
	}
}

void DIMSUM::do_some_clearing() {
    int updatesLeft = activeSize - nActive;
    assert(movedFromPassive == nPassive);
	assert(left2move == 0);
	assert(updatesLeft >= 0);
	stepsLeft = passiveHashSize - clearedFromPassive;
	int steps_left_this_update = stepsLeft / (updatesLeft + 1);
	for (int i = 0; i < steps_left_this_update; i++) {
		passiveHashtable[clearedFromPassive] = NULL;
		clearedFromPassive++;
	}
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

/**
 * All of the find_item functions return NULL if the item is not found
 */
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

/**
 * If item was present when the function was called, find it.
 *If item was not present when the function finished, return NULL.
 * Else, do either.
 * Assume: No items are deleted during the runtime of the function.
 */
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
	int i;
	for (i = 0; i < nActive; ++i)
	{
		if (activeCounters[i].count >= thresh)
			res.insert(std::pair<uint32_t, uint32_t>(activeCounters[i].item, 
				activeCounters[i].count));
	}
	for (i = 0; i < nPassive; ++i) {
		if ((find_item_in_active(passiveCounters[i].item) == NULL) 
				&& (passiveCounters[i].count >= thresh)) {
			res.insert(std::pair<uint32_t, uint32_t>(
				passiveCounters[i].item, passiveCounters[i].count));
		}
	}
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

/**
 * Debugging routine to validate one item in the hash table
 * in the active hash table.
 */
void DIMSUM::check_hash(int item, int hash) {
	int i;
	DIMCounter *hashptr, *prev;
	for (int i = 0; i < activeHashSize; i++) {
		prev = NULL;
		hashptr = activeHashtable[i];
		while (hashptr) {
			prev = hashptr;
			hashptr = hashptr->next;
		}
	}
	// TODO: Check that the item is there.q
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
	std::cerr << "Destroying the read-only passive table" << std::endl;
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
