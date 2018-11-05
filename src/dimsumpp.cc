#include "dimsumpp.h"

#define DIM_NULLITEM 0x7FFFFFF


DIMSUMpp::DIMSUMpp(float ep, float g) {
    epsilon = ep;
    gamma = g;
    
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
    extra = activeSize;

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
    
    // Initialize a buffer that we will use to find the quantile.
    // we will need to have both the large passive and small passive table
    // for this
    buffer = (int*) calloc(smallPassiveSize + largePassiveSize, sizeof(int));

    // Allocate the number of spaces to our buffer for
    // finding the topk and quantile stuff
    quantile = 0;

    // Allocate all of the shared parameters used during maintenance
    // blocksLeft = 0;
    // left2move = 0;
    // all_done = false;
    // finishedMedian = false;
    // stepsLeft = 0;
    // movedFromPassive = 0;
    // clearedFromPassive = smallPassiveHashSize;
    // copied2buffer = 0;

    // Make the maintenance thread and shit
    // std::thread maintenance_thread(&DIMSUMpp::maintenance, this);
    // maintenance_thread.detach();
}


DIMSUMpp::~DIMSUMpp() {
    /*
    std::cout << "Destroying, taking control of all the mutexes." << std::endl;
    maintenance_step_mutex.lock();
    finish_update_mutex.lock();
    std::cerr << "Took control of mutexes, destroying everying." << std::endl;
    maintenance_step_mutex.unlock();
    finish_update_mutex.unlock();
    */
    destroy_passive();
    destroy_active();
}

/**
 * Update function for our system. User should be calling this function.
 */
void DIMSUMpp::update(DIMitem_t item, DIMweight_t value) {
    int hashval;
	DIMCounter* hashptr;
	// find whether new item is already stored, if so store it and add one
	// update heap property if necessary
	
	hashptr = find_item_in_active(item);
	if (hashptr) {
		hashptr->count += value; // increment the count of the item
		return;
	}
	else {
		// if control reaches here, then we have failed to find the item in the active table.
		// so, search for it in the passive table
		hashptr = find_item_in_passive(item);
		if (hashptr) {
			value += hashptr->count;
		}
		else {
            // add error term if we could not find it in passive
			value += quantile;
		}
		//if (ALS->newItems == ALS->bucketSize) {
		if (extra <= 0) {
			// start the maintenance anew 
			restart_maintenance();
		}
		// Now add the item to the active hash table.
		extra--;
		add_item(item, value);
	}
}


/**
 * Adds an item to to our system. Can be executed while FindItem is running.
 * User should not be calling this.
 */
void DIMSUMpp::add_item(DIMitem_t item, DIMweight_t value) {
    int hashval = (int)hash31(hasha, hashb, item) % activeHashSize;
	DIMCounter* hashptr = activeHashtable[hashval];
	// so, overwrite smallest heap item and reheapify if necessary
	// fix up linked list from hashtable
	if (nActive >= activeSize) {
		std::cerr << "Error! Not enough room in table."<<std::endl;
		std::cerr << "Size:"<<activeSize << " Active: " << nActive 
            << " Large Passive:" << largePassiveSize
            << " Small Passive:" << smallPassiveSize
			<< " Extra:" << extra
            << " From passive:" << movedFromPassive 
			<< std::endl;
	}
	assert(nActive < activeSize);
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
int DIMSUMpp::size() {
    // TODO:
    return 0;
}

/*************************************************************************
 * MAINTENANCE THREAD STUFF 
 * Might not have to do maintenance for DIMSUM++ just yet.
 *************************************************************************/
void DIMSUMpp::restart_maintenance() {
    std::cerr << "Starting the maintenance..." << std::endl;
    // switch the active counter and the small passive counter
    DIMCounter* tmp = activeCounters;
    activeCounters = smallPassiveCounters;
    smallPassiveCounters = tmp;

    // switch the number of active counter and passive counter
    int t = nActive;
    nActive = nSmallPassive;
    nSmallPassive = nActive;

    // switch the actual hashtables
    DIMCounter** tmpTable = activeHashtable;
    activeHashtable = smallPassiveHashtable;
    smallPassiveHashtable = tmpTable;

    // Update the number of extra updates we have until another maintainace
    // is required
    // TODO: maybe this is wrong calculation of extra?
    extra = activeSize - nActive;
    movedFromPassive = 0;
    assert(extra >= 0);
    
    // call the real maintenance that we need once we have swapped the tables.
    maintenance();
}

void DIMSUMpp::maintenance() {
    int k = smallPassiveSize;
    if (k >= 0) {
        // copy the large buffer, then the small buffer, in sequence.
        
        for (int i = 0; i < smallPassiveSize; i++) {
            buffer[i] = smallPassiveCounters[i].count;
        }
        for (int i = 0; i < largePassiveSize; i++) {
            buffer[smallPassiveSize + i] = largePassiveCounters[i].count; 
        }
        int median = in_place_find_kth(buffer, largePassiveSize + smallPassiveSize, k, 1, quantile+1);
        std::cout << "top kth: " << median << std::endl;
        quantile = std::max(median, quantile); 
    }
    std::cout << "Quantile: " << quantile << std::endl;

    // copy from passive to SUPER passive. Since we already have the quantile,
    // we can swap out all elements that are below or equal to the quantile into
    // the small passive. We have to do the equals case in case all the flows
    // are the same size. We are also capped out at smallPassiveSize number of
    // swaps. Therefore, we just iterate through the smallPassiveTable first
    // and swap out stuff in the big passive table.
    int small = 0, large = 0, num_swaps = 0;
    while (small < smallPassiveSize && large < largePassiveSize
            && num_swaps < smallPassiveSize) {
        DIMCounter* smallctr = &smallPassiveCounters[small];
        DIMCounter* largectr = &largePassiveCounters[large];
        if (largectr->count > quantile) {
            // It's large enough - we don't want to swap it out.
            large++;
            continue;
        }
        if (smallctr->count <= quantile) {
            // It's small enough - we can leave it in old passive
            small++;
            continue;
        }
        if (smallctr->count > quantile && largectr->count <= quantile) {
            // we can swap and skip past our swap point.
            swap_small_large_passive(small, large);
            num_swaps++; small++; large++;
            continue;
        }
    }
    // After this process is done and we have updated our quantile, we can
    // feel free to run rampant and erase the things in our active site.
    extra = activeSize;
    nActive = 0;
    destroy_active();
    init_active();
}

/*
// This is the maintenance code that uses threading
int DIMSUMpp::maintenance() {
    // We want to run the maintenance thread forever, but only try doing the
    // maintenance if we can acquire the lock in some way.
    for (;;) {
        maintenance_step_mutex.lock();
        std::cerr << "In maintenance right now!" << std::endl;
        if (all_done) {
            std::cerr << "Object getting destroyed... goodbye from maintenance thread!";
            std::cerr << std::endl;
            return 0;
        }
        std::cerr << "Trying to getting out of maintenance..." << std::endl;
        finish_update_mutex.unlock();
        std::cerr << "Got out of maintenance." << std::endl;
    }
    return 0;
}
*/

/*************************************************************************
 * INTERNAL UPDATING 
 *************************************************************************/
/**
 * Swaps index i from the small passive counters with the record from
 * index j of the large passive counters. We will have to replace the values
 * store in the counters and also swap some records in their hashtables.
 */
void DIMSUMpp::swap_small_large_passive(int i, int j) {
    assert(i < smallPassiveSize);
    assert(j < largePassiveSize);

    DIMCounter *counteri = &smallPassiveCounters[i];
    DIMCounter *counterj = &largePassiveCounters[j];

    // Find the counters that actually match smallPassiveCounters[i] and
    // largePassiveCounters[j]
    if (counteri->item == DIM_NULLITEM && counterj->item == DIM_NULLITEM) {
        std::cerr << "Both swapped counters were uninitialized, this prolly shudn't happen "
            << std::endl;
        return;
    }

    // Case where both the counters actually contain something, this will
    // take much more work to do.
    if (counteri->item != DIM_NULLITEM && counterj->item != DIM_NULLITEM) {
        // std::cerr << "Swapping when both are not null" << std::endl;
        // We want to remove both of these from the hashtable. We just have
        // to be careful if it is the head of the hashtable linked list. 
        int hashi = counteri->hash;
        int hashj = counterj->hash;

        // remove the counter stuff from the linked list. The counters still
        // retain their own values.
        // if it was the head, then make the new head the next value. Otherwise,
        // we can just remove from the middle of the linked list.
        if (smallPassiveHashtable[hashi]->item == counteri->item) {
            smallPassiveHashtable[hashi] = counteri->next;
        } else {
            // it must have come from the middle if the first one wasnt it.
            // We can simply do the regular linked list deletion.
            (counteri->prev)->next = counteri->next;
        }

        if (largePassiveHashtable[hashj] ->item == counterj->item) {
            largePassiveHashtable[hashj] = counterj->next;
        } else {
            (counterj->prev)->next = counterj->next;
        } 
        counteri->prev = NULL;
        counteri->next = NULL;
        counterj->prev = NULL;
        counterj->next = NULL;

        // swap the values for this.
        std::swap(counteri->hash, counterj->hash);
        std::swap(counteri->item, counterj->item);
        std::swap(counteri->count, counterj->count);

        // Add these things back into your hashmap bruh
        // counteri is still in small passive table
        // counterj is still in large passive table
        // however, hash values are different now!
        if (smallPassiveHashtable[counteri->hash]) {
            smallPassiveHashtable[counteri->hash]->prev = counteri;
        }
        smallPassiveHashtable[counteri->hash] = counteri;
        counteri->prev = NULL;
        
        if (largePassiveHashtable[counterj->hash]) {
            largePassiveHashtable[counterj->hash]->prev = counterj;
        }
        largePassiveHashtable[counterj->hash] = counterj;
        counterj->prev = NULL;
        return;
    }

    if (counteri->item == DIM_NULLITEM) {
        std::cerr << "This probably shouldn't happen normally..." << std::endl;
        assert(false);
        return;
    }

    if (counterj->item == DIM_NULLITEM) {
        // std::cerr << "Swapping small passive with empty large passive " << std::endl;
        // Counteri is not null but counterj is NULL.
        // clear out the entry from the hashtable for counteri
        if (counteri->prev) {
            (counteri->prev)->next = counteri->next;
        } else {
            smallPassiveHashtable[counteri->hash] = counteri->next;
        }
        counteri->prev = NULL;
        counteri->next = NULL;
        counterj->prev = NULL;
        counterj->next = NULL;
        
        std::cout << "BEFORE SWAP" << std::endl;
        std::cout << counteri->hash << " " << counteri->item << " " << counteri->count << std::endl; 
        std::cout << counterj->hash << " " << counterj->item << " " << counterj->count << std::endl; 
        // swap i and j's info.
        std::swap(counteri->hash, counterj->hash);
        std::swap(counteri->item, counterj->item);
        std::swap(counteri->count, counterj->count);
        std::cout << "AFTER SWAP" << std::endl;
        std::cout << counteri->hash << " " << counteri->item << " " << counteri->count << std::endl; 
        std::cout << counterj->hash << " " << counterj->item << " " << counterj->count << std::endl; 
        
        // put j back into the hashtable
        if (largePassiveHashtable[counterj->hash]) {
            largePassiveHashtable[counterj->hash]->prev = counterj;
        }
        largePassiveHashtable[counterj->hash] = counterj;
        counterj->prev = NULL;
        return;        
    }
}

/**
 * Adds an item to a specified location in our hash table. Can be executed
 * when find_item or add_item is still running.
 * Only adds the item to the active table.
 */
/*
void DIMSUMpp::add_item_to_location(DIMitem_t item, DIMweight_t value, DIMCounter** location) {
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

void DIMSUMpp::update(DIMitem_t item, DIMweight_t value) {
}
*/
/*
void DIMSUMpp::do_some_moving() {
}

void DIMSUMpp::do_some_clearing() {
    int updatesLeft = activeSize - nActive;
    assert(movedFromPassive == nSmallPassive);
	assert(left2move == 0);
	assert(updatesLeft >= 0);
}
*/
/*************************************************************************
 * INTERNAL QUERYING 
 *************************************************************************/
DIMCounter* DIMSUMpp::find_item(DIMitem_t item) {
	DIMCounter* hashptr;
	hashptr = find_item_in_active(item);
	if (!hashptr) {
		hashptr = find_item_in_passive(item);
	}
	return hashptr;
}

DIMCounter* DIMSUMpp::find_item_in_active(DIMitem_t item) {
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

DIMCounter* DIMSUMpp::find_item_in_passive(DIMitem_t item) {
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
 * QUERYING 
 *************************************************************************/
/**
 * Returns a list of items and counts that are greater than a certain threshold.
 */
std::map<uint32_t, uint32_t> DIMSUMpp::output(uint64_t thresh) {
    std::map<uint32_t, uint32_t> res;

    for (int i = 0; i < nActive; i++) {
        if (activeCounters[i].count >= thresh) {
            res.insert(std::pair<uint32_t, uint32_t>(activeCounters[i].item, activeCounters[i].count));
        }
    }
    for (int i = 0; i < nLargePassive; i++) {
        // See if something is in the passive table and not in the active.
        if (find_item_in_active(largePassiveCounters[i].item) == NULL
            && largePassiveCounters[i].count >= thresh) {
            res.insert(std::pair<uint32_t, uint32_t>(
				largePassiveCounters[i].item, largePassiveCounters[i].count));
        }
    }
    // TODO: Have to scan through the small passive table
    return res;
}

DIMweight_t DIMSUMpp::point_err() {
    return quantile;
}

DIMweight_t DIMSUMpp::point_est(DIMitem_t item) {
    DIMCounter* a;
    a = find_item(item);
    return a ? a->count : quantile;
}

/**
 * Recursively finds the kth using quintets. Modifies the array in-place such
 * that all of the elements in the kth index or below will be less than the
 * kth index. Also returns the value of the kth index.
 *  @param v: pointer to the c style array
 *  @param n: size of the input array
 *  @param k: index of which to guarantee things will be less than
 *  @param jump: internal jump paramter
 *  @param pivot: default pivot value
 *
 * @returns: the value of the kth element.
 */
int DIMSUMpp::in_place_find_kth(int* v, int n, int k, int jump = 1, int pivot = 0) {
	assert(k < n);
	if ((n == 1) && (k == 0)) return v[0];
	else if (n == 2) {
		return (v[k*jump] < v[(1 - k)*jump]) ? v[k*jump] : v[(1 - k)*jump];
	}
	if (pivot == 0) {
		int m = (n + 4) / 5; // number of medians
								//allocate space for medians.
		for (int i = 0; i < m; i++) {
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
		pivot = in_place_find_kth(v + 2*jump, (n+2)/5, (n + 2) / 5 / 2, jump * 5);
	}
	// put smaller items in the beginning
	int store = 0;
	for (int i = 0; i < n; i++) {
		if (v[i*jump] < pivot) {
			std::swap(v[i*jump], v[store*jump]);
			store++;
		}
	}
	// put pivots next
	int store2 = store;
	for (int i = store; i < n; i++) {
		if (v[i*jump] == pivot) {
			std::swap(v[i*jump], v[store2*jump]);
			store2++;
		}
	}
	// Then put the pivot
	// if k is small, search for it in the beginning.
	if (store > k) {
		return in_place_find_kth(v, store, k, jump);
	}
	// if k is large, search for it at the end.
	else if (k >= store2){
		return in_place_find_kth(v + store2*jump, n - store2, k - store2, jump);
	}
	else {
		return pivot;
	}
}


/*************************************************************************
 * DEBUGGING (I enjoy debugging in a very deep level.)
 *************************************************************************/
void DIMSUMpp::show_large_passive_table() {
    for (int i = 0; i < largePassiveSize; i++) {
        std::cout << "|" << largePassiveCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUMpp::show_small_passive_table() {
    for (int i = 0; i < smallPassiveSize; i++) {
        std::cout << "|" << smallPassiveCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUMpp::show_passive_table() {
    std::cout << "LARGE PASSIVE TABLE" << std::endl;
    show_large_passive_table();
    std::cout << "SMALL PASSIVE TABLE" << std::endl;
    show_small_passive_table();
    std::cout << std::endl;
}

void DIMSUMpp::show_active_table() {
    for (int i = 0; i < activeSize; i++) {
        std::cout << "|" << activeCounters[i].count;
    }
    std::cout << "|" << std::endl;
}

void DIMSUMpp::show_table() {
    std::cout << std::endl;
    std::cout << "LARGE PASSIVE TABLE" << std::endl;
    show_large_passive_table();
    std::cout << "SMALL PASSIVE TABLE" << std::endl;
    show_small_passive_table();
    std::cout << "ACTIVE TABLE" << std::endl;
    show_active_table();
    std::cout << std::endl;
}

void DIMSUMpp::show_hash() {
    int i;
	DIMCounter* hashptr;

    std::cout << "SMALL PASSIVE HASH TABLE" << std::endl;
	for (i = 0; i< smallPassiveHashSize; i++)
	{
		printf("%d:", i);
		hashptr = smallPassiveHashtable[i];
		while (hashptr) {
			std::cout << " " << (size_t) hashptr << " [h(" 
            << (unsigned int)hashptr->item << ") = ?, prev = ?] ---> ";
			hashptr = hashptr->next;
		}
		printf(" *** \n");
	}
    std::cout << std::endl;

    std::cout << "LARGE PASSIVE HASH TABLE" << std::endl;
    for (i = 0; i< largePassiveHashSize; i++)
	{
		printf("%d:", i);
		hashptr = largePassiveHashtable[i];
		while (hashptr) {
			std::cout << " " << (size_t) hashptr << " [h(" 
            << (unsigned int)hashptr->item << ") = ?, prev = ?] ---> ";
			hashptr = hashptr->next;
		}
		printf(" *** \n");
	}
    std::cout << std::endl;

    std::cout << "ACTIVE HASH TABLE" << std::endl;
    for (i = 0; i< activeHashSize; i++)
	{
		printf("%d:", i);
		hashptr = activeHashtable[i];
		while (hashptr) {
			std::cout << " " << (size_t) hashptr << " [h(" 
            << (unsigned int)hashptr->item << ") = ?, prev = ?] ---> ";
			hashptr = hashptr->next;
		}
		printf(" *** \n");
	}
    std::cout << std::endl;
}

/*************************************************************************
 * Helper Allocation and Deallocation functions 
 *************************************************************************/
void DIMSUMpp::init_passive() {
    // Allocate the large hash table. 
    largePassiveHashSize = DIM_HASHMULT * largePassiveSize;
    largePassiveCounters = (DIMCounter *) calloc(largePassiveSize, sizeof(DIMCounter));
    largePassiveHashtable = (DIMCounter**) calloc(largePassiveHashSize, sizeof(DIMCounter*));
    for (int i = 0; i < largePassiveHashSize; i++) {
        largePassiveHashtable[i] = NULL;
    }
    for (int i = 0; i < largePassiveSize; i++) {
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
    for (int i = 0; i < smallPassiveSize; i++) {
		smallPassiveCounters[i].next = NULL;
		smallPassiveCounters[i].prev = NULL;
		smallPassiveCounters[i].item = DIM_NULLITEM;
	}
	nSmallPassive = 0;
}

void DIMSUMpp::destroy_passive() {
    free(smallPassiveHashtable);
    free(smallPassiveCounters);
    free(largePassiveHashtable);
    free(largePassiveCounters);
}

void DIMSUMpp::init_active() {
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

void DIMSUMpp::destroy_active() {
    free(activeCounters);
    free(activeHashtable);
}
