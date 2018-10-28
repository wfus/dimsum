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
	// nitems in large table
	nActive = 0;
	countersize = int(ceil(gamma / fPhi) + ceil(1 / fPhi) - 1);
	maxMaintenanceTime = int(ceil(gamma / fPhi));
	hashsize = ALS_HASHMULT * countersize;
	
    // hard coded constants for the hash table,
	// should really generate these randomly
    hasha = 151261303;
	hashb = 6722461; 
    n = (ALSweight_t) 0;

	// Allocate the pointers for the active hashtable and initialize
	activeHashtable = (ALSCounter **)calloc(hashsize, sizeof(ALSCounter*));
	for (int i = 0; i < hashsize; i++) {
		activeHashtable[i] = NULL;
	}
	// initialize all active counters to have no links and no item
	activeCounters = (ALSCounter*)calloc(countersize, sizeof(ALSCounter));
	for (i = 0; i < countersize; i++) {
		activeCounters[i].next = NULL;
		activeCounters[i].prev = NULL;
		activeCounters[i].item = ALS_NULLITEM;
	}

	// initialize the passive array
	init_passive();
	// number of free spaces is initialized to entire size of hashtable
	extra = countersize;
	quantile = 0;
	buffer = (int*) calloc(countersize, sizeof(int));
	handle = NULL;
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

void ALS::destroy_passive() {
    free(passiveHashtable);
    free(passiveCounters);
}

/**
 * Returns the size of ALL datastructures used, including on heap.
 */
int ALS::size() {
    return sizeof(ALS) + countersize * sizeof(int)    // size of median buffer
		+ 2 * (hashsize * sizeof(ALSCounter*))        // two hash tables
		+ 2 * (countersize * sizeof(ALSCounter));     // two counter arrays
};

/**
 * Estimate the count for a specific id. Returns the error term
 * if we cannot find it in our hashtables.
 */
int ALS::point_est(ALSitem_t item) {
	ALSCounter* i;
	i = find_item(item);
	return i ? i->count : quantile;
	// if we couldn't find the item, we provide an overestimate
	// with our error 'quantile'
};

/**
 * Estimate the worst case error in our threshold estimate
 */
int ALS::point_err() {
	return quantile;
};

/**
 * Returns the Elephant Flows - All flows that have been estimated to
 * be greater than value "thresh".
 */
std::map<uint32_t, uint32_t> ALS::output(uint64_t thresh) {
	std::map<uint32_t, uint32_t> res;
    // Iterate through active block. Then, we iterate through 
	for (int i = 0; i < nActive; ++i) {
		if (activeCounters[i].count >= static_cast<int>(thresh))
			res.insert(std::pair<uint32_t, uint32_t>(activeCounters[i].item, activeCounters[i].count));
	}
	// If we see it in passive, maybe it was dupliced and is also in active
	for (int i = 0; i < nPassive; ++i) {
		if (find_item_in_active(passiveCounters[i].item) == NULL) {
			if (passiveCounters[i].count >= static_cast<int>(thresh)) {
				res.insert(std::pair<uint32_t, uint32_t>(passiveCounters[i].item, passiveCounters[i].count));
			}
		}
	}
	return res;
};


/**
 * Recursively finds the kth using quintets
 */
int ALS::in_place_find_kth(int* v, int n, int k, int jump = 1, int pivot = 0) {
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
				swap(w[j0*jump], w[jmin*jump]);
			}
		}
		pivot = in_place_find_kth(v + 2*jump, (n+2)/5, (n + 2) / 5 / 2, jump * 5);
	}
	// put smaller items in the beginning
	int store = 0;
	for (int i = 0; i < n; i++) {
		if (v[i*jump] < pivot) {
			swap(v[i*jump], v[store*jump]);
			store++;
		}
	}
	// put pivots next
	int store2 = store;
	for (int i = store; i < n; i++) {
		if (v[i*jump] == pivot) {
			swap(v[i*jump], v[store2*jump]);
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

/*
uint32_t ALS_Maintenance(void* lpParam) {
	// FINISH MAINTENANCE	
	// dnd quantile
	ALS_type* ALS = (ALS_type*) lpParam;
	int k = ALS->nPassive - ceil(1 / ALS->epsilon)+1;
	if (k >= 0) {
		for (int i = 0; i < ALS->nPassive; ++i) {
			ALS->buffer[i] = ALS->passiveCounters[i].count;
		}
		int median = ALS_in_place_find_kth(ALS->buffer, ALS->nPassive, k, 1, ALS->quantile+1);
		int test = 0;
		if (median > ALS->quantile) {
			ALS->quantile = median;
		}
	}
	// Copy passive to active
	movedFromPassive = 0;
	for (int i = 0; i < nPassive; i++) {
		if (passiveCounters[i].count > quantile) {
			ALSCounter* c = find_item_in_active(passiveCounters[i].item);
			if (!c) {
				++movedFromPassive;
				add_item(passiveCounters[i].item, passiveCounters[i].count);
			}
			else {
				// counter was already moved. We earned an extra addition.
				++extra;
			}
		}
	}
	destroy_passive();
	init_passive();
	extra += maxMaintenanceTime;
	return 0;
};*/

/*************************************************************************
 * QUERYING 
 *************************************************************************/

/**
 * Returns NULL if not found, pointer to counter if is found.
 */
ALSCounter* ALS::find_item(ALSitem_t item) {
	ALSCounter* hashptr;
	hashptr = find_item_in_active(item);
	if (!hashptr) {
		hashptr = find_item_in_passive(item);
	}
	return hashptr;
}

ALSCounter* ALS::find_item_in_active(ALSitem_t item) {
	ALSCounter* hashptr;
	int hashval;
	hashval = static_cast<int>(hash31(hasha, hashb, item) % hashsize);
	hashptr = activeHashtable[hashval];
	// Continue to look for the item through the LL in the passive Hashtable
	while (hashptr) {
		if (hashptr->item == item) break;
		else hashptr = hashptr->next;
	}
	return hashptr;
}

ALSCounter* ALS::find_item_in_passive(ALSitem_t item) {
	ALSCounter* hashptr;
	int hashval;
	hashval = static_cast<int>(hash31(hasha, hashb, item) % hashsize);
	hashptr = passiveHashtable[hashval];

	// Continue to look for the item through the LL in the passive Hashtable
	while (hashptr) {
		if (hashptr->item == item) break;
		else hashptr = hashptr->next;
	}
	return hashptr;
}

void ALS::add_item(ALSitem_t item, ALSweight_t value) {
	int hashval = static_cast<int>(hash31(hasha, hashb, item) % hashsize);
	ALSCounter* hashptr = activeHashtable[hashval];

	// Coherence checks - we need to make sure we always have room to
	// put in more network flows.
	if (nActive >= countersize) {
		std::cerr << "Error! Not enough room in table." << std::endl;
		std::cerr << "Size:"<< countersize << " Active: " << nActive << " Passive:" << nPassive 
			<< " Extra:" << extra << " From passive:" << movedFromPassive 
			<< std::endl;
	}
	assert(nActive < countersize);
	// put the new item into the hashtable at the beginning.
	ALSCounter* counter = &(activeCounters[nActive++]);
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


/*************************************************************************
 * DEBUGGING
 *************************************************************************/
/**
 * Shows the hashtable for debugging purposes
 */
void ALS::show_hash() {
	int i;
	ALSCounter* hashptr;

	for (i = 0; i < hashsize; i++)
	{
		printf("%d:", i);
		hashptr = activeHashtable[i];
		while (hashptr) {
			printf(" %p [h(%u) = %d, prev = %p] ---> ", hashptr,
				(unsigned int)hashptr->item,
				hashptr->hash,
				hashptr->prev);
			hashptr = hashptr->next;
		}
		printf(" *** \n");
	}
}

/**
 * Shows the heap for debugging purposes
 */
void ALS::show_heap() {
    int i;
    int j = 1;
    for (i = 1; i <= countersize; i++) {
		std::cout << (int) activeCounters[i].count;
		if (i == j) {
            std::cout << std::endl;
			j = 2 * j + 1;
		}
	}
    std::cout << std::endl << std::endl;
}

/**
 * Validates the hash table to make sure everything is correct
 */
void ALS::check_hash(int item, int hash) {
	int i;
	ALSCounter *hashptr, *prev;

	for (i = 0; i < hashsize; i++) {
		prev = NULL;
		hashptr = activeHashtable[i];
		while (hashptr) {
			if (hashptr->hash != i) {
				printf("\n Hash violation! hash = %d, should be %d \n",
					hashptr->hash, i);
				printf("after inserting item %d with hash %d\n", item, hash);
			}
			if (hashptr->prev != prev) {
				printf("\n Previous violation! prev = %p, should be %p\n", hashptr->prev, prev);
				printf("after inserting item %d with hash %d\n", item, hash);
				exit(1);
			}
			prev = hashptr;
			hashptr = hashptr->next;
		}
	}
};
