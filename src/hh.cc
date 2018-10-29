/********************************************************************
This code is based on Approximate frequent items in a data stream from
the work of G. Cormode 2002, 2003,2005.

Handles the evaluation of different 
*********************************************************************/
#define VERBOSE_STATS true
#define VERBOSE_EXACT false

#include "countmin.h"  // naive count min sketch

// #include "losum.h"
#include "alosum.h"
#include <fstream>
#include <chrono>
#include <thread>
#include <sys/time.h>
#include <cstring>


using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;


/******************************************************************/
class Stats {
public:
	Stats() : dU(0.0), dQ(0.0), dP(0.0), dR(0.0), dF(0.0), dF2(0.0) {}

	double dU, dQ;
	double dP, dR, dF, dF2;
	std::multiset<double> P, R, F, F2;
};

void usage() {
	std::cerr
		<< "Usage: graham"                   << std::endl
		<< "\t-np		number of packets"   << std::endl
		<< "\t-r		number of runs"      << std::endl
		<< "\t-phi		phi"                 << std::endl
		<< "\t-d		depth"               << std::endl
		<< "\t-g		granularity"         << std::endl
		<< "\t-gamma    DIM-SUM coefficient" << std::endl
		<< "\t-z        skew"                << std::endl
		<< std::endl;
}


/**
 * Stops the timer and returns the time elapsed in milliseconds.
 * 		start = Clock::now();
 *      f();
 * 		uint64_t elapsed = StopTheClock(start);
 */
uint64_t StopTheClock(time_point<Clock> &start) {
	auto end = Clock::now();
    milliseconds diff = duration_cast<milliseconds>(end - start);
    return static_cast<uint64_t>(diff.count());
}

/**
 * Calculates statitics for our heavy hitter algorithms, compared to the
 * actual actual values (since our algorithms overestimate)
 */
void CheckOutput(std::map<uint32_t, uint32_t>& res, uint64_t thresh, size_t hh,
				 Stats& S, const std::vector<uint32_t>& exact) {
	/*
	std::cout << "Exact heavy hitter ids" << std::endl;
	for (auto hitter : exact) {
		std::cout << hitter << " ";
	}*/
	if (res.empty()) {
		S.F.insert(0.0);
		S.F2.insert(0.0);
		S.P.insert(100.0);
		S.dP += 100.0;

		if (hh == 0) {
			S.R.insert(100.0);
			S.dR += 100.0;
		} else {
			S.R.insert(0.0);
		}
		return;
	}

	size_t correct = 0;
	size_t claimed = res.size();
	size_t falsepositives = 0;
	double e = 0.0, e2 = 0.0;

	std::map<uint32_t, uint32_t>::iterator it;
	for (it = res.begin(); it != res.end(); ++it) {
		if (exact[it->first] >= thresh) {
			++correct;
			uint32_t ex = exact[it->first];
			double diff = (ex > it->second) ? ex - it->second : it->second - ex;
			e += diff / ex;
		}
		else {
			++falsepositives;
			uint32_t ex = exact[it->first];
			double diff = (ex > it->second) ? ex - it->second : it->second - ex;
			e2 += diff / ex;
		}
	}

	if (correct != 0) {
		e /= correct;
		S.F.insert(e);
		S.dF += e;
	}
	else {
		S.F.insert(0.0);
	}

	if (falsepositives != 0) {
		e2 /= falsepositives;
		S.F2.insert(e2);
		S.dF2 += e2;
	} else {
		S.F2.insert(0.0);
	}

	double r = 100.0;
	if (hh != 0) r = 100.0 * ((double) correct) / ((double) hh);
	double p = 100.0 * ((double) correct) / ((double) claimed);

	S.R.insert(r);
	S.dR += r;
	S.P.insert(p);
	S.dP += p;
}

/**
 * Pretty prints the times of each iteration in our algorithm.
 */
void PrintTimes(std::string title, std::vector<uint64_t> times) {
	std::cout << title;
	for (auto const& t : times) {
		std::cout << "\t" << t;
	}
	std::cout << std::endl;
}


/**
 * Pretty prints our statistics class.
 */
void PrintOutput(std::string title, size_t size, const Stats& S, size_t u32NumberOfPackets) {
	double p5th = -1.0, p95th = -1.0, r5th = -1.0, r95th = -1.0, f5th = -1.0, f95th = -1.0, f25th = -1.0, f295th = -1.0;
	size_t i5, i95;
	std::multiset<double>::const_iterator it;

	if (! S.P.empty()) {
		it = S.P.begin();
		i5 = (size_t) (S.P.size() * 0.05);
		for (size_t i = 0; i < i5; ++i) ++it;
		p5th = *it;
		i95 = (size_t) (S.P.size() * 0.95);
		for (size_t i = 0; i < (i95 - i5); ++i) ++it;
		p95th = *it;
	}

	if (! S.R.empty()) {
		it = S.R.begin();
		i5 = S.R.size() * 0.05;
		for (size_t i = 0; i < i5; ++i) ++it;
		r5th = *it;
		i95 = S.R.size() * 0.95;
		for (size_t i = 0; i < (i95 - i5); ++i) ++it;
		r95th = *it;
	}

	if (! S.F.empty()) {
		it = S.F.begin();
		i5 = S.F.size() * 0.05;
		for (size_t i = 0; i < i5; ++i) ++it;
		f5th = *it;
		i95 = S.F.size() * 0.95;
		for (size_t i = 0; i < (i95 - i5); ++i) ++it;
		f95th = *it;
	}

	if (! S.F2.empty()) {
		it = S.F2.begin();
		i5 = S.F2.size() * 0.05;
		for (size_t i = 0; i < i5; ++i) ++it;
		f25th = *it;
		i95 = S.F2.size() * 0.95;
		for (size_t i = 0; i < (i95 - i5); ++i) ++it;
		f295th = *it;
	}
	
	if (S.dU <= 0) {
		printf("Error! Total update time %f not positive\n", S.dU);
	}
	
	printf("%s\t%1.2f\t%zd\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\n",
		title.c_str(), u32NumberOfPackets / S.dU, size,
		(S.R.size() > 0) ? S.dR / S.R.size():0, r5th, r95th,
		(S.P.size() > 0) ? S.dP / S.P.size():0, p5th, p95th,
		(S.F.size() > 0) ? S.dF / S.F.size():0, f5th, f95th,
		(S.F2.size()> 0) ? S.dF2 / S.F2.size():0, f25th, f295th
	);
}

/**
 * Uses the slow algorithm to find which streams have above a certain threshold.
 */
size_t RunExact(uint64_t thresh, std::vector<uint32_t>& exact) {
	size_t hh = 0;
	for (size_t i = 0; i < exact.size(); ++i) {
		if (exact[i] >= thresh) ++hh;
	}
	return hh;
}

/******************************************************************/

int main(int argc, char **argv) {
	// algorithm and data default parameters
	size_t stNumberOfPackets = 10000000;
	size_t stRuns = 20;
	double dPhi = 0.001; //0.000001; //0.001;
	double gamma = 2.0;
	bool gammaDefined = false;
	uint32_t u32Depth = 10;
	uint32_t u32Granularity = 8;
	std::string file = "../trace/nyc.dmp";
	bool timeLaspe = false;
	double dSkew = 1.0;

	// timing
	uint64_t t;
	auto start = Clock::now();

	for (int i = 1; i < argc; ++i) {
		if (strcmp(argv[i], "-np") == 0)
		{
			i++;
			if (i >= argc) {
				std::cerr << "Missing number of packets." << std::endl;
				return -1;
			}
			stNumberOfPackets = atoi(argv[i]);
		}
		else if (strcmp(argv[i], "-r") == 0)
		{
			i++;
			if (i >= argc) {
				std::cerr << "Missing number of runs." << std::endl;
				return -1;
			}
			stRuns = atoi(argv[i]);
		}
		else if (strcmp(argv[i], "-d") == 0)
		{
			i++;
			if (i >= argc) {
				std::cerr << "Missing depth." << std::endl;
				return -1;
			}
			u32Depth = atoi(argv[i]);
		}
		else if (strcmp(argv[i], "-g") == 0) {
			i++;
			if (i >= argc)
			{
				std::cerr << "Missing granularity." << std::endl;
				return -1;
			}
			u32Granularity = atoi(argv[i]);
		}
		else if (strcmp(argv[i], "-phi") == 0)
		{
			i++;
			if (i >= argc)
			{
				std::cerr << "Missing phi." << std::endl;
				return -1;
			}
			dPhi = atof(argv[i]);
		}
		else if (strcmp(argv[i], "-f") == 0)
		{
			i++;
			if (i >= argc) {
				std::cerr << "Missing file name." << std::endl;
				return -1;
			}
			file = std::string(argv[i]);
		}
		else if (strcmp(argv[i], "-t") == 0) {
			timeLaspe = true;
		}
		else if (strcmp(argv[i], "-gamma") == 0) {
			i++;
			if (i >= argc)
			{
				std::cerr << "Missing gamma." << std::endl;
				return -1;
			}
			gamma = atof(argv[i]);
			gammaDefined = true;

		} else if (strcmp(argv[i], "-z") == 0)
		{
			i++;
			if (i >= argc) {
				std::cerr << "Missing skew parameter." << std::endl;
				return -1;
			}
			dSkew = atof(argv[i]);
		}
		else if (strcmp(argv[i], "-measure_time_granularity") == 0) {
			uint64_t start_time = 0;
			auto start = Clock::now();
			while (start_time == 0) {
				t = StopTheClock(start);
			}
			std::cout << "Time granularity is " << t << " ms" << std::endl;
		} else {
			usage();
			return -1;
		}
	}

	uint32_t u32Width = 2.0 / dPhi;

	// We fix PRNG to a specific seed for reproducibility.
	prng_type* prng;
	prng=prng_Init(44545, 2);
	int64_t a = (int64_t) (prng_int(prng) % MOD);
	int64_t b = (int64_t) (prng_int(prng) % MOD);
	prng_Destroy(prng);

	uint32_t u32DomainSize = 1048575;
	std::vector<uint32_t> exact(u32DomainSize + 1, 0);
	Stats SLS, SCM ,SCMH, SCCFC, SALS, SLCL;
	std::vector<uint64_t> TLS, TCM, TCMH, TCCFC, TALS, TLCL;

	/***************************************************************************
	 * DATA LOADING - preload all data to remove IO element from algorithm. 
	 **************************************************************************/
	std::vector<uint32_t> data;
	std::vector<uint32_t> values;
	// Read in trace file
	size_t stCount = 0;
	if (file != "") {
		uint64_t total = 0;
		std::cout << "Using file: " << file << std::endl;
		std::ifstream f;
		f.open(file);
		if (!f) {
			std::cout << "Unable to load file" << std::endl;
			exit(1);
		}
		uint32_t id, length;
		while (f >> id >> length) {
			// std::cout << id << " " << length << std::endl;
			if (length <= 0) continue; // Packets should not be empty!
			if (total < 0) {
				std::cerr << "Why is total negative? " << total<<std::endl;
				break;
			}
			if ((total + length) >= 0x7FFFFFFE) {
				std::cerr <<  "Error! total number of bytes is " << total << " and trying to add " << length << std::endl;
				break;
			}
			data.push_back(id);
			values.push_back(length);
			total += length;
		}
		std::cerr << "Finished loading file. Total number of bytes: " << total << std::endl;
	}
	else {
		Tools::Random r = Tools::Random(0xF4A54B);
		Tools::PRGZipf zipf = Tools::PRGZipf(0, u32DomainSize, dSkew, &r);
		for (int i = 0; i < stNumberOfPackets; ++i)
		{
			++stCount;
			if (stCount % 500000 == 0)
				std::cerr << stCount << std::endl;
			uint32_t v = zipf.nextLong();
			uint32_t value = hash31(a, b, v) & u32DomainSize;
			if (value > 0) {
				data.push_back(value);
				values.push_back(1);
			}
			else {
				data.push_back(-value);
				values.push_back(1);
			}
		}
	}

	/***************************************************************************
	 * ALGORITHM INITIALIZATION
	 **************************************************************************/
	ALS_type* als = ALS_Init(dPhi, gamma);
	CM_type* cm = CM_Init(u32Width, u32Depth, 0);
	
	// Number of runs to complete one pass through our trace. 
	const size_t MAX_TRACE_SIZE = 1000000000;
	size_t experimentSize = data.size() > MAX_TRACE_SIZE ? MAX_TRACE_SIZE : data.size();
	size_t stRunSize = experimentSize / stRuns;
	if (VERBOSE_EXACT) {
		std::cout << "Total Number of Packets in Trace: " << data.size() << std::endl;
		std::cout << "Number of packets in each run: " << stRunSize << std::endl;
	}
	size_t stStreamPos = 0;
	long long total = 0;

	for (size_t run = 1; run <= stRuns; ++run) {

		bool stop = false;
		for (size_t i = stStreamPos; i < stStreamPos + stRunSize; ++i)
		{
			assert(values[i] > 0);
			total += abs((int)values[i]);
			if (total >= 0x7FFFFFFF) {
				std::cerr << "Error! Total number of bytes is " << total << std::endl;
				stop = true;
				break;
			}
			exact[data[i]] += values[i];
			if (exact[data[i]] > 0x7FFFFFFF) {
				std::cerr << "Strange. Value is too large " <<exact[data[i]]<< " after addding "<<values[i]<< std::endl;
			}
		}
		if (stop) break;

		start = Clock::now();
		for (size_t i = stStreamPos; i < stStreamPos + stRunSize; ++i) {
			ALS_Update(als, data[i], values[i]);
		}
		SALS.dU += t = StopTheClock(start);
		TALS.push_back(t);
		
		start = Clock::now();
		for (size_t i = stStreamPos; i < stStreamPos + stRunSize; ++i) {
			CM_Update(cm, data[i], values[i]);
		}
		SCM.dU += t = StopTheClock(start);
		TCM.push_back(t);

		uint64_t thresh = static_cast<uint64_t>(floor(dPhi * total)+1);//floor(dPhi * run * stRunSize));
		if (VERBOSE_EXACT) std::cerr << "total " << total << " thresh " << thresh << std::endl;
		size_t hh = RunExact(thresh, exact);
		if (VERBOSE_EXACT) std::cerr << "Run: " << run << ", Exact: " << hh << std::endl;

		std::map<uint32_t, uint32_t> res;
		
		start = Clock::now();
		res = ALS_Output(als, thresh);
		SLS.dQ += StopTheClock(start);
		CheckOutput(res, thresh, hh, SALS, exact);
		
		stStreamPos += stRunSize;
	} 

	printf("\nMethod\tUpdates/ms\tSpace\tRecall\t5th\t95th\tPrecis\t5th\t95th\tFreq RE\t5th\t95th\n");
	stNumberOfPackets = data.size();
	PrintOutput("ALS", ALS_Size(als), SALS, stNumberOfPackets);
	PrintOutput("CM", CM_Size(cm), SCM, stNumberOfPackets);

	ALS_Destroy(als);
	CM_Destroy(cm);

	std::cout << std::endl;
	return 0;
}
