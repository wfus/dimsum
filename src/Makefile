CXXFLAGS=-O2 -DNDEBUG -m32
CXX=g++

PCAP=-L../lib/winpcap/Lib -llibwpcap -I../lib/winpcap/Include
OBJECTS=rand48.o prng.o lossycount.o countmin.o ccfc.o 

all: $(OBJECTS)
	$(CXX) $(CXXFLAGS) hh.cc $(OBJECTS) -o Release/hh-zipf
	$(CXX) $(CXXFLAGS) hh.cc $(OBJECTS) -o Release/hh-pcap -DPCAP $(PCAP)

$(OBJECTS): rand48.h prng.h lossycount.h countmin.h ccfc.h
	$(CXX) $(CXXFLAGS) -c $*.cc

clean:
	rm -rf *.o Release/hh-zipf Release/hh-zipf.exe Release/hh-pcap Release/hh-pcap.exe
