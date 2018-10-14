CXXFLAGS=-O2 -stdlib=libstdc++ -std=c++11
CXX=g++

# PCAP=-L../lib/winpcap/Lib -llibwpcap -I../lib/winpcap/Include
OBJECTS=rand48.o prng.o alosum.o

all: $(OBJECTS)
	$(CXX) $(CXXFLAGS) hh.cc $(OBJECTS) -o main.o 
	# $(CXX) $(CXXFLAGS) hh.cc $(OBJECTS) -o Release/hh-pcap -DPCAP $(PCAP)

$(OBJECTS): rand48.h prng.h alosum.h
	$(CXX) $(CXXFLAGS) -c $*.cc

clean:
	rm -rf *.o Release/hh-zipf Release/hh-zipf.exe Release/hh-pcap Release/hh-pcap.exe
