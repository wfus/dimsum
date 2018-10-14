CXXFLAGS=-O2 -std=c++11 -stdlib=libc++
CXX=clang++

# PCAP=-L../lib/winpcap/Lib -llibwpcap -I../lib/winpcap/Include
OBJECTS=rand48.o prng.o alosum.o

all: $(OBJECTS)
	$(CXX) $(CXXFLAGS) hh.cc countmin.cc $(OBJECTS) -o main.o 

$(OBJECTS): rand48.h prng.h alosum.h countmin.h
	$(CXX) $(CXXFLAGS) -c $*.cc
	
clean:
	rm -rf *.o 
