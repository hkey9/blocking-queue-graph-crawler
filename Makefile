CXXFLAGS=-I ~/rapidjson/include
LDFLAGS=-lcurl -pthread
LD=g++
CC=g++

all: client

client: client.o
	$(LD) $< -o $@ $(LDFLAGS)

client.o: client.cpp
	$(CC) $(CXXFLAGS) -c client.cpp -o client.o

clean:
	-rm client client.o