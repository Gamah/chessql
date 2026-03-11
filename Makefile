CXX      = g++
CXXFLAGS = -I/usr/include/postgresql -O3 -std=c++17 -Wall -Wextra -pthread
LDFLAGS  = -lpq -lzstd -lpthread

TARGET   = lichess_import
SRCS     = lichess_import.cpp zobrist.cpp
HEADERS  = board.h pgn_parser.h zobrist.h

all: $(TARGET)

$(TARGET): $(SRCS) $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $@ $(SRCS) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
