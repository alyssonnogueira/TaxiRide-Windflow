# Author: Alysson Nogeura <alysson.rodrigues@edu.pucrs.br>
# Date:   December 2019

FF_ROOT			= $(HOME)/fastflow
OUT_DIR			= ../bin/
INCLUDE_DIR		= ../includes

CXX 			= g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
MACROS 			= -DFF_BOUNDED_BUFFER
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

SRCS			:= $(wildcard *.cpp)
TGTS			:= $(basename $(SRCS))

all: $(TGTS)

%: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o $(OUT_DIR)/$@ $< $(LDFLAGS)

clean:
	rm -rf $(OUT_DIR)

.DEFAULT_GOAL := all
.PHONY: all clean
