cmake_minimum_required(VERSION 3.10)
project(TaxiRide_Windflow)
include_directories (includes $ENV{HOME}/fastflow)
set(CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_FLAGS "-DFF_BOUNDED_BUFFER  -g -O3 -finline-functions -pthread")
add_executable(TaxiRide_Windflow src/TotalArrivalCount.cpp)
add_executable(SlidingArrivalCount_Windflow src/SlidingArrivalCount.cpp)