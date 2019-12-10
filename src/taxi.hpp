//
// Created by alyss on 09/12/2019.
//

#ifndef TAXIRIDE_WINDFLOW_TAXI_HPP
#define TAXIRIDE_WINDFLOW_TAXI_HPP

#include <iostream>
#include <cmath>
#include <string>
#include <fstream>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <time.h>
#include <getopt.h>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include "tuplas.hpp"
#include "operators.hpp"

using namespace std;
using namespace wf;

atomic<int> sink_zero_processed;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"nsource", REQUIRED, 0, 's'},      // pipe start (source) parallelism degree
        {"nsplitter", REQUIRED, 0, 'p'},    // splitter parallelism degree
        {"ncounter", REQUIRED, 0, 'c'},     // word counter parallelism degree
        {"nsink", REQUIRED, 0, 'e'},        // pipe end (sink) parallelism degree
        {"pardeg", REQUIRED, 0, 'n'},       // parallelism degree for all nodes
        {"rate", REQUIRED, 0, 'r'},         // stream generation rate
        {0, 0, 0, 0}
};

#endif //TAXIRIDE_WINDFLOW_TAXI_HPP
