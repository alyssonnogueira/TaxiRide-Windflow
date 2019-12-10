//
// Created by alyss on 09/12/2019.
//

#ifndef TAXIRIDE_WINDFLOW_OPERATORS_HPP
#define TAXIRIDE_WINDFLOW_OPERATORS_HPP

#include <fstream>
#include <vector>
#include <ff/ff.hpp>
#include "NycGeoUtils.hpp"

using namespace std;
using namespace wf;

atomic<long> total_rides;
unsigned long app_run_time = 60 * 1000000L; // 60 seconds

/**
 *  @class Source_Functor
 *
 *  @brief Define the logic of the Source
 */
class Source_Functor {
private:
    vector<string> rideLines;         // contains all the tuples
    int rate;                       // stream generation rate
    size_t next_tuple_idx;          // index of the next tuple to be sent
    int generations;                // counts the times the file is generated
    long generated_tuples;          // tuples counter

    // time variables
    unsigned long app_start_time;   // application start time
    unsigned long current_time;
    unsigned long interval;

    /**
     *  @brief Add some active delay (busy-waiting function)
     *
     *  @param waste_time wait time in microseconds
     */
    void active_delay(unsigned long waste_time) {
        auto start_time = current_time_usecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_usecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    /**
     *  @brief Constructor.
     *
     *  @param _dataset all the tuples that will compose the stream
     *  @param _rate stream generation rate
     *  @param _app_start_time application starting time
     */
    Source_Functor(const vector<string>& _rideLines,
                   const int _rate,
                   const unsigned long _app_start_time):
            rate(_rate),
            app_start_time(_app_start_time),
            current_time(_app_start_time),
            next_tuple_idx(0),
            generations(0),
            generated_tuples(0){
        rideLines = _rideLines;
        interval = 1000000L; // 1 second (microseconds)
    }

    /**
     *  @brief Send tuples in a item-by-item fashion
     *
     *  @param t reference to the tuple structure
     *  @return true if the stream is not ended, false if the EOS has been reached
     */
    bool operator()(TaxiRide& ride) {
        if (generated_tuples > 0) current_time = current_time_usecs();
        if (next_tuple_idx == 0) generations++;         // file generations counter
        generated_tuples++;                             // tuples counter

        ride = TaxiRide::fromString(rideLines[next_tuple_idx]);

//        cout << "[Source] tuple content: " << ride.toString() << endl;

        if (rate != -1) // stream generation rate is fixed
            active_delay(interval / rate);

        next_tuple_idx = (next_tuple_idx + 1) % rideLines.size();
        if (current_time - app_start_time >= app_run_time || next_tuple_idx == 0) {
            cout << "[Source] execution time: " << (current_time - app_start_time) / 1000000L
                 << " s, generations: " << generations
                 << ", generated: " << generated_tuples
                 << ", bandwidth: " << generated_tuples / ((current_time - app_start_time) / 1000000L)
                 << " tuples/s" << endl;
            return false;
        }
        return true;
    }

    ~Source_Functor() {}
};

class Filter_Functor {
public:

    bool operator()(TaxiRide &ride) {
//        cout << "[Filter]: " << !ride.isStart << endl;

        return !ride.isStart && isInNYC(ride.location);
    }

};

class FlatMap_Functor {
public:

    void operator()(const TaxiRide &ride, Shipper<cellId_t> &shipper) {
        cellId_t cellId;
        cellId.id = mapToGridCell(ride.location);
        cellId.ts = ride.time;
        cellId.passengerCnt = ride.passengerCnt;
//        cout << "[FlatMap] id: " << cellId.id << endl;
        shipper.push(cellId);
    }

};

class Accumulator_Functor {
private:
    vector<cellId_t> cellIds;
public:
    void operator()(const cellId_t &cellId, cellId_t &cellId_result) {
//        cout << "locationId: " << cellId.id << endl
//             << "Time: " << cellId.ts << endl
//             << "Passengers Count: " << cellId.passengerCnt << endl;
        int position = findCellId(cellId.id);
        if (position == -1) {
            cellIds.push_back(cellId);
        } else {
            cellIds[position].ts = max(cellId.ts, cellIds[position].ts);
            cellIds[position].passengerCnt = cellId.passengerCnt + cellIds[position].passengerCnt;
        }
        cellId_result = cellId_result;
//        cout << "[Accumaltor] size: " << cellIds.size() << endl;
    }

    int findCellId(int id) {
        int position = -1;
        int i = 0;
        while (position == -1 && i < cellIds.size()) {
            if (cellIds[i].id == id) {
                position = i;
            }
            i++;
        }

        return position;
    }
};

class FlatMap_Functor2 {
public:

    void operator()(const cellId_t &cellId, Shipper<cntByLocation_t> &shipper) {
            shipper.push(
                    cntByLocation_t(cellId.id,
                                    cellId.ts, getGridCellCenter(cellId.id), cellId.passengerCnt));
    }

};

class Sink_Functor {
private:
    size_t received; // counter of received results
    long counter = 0;

public:
    // constructor
    Sink_Functor(size_t _keys): received(0), counter(0) {}

    // default
    Sink_Functor(): received(0), counter(0) {}

    // operator()
    void operator()(optional<cntByLocation_t> &out) {
        if (out) {
            received++;
            counter += (*out).passengerCnt;
//            cout << "Received" << (*out).toString() << endl;
        }
        else {
            LOCKED_PRINT("[Sink] Received " << received << " results, total sum " << counter << endl;)
            total_rides.fetch_add(counter);
        }
    }
};

auto F = [](size_t pid, const Iterable<cellId_t> &input, cellId_t &pane_result) {
    long sum = 0;
    for (auto t : input) {
        pane_result.ts = max(pane_result.ts, t.ts);
        pane_result.passengerCnt = pane_result.passengerCnt + t.passengerCnt;
    }
};
// user-defined window function (Non-Incremental Query)
auto G = [](size_t wid, const Iterable<cellId_t> &input, cellId_t &win_result) {
    for (auto t : input) {
        win_result.ts = max(win_result.ts, t.ts);
        win_result.passengerCnt = win_result.passengerCnt + t.passengerCnt;
    }
};

#endif //TAXIRIDE_WINDFLOW_OPERATORS_HPP
