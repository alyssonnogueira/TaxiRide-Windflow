//
// Created by alyss on 09/12/2019.
//

#ifndef TAXIRIDE_WINDFLOW_TUPLAS_HPP
#define TAXIRIDE_WINDFLOW_TUPLAS_HPP

#include <string>
#include <time.h>

using namespace std;

struct GeoPoint {
    double lon;
    double lat;

    //constructor
    GeoPoint(double _lon, double _lat):lon(_lon), lat(_lat){}

    //default
    GeoPoint():lon(0.0), lat(0.0){}

    // getControlFields method
    tuple<double, double> getControlFields() const {
        return tuple<double, double>(lon, lat);
    }

    // setControlFields method
    void setControlFields(double _lon, double _lat) {
        lon = _lon;
        lat = _lat;
    }

    string toString(){
        ostringstream oss;
        oss << "Latitude: " << lat << endl
            << "Longitude: " << lon << endl;
        return oss.str();
    }
};

struct TaxiRide {
    uint64_t rideId;
    time_t time;
    bool isStart;
    GeoPoint location;
    int passengerCnt;
    float travelDist;
    int locationId;

    // default constructor
    TaxiRide():
            rideId(0),
            time(0),
            isStart(true),
            location(GeoPoint()),
            passengerCnt(0),
            travelDist(0.0),
            locationId(0)
    {}

    // constructor
    TaxiRide(uint64_t _rideId,
             time_t _time,
             bool _isStart,
             GeoPoint _location,
             int _passengerCnt,
             float _travelDist):
            rideId(_rideId),
            time(_time),
            isStart(_isStart),
            location(_location),
            passengerCnt(_passengerCnt),
            travelDist(_travelDist),
            locationId(0)
    {}

    // getControlFields method
    tuple<uint64_t, time_t, bool, GeoPoint, int, float> getControlFields() const {
        return tuple<uint64_t, time_t, bool, GeoPoint, int, float>(rideId, time, isStart, location, passengerCnt, travelDist);
    }

    // setControlFields method
    void setControlFields(uint64_t _rideId, time_t _time, int _passengerCnt) {
        rideId = _rideId;
        isStart = _time;
        passengerCnt = _passengerCnt;
    }

    static TaxiRide fromString(string line){
        size_t pos = 0;
        vector<string> tokens;
        while((pos = line.find(',')) != string::npos){
            tokens.push_back(line.substr(0, pos));
            line.erase(0, pos + 1);
        }

        if(tokens.size() != 6)
            return TaxiRide();

        if (tokens[3].empty() || tokens[4].empty()){
            tokens[3] = "0";
            tokens[4] = "0";
        }
        struct tm tm{};
        strptime(tokens[1].c_str(), "%Y-%m-%d %H:%M:%S", &tm);
        return TaxiRide(
                stoul(tokens[0]),  //rideId
                mktime(&tm), //time
                tokens[2] == "START", //isStart
                GeoPoint(stod(tokens[3]), stod(tokens[4])), // GeoPoint
                stoi(tokens[5]), // passengerCnt
                stof(line)); //travelDist
    }

    string toString(){
        ostringstream oss;
        oss << "RideId: " << rideId << endl
            << "Time: " << time << endl
            << "isStart: " << isStart << endl
            << "Location: " << location.toString() << endl
            << "Passengers Count: " << passengerCnt << endl
            << "Travel Distance: " << travelDist << endl;
        return oss.str();
    }
};

struct cellId_t {
    int id;            // id that indicates the current number of occurrences of the key word
    uint64_t ts;            // timestamp
    int passengerCnt;

    // default constructor
    cellId_t(): id(0), ts(0), passengerCnt(0) {}

    // constructor
    cellId_t(int _id, uint64_t _ts, int _passengerCnt): id(_id), ts(_ts), passengerCnt(_passengerCnt) {}

    // getControlFields method
    tuple<int, uint64_t, int> getControlFields() const {
        return tuple<int, uint64_t, int>(id, ts, passengerCnt);
    }

    // setControlFields method
    void setControlFields(int _id, uint64_t _ts, int _passengerCnt) {
        id = _id;
        ts = _ts;
        passengerCnt = _passengerCnt;
    }

    string toString() {
        ostringstream oss;
        oss << "locationId: " << id << endl
            << "Time: " << ts << endl
            << "Passengers Count: " << passengerCnt << endl;
    }

    // destructor
    ~cellId_t() = default;
};

struct cntByLocation_t {
    int id;            // id that indicates the current number of occurrences of the key word
    uint64_t ts;            // timestamp
    GeoPoint location;
    int passengerCnt;

    // default constructor
    cntByLocation_t(): id(0), ts(0), location(GeoPoint()), passengerCnt(0) {}

    // constructor
    cntByLocation_t(int _id, uint64_t _ts, GeoPoint _location, int _passengerCnt):
                                        id(_id), ts(_ts), location(_location), passengerCnt(_passengerCnt) {}

    // getControlFields method
    tuple<int, uint64_t, int> getControlFields() const {
        return tuple<int, uint64_t, int>(id, ts, passengerCnt);
    }

    // setControlFields method
    void setControlFields(int _id, uint64_t _ts, int _passengerCnt) {
        id = _id;
        ts = _ts;
        passengerCnt = _passengerCnt;
    }

    string toString() {
        ostringstream oss;
        oss << "locationId: " << id << endl
            << "Time: " << ts << endl
            << "Location: " << location.toString() << endl
            << "Passengers Count: " << passengerCnt << endl;
    }

    // destructor
    ~cntByLocation_t() = default;
};

#endif //TAXIRIDE_WINDFLOW_TUPLAS_HPP
