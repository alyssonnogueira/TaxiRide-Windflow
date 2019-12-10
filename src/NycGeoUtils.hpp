//
// Created by alyss on 08/12/2019.
//

#ifndef TAXIRIDE_NYCGEOUTILS_HPP
#define TAXIRIDE_NYCGEOUTILS_HPP

const double LonEast = -73.7;
const double LonWest = -74.05;
const double LatNorth = 41.0;
const double LatSouth = 40.5;
const double DeltaLon = 0.0014;
const double DeltaLat = 0.00125;
const int CellCntX = 250;
const int CellCntY = 400;

bool isInNYC(GeoPoint point){
    if (point.lon > LonEast || point.lon < LonWest)
        return false;

    if (point.lat > LatNorth || point.lat < LatSouth)
        return false;

    return true;
}

int mapToGridCell(GeoPoint point) {
    int xIndex = floor((abs(LonWest) - abs(point.lon)) / DeltaLon);
    int yIndex = floor((LatNorth - point.lat) / DeltaLat);
    return xIndex + (yIndex * CellCntX);
}

GeoPoint getGridCellCenter(int gridCellId) {
    int xIndex = gridCellId % CellCntX;
    double lon = ((float) abs(LonWest) - (xIndex * DeltaLon) - (DeltaLon / 2)) * -1.0f;
    int yIndex = (gridCellId - xIndex) / CellCntX;
    double lat = (float) LatNorth - (yIndex * DeltaLat) - (DeltaLat / 2);
    new GeoPoint(lon, lat);
}

#endif //TAXIRIDE_NYCGEOUTILS_HPP