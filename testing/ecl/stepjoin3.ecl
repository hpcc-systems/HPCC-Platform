/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

//skip type==thor TBD

ProbRecord := RECORD
    INTEGER area;
    integer zone;
    real prob;
END;


tankSightings := dataset([{1,1,0.1},{1,2,0.3},{1,3,0.2},
                        {1,4,0.7},
                        {2,2,0.8},{2,3,0.6},{2,4,0.9},
                        {3,1,0.3},{3,2,0.6},{3,4,0.2}], ProbRecord);

gunSightings := dataset([{1,1,0.3},{1,2,0.3},{1,4,0.5},
                        {1,4,0.7},
                        {2,1,0.4},{2,2,1},{2,3,0.6},{2,4,0.9},
                        {3,1,0.3},{3,2,0.6},{3,4,0.2}], ProbRecord);

troupSightings := dataset([{1,1,0.8},{1,2,0.7},{1,3,0.9},{1,4,0.4},
                        {1,5,1.0},{1,6,0.7},
                        {2,2,0.3},{2,3,0.4},{2,4,0.7},
                        {3,1,0.3},{3,2,0.6},{3,4,0.2}], ProbRecord);

//Join potential sightings from different areas to select areas that are likely to show an impending attack

real totalProbability(dataset(ProbRecord) values) := round(EXP(SUM(values, LN(prob))), 6);

ProbRecord createCombined(ProbRecord l, dataset(ProbRecord) values, boolean skipFilter = true) := transform, skip(not skipFilter)
    self.prob := totalProbability(values);
    self := l;
end;


//Simple nary-join
j1in := JOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             createCombined(left, rows(left)), assert sorted, sorted(area, zone));

//join with global filter
j1inf := JOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area and left.zone = right.zone) and totalProbability(rows(left))>0.1,
             createCombined(left, rows(left)), assert sorted, sorted(area, zone));

//inverse global filter
j1inf2 := JOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area and left.zone = right.zone) and totalProbability(rows(left))<=0.1,
             createCombined(left, rows(left)), assert sorted, sorted(area, zone));

//troups, but no guns and tanks - probably harmless.
j1oy := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             createCombined(left, rows(left)), sorted(area, zone), assert sorted, left only);
j1ou := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             createCombined(left, rows(left)), sorted(area, zone), assert sorted, left outer);      // doesn't make much sense in real life

//A global filter on a left outer join really doesn't make much sense.
//It should mean a match on all 3 which is out of range is handled as a left only instead.
j1ouf := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone) and totalProbability(rows(left)) between 0.1 and 0.8,
             createCombined(left, rows(left)), sorted(area, zone), assert sorted, left outer);

j1ous := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             createCombined(left, rows(left), totalProbability(rows(left)) between 0.1 and 0.8), sorted(area, zone), assert sorted, left outer);

//Same but with a smaller stepped condition
//Simple nary-join
j2in := JOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             createCombined(left, rows(left)), assert sorted, sorted(area, zone));

//join with global filter
j2inf := JOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone and totalProbability(rows(left))>0.1,
             createCombined(left, rows(left)), assert sorted, sorted(area, zone));

//inverse global filter
j2inf2 := JOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone and totalProbability(rows(left))<=0.1,
             createCombined(left, rows(left)), assert sorted, sorted(area, zone));

//troups, but no guns and tanks - probably harmless.
j2oy := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             createCombined(left, rows(left)), sorted(area, zone), assert sorted, left only);
j2ou := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             createCombined(left, rows(left)), sorted(area, zone), assert sorted, left outer);      // doesn't make much sense in real life
//join with global join condition
j2ouf := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone and totalProbability(rows(left)) between 0.1 and 0.8,
             createCombined(left, rows(left)), sorted(area, zone), assert sorted, left outer);

//similar, but implemented via a skip, so filters records differently
j2ous := JOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             createCombined(left, rows(left), totalProbability(rows(left)) between 0.1 and 0.8), sorted(area, zone), assert sorted, left outer);


sequential(
output(j1in,named('j1in')),
output(j2in,named('j2in')),
output(j1inf,named('j1inf')),
output(j2inf,named('j2inf')),
output(j1inf2,named('j1inf2')),
output(j2inf2,named('j2inf2')),

output(j1oy,named('j1oy')),
output(j2oy,named('j2oy')),
output(j1ou,named('j1ou')),
output(j2ou,named('j2ou')),

output(j1ouf,named('j1ouf')),
output(j2ouf,named('j2ouf')),
output(j1ous,named('j1ous')),
output(j2ous,named('j2ous')));
