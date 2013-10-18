/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//nothor

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

real totalProbability(dataset(ProbRecord) values) := EXP(SUM(values, LN(prob)));

ProbRecord createCombined(ProbRecord l, dataset(ProbRecord) values, boolean skipFilter = true) := transform, skip(not skipFilter)
    self.prob := totalProbability(values);
    self := l;
end;


//Simple nary-join
j1in := MERGEJOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             assert sorted, sorted(area, zone));

//join with global filter
j1inf := MERGEJOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area and left.zone = right.zone) and totalProbability(rows(left))>0.1,
             assert sorted, sorted(area, zone));

//inverse global filter
j1inf2 := MERGEJOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area and left.zone = right.zone) and totalProbability(rows(left))<=0.1,
             assert sorted, sorted(area, zone));

//troups, but no guns and tanks - probably harmless.
j1oy := MERGEJOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             sorted(area, zone), assert sorted, left only);
j1ou := MERGEJOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone),
             sorted(area, zone), assert sorted, left outer);        // doesn't make much sense in real life

//A global filter on a left outer join really doesn't make much sense.
//It should mean a match on all 3 which is out of range is handled as a left only instead.
j1ouf := MERGEJOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area and left.zone = right.zone) and totalProbability(rows(left)) between 0.1 and 0.8,
             sorted(area, zone), assert sorted, left outer);

//Same but with a smaller stepped condition
//Simple nary-join
j2in := MERGEJOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             assert sorted, sorted(area, zone));

//join with global filter
j2inf := MERGEJOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone and totalProbability(rows(left))>0.1,
             assert sorted, sorted(area, zone));

//inverse global filter
j2inf2 := MERGEJOIN([tankSightings, gunSightings, troupSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone and totalProbability(rows(left))<=0.1,
             assert sorted, sorted(area, zone));

//troups, but no guns and tanks - probably harmless.
j2oy := MERGEJOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             sorted(area, zone), assert sorted, left only);
j2ou := MERGEJOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone,
             sorted(area, zone), assert sorted, left outer);        // doesn't make much sense in real life
//join with global join condition
j2ouf := MERGEJOIN([troupSightings, tankSightings, gunSightings],
             STEPPED(left.area = right.area) and left.zone = right.zone and totalProbability(rows(left)) between 0.1 and 0.8,
             sorted(area, zone), assert sorted, left outer);


output(j1in,named('j1in'));
output(j2in,named('j2in'));
output(j1inf,named('j1inf'));
output(j2inf,named('j2inf'));
output(j1inf2,named('j1inf2'));
output(j2inf2,named('j2inf2'));

output(j1oy,named('j1oy'));
output(j2oy,named('j2oy'));
output(j1ou,named('j1ou'));
output(j2ou,named('j2ou'));

output(j1ouf,named('j1ouf'));
output(j2ouf,named('j2ouf'));
