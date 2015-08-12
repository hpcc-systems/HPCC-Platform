/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

person := dataset('person', { unsigned8 person_id, string1 per_marital_status, string1 per_x }, thor);

//------------------------------------------------------------------
tab1 := person(person.per_x = '1');

segmentid1 := '9480';

countrec1 := RECORD
segmentid1 ;
total := SUM(GROUP, 1);
marital_status := SUM(GROUP, IF(person.per_marital_status = ' ', 0,
1));
END;

pricetable1 := TABLE(tab1, countrec1) ;

OUTPUT(pricetable1) ;
//------------------------------------------------------------------


//... to behave exactly like this piece of ECL below:
//------------------------------------------------------------------
tab2 := person(person.per_x = '1');

segmentid2 := '9480';

countrec2 := RECORD
segmentid2 ;
total := SUM(GROUP, 1);
marital_status := SUM(GROUP, IF(person.per_marital_status = ' ', 0,
1));
END;

pricetable2 := TABLE(tab2, countrec2, segmentid2, FEW) ;

OUTPUT(pricetable2) ;
//------------------------------------------------------------------

//i.e. we don't need to specify the
//
//   ", segmentid, FEW) "
//
//in the last TABLE line.
//
//Also we need to make sure that while grouping constants (for eg. "segmentid" in
//the ECL above), the constant value that is finally generated is correct.