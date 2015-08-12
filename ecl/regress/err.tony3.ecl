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

person := dataset('person', { unsigned8 person_id, string2 per_st, string40 per_first_name, unsigned8 per_dact }, thor);

section_GEOGRAPHY_STATES :=  person.per_st = 'MI' ;

section_GEOGRAPHY := section_GEOGRAPHY_STATES ;

SEGMENT_SELECTS := section_GEOGRAPHY  ;

qset := person(person.per_dact = 1  AND
                    SEGMENT_SELECTS ) ;

iftotal := IF(TRUE, 1, 0);

segmentid := '482';

countrec := RECORD
    segmentid ;
    total := SUM(GROUP, iftotal);
END;

EXPORT counttable_482 := TABLE(qset, countrec, 1, segmentid);
OUTPUT(counttable_482) ;

