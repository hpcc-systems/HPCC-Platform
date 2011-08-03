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

