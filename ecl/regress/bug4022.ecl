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