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

//UseStandardFiles
//UseIndexes
//nolocal
unsigned inrecs := count(DG_FlatFile) : stored('incount');

sequential(
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('left outer self failed 1')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('left outer self atmost failed 2')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('half-keyed Left outer failed 3')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('half-keyed Left outer atmost failed 4')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('left outer failed 5')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('left outer atmost failed 6')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('left outer failed 7')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('left outer atmost failed 8')),

    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != 0, FAIL('left only self failed 1')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('left only self atmost failed 2')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != inrecs/2, FAIL('half-keyed Left only failed 3')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('half-keyed Left only atmost failed 4')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != 0, FAIL('left only failed 5')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('left only atmost failed 6')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != inrecs, FAIL('left only failed 7')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('left only atmost failed 8')),

    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname),record)) != inrecs, FAIL('left inner self failed 1')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('left inner self atmost failed 2')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname),record)) != inrecs/2, FAIL('half-keyed Left inner failed 3')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('half-keyed Left inner atmost failed 4')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname),record)) != inrecs, FAIL('left inner failed 5')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('left inner atmost failed 6')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname),record)) != 0, FAIL('left inner failed 7')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('left inner atmost failed 8')),
    output('All tests succeeded')
);
