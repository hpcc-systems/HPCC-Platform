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

import Std.File AS FileServices;

FetchData   := DATASET([
    {3000,   'FL', 'Boca Raton',  'London',   'Bridge'},
    {3500,   'FL', 'Boca Raton',  'Anderson', 'Sue'},
    {3500,   'FL', 'Boca Raton',  'Anderson', 'John'},
    {35,     'FL', 'Boca Raton',  'Smith',    'Frank'},
    {3500,   'FL', 'Boca Raton',  'Johnson',  'Joe'},
    {50,     'FL', 'Boca Raton',  'Smith',    'Sue'},
    {135,    'FL', 'Boca Raton',  'Smith',    'Nancy'},
    {3500,   'FL', 'Boca Raton',  'Johnson',  'Sue'},
    {235,    'FL', 'Boca Raton',  'Smith',    'Fred'},
    {335,    'FL', 'Boca Raton',  'Taylor',   'Frank'},
    {3500,   'FL', 'Boca Raton',  'Johnson',  'Jane'},
    {3500,   'FL', 'Boca Raton',  'Jones',    'Frank'},
    {3500,   'FL', 'Boca Raton',  'Jones',    'Tommy'},
    {3500,   'FL', 'Boca Raton',  'Doe',        'John'},
    {3500,   'FL', 'Boca Raton',  'Anderson', 'Joe'},
    {3500,   'FL', 'Boca Raton',  'Doe',        'Jane'},
    {3500,   'FL', 'Boca Raton',  'Doe',        'Joe'},
    {3500,   'FL', 'Boca Raton',  'Johnson',  'Larry'},
    {3500,   'FL', 'Boca Raton',  'Johnson',  'John'},
    {3500,   'FL', 'Boca Raton',  'Anderson', 'Larry'},
    {3500,   'FL', 'Boca Raton',  'Anderson', 'Jane'},
    {30,     'FL', 'Boca Raton',  'Smith',      'Zeek'}], DG_FetchRecord);

// Try to make sure that there are at least 2 parts with data on... This gets a little hairy in places!
// Note that in order to facilitate testing of preload / preloadIndexed versions of data, we generate the same file under 3 names

#IF (useLocal=true)
twoways := distribute(FetchData, IF(lname < 'Jom', 1, 0));
output(sort(twoways,record,local),,DG_FetchFileName,OVERWRITE);
output(sort(twoways,record,local),,DG_FetchFilePreloadName,OVERWRITE);
output(sort(twoways,record,local),,DG_FetchFilePreloadIndexedName,OVERWRITE);

BUILDINDEX(DG_FetchIndex1,DG_FetchIndex1Name,OVERWRITE,NOROOT);
BUILDINDEX(DG_FetchIndex2,DG_FetchIndex2Name,OVERWRITE,NOROOT);

#ELSE
twoways := distribute(FetchData, IF(lname < 'Jom', 0, 1));
output(sort(twoways,record,local),,DG_FetchFileName,OVERWRITE);
output(sort(twoways,record,local),,DG_FetchFilePreloadName,OVERWRITE);
output(sort(twoways,record,local),,DG_FetchFilePreloadIndexedName,OVERWRITE);

#IF (usePayload=false)
 sortedFile := SORT(DG_FETCHFILE, Lname,Fname,__filepos, LOCAL);
 BUILDINDEX(sortedFile,{Lname,Fname,__filepos},DG_FetchIndex1Name, OVERWRITE, SORTED);
 BUILDINDEX(sortedFile,{Lname,Fname, __filepos}, DG_FetchIndex2Name, OVERWRITE, SORTED);
#ELSE
 #IF (useVarIndex=true)
  sortedFile := SORT(DG_FETCHFILE, Lname,Fname,state ,__filepos, LOCAL);
  BUILDINDEX(sortedFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex1Name, OVERWRITE, SORTED);
  BUILDINDEX(sortedFile,{Lname,Fname},{STRING fn := TRIM(Fname), state, STRING100 x {blob}:= fname, __filepos},DG_FetchIndex2Name, OVERWRITE, SORTED);
 #ELSE
  sortedFile := SORT(DG_FETCHFILE, Lname,Fname,state ,__filepos, LOCAL);
  BUILDINDEX(sortedFile,{Lname,Fname},{state, __filepos}, DG_FetchIndex1Name, OVERWRITE, SORTED);
  BUILDINDEX(sortedFile,{Lname,Fname},{state, __filepos}, DG_FetchIndex2Name, OVERWRITE, SORTED);
 #END
#END

#END

fileServices.AddFileRelationship( DG_FetchFileName, DG_FetchFilePreloadName, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( DG_FetchFileName, DG_FetchFilePreloadIndexedName, '', '', 'view', '1:1', false);

fileServices.AddFileRelationship( DG_FetchFileName, DG_FetchIndex1Name, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( DG_FetchFileName, DG_FetchIndex1Name, '__fileposition__', '__filepos', 'link', '1:1', true);
fileServices.AddFileRelationship( DG_FetchFileName, DG_FetchIndex2Name, '', '', 'view', '1:1', false);
fileServices.AddFileRelationship( DG_FetchFileName, DG_FetchIndex2Name, '__fileposition__', '__filepos', 'link', '1:1', true);
