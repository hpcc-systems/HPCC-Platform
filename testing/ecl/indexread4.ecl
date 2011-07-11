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


recordof(DG_FetchIndex1) createError(boolean isKeyed) := TRANSFORM
    SELF.fname := IF(isKeyed, 'Keyed Limit exceeded', 'Limit exceeded');
    SELF := [];
END;

//Use different filters to ensure the index reads aren't commoned up - and the limit is done within the index read activity

// try it with just one limit
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch1'),1,SKIP), {fname});
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch2'),10,SKIP), {fname});
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch3'),1,SKIP,KEYED), {fname});
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch4'),10,SKIP,KEYED), {fname});

// then try with a keyed and unkeyed....

output(LIMIT(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch5'),1,SKIP,keyed),1,skip), {fname});
output(LIMIT(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch6'),10,SKIP,keyed),10,skip), {fname});


// try it with just one limit

output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch8'),1,ONFAIL(createError(false))), {fname});
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch9'),10,ONFAIL(createError(false))), {fname});
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch10'),1,ONFAIL(createError(true)),KEYED), {fname});
output(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch11'),10,ONFAIL(createError(true)),KEYED), {fname});

// then try with a keyed and unkeyed....

output(LIMIT(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch12'),1,ONFAIL(createError(true)),keyed),1,ONFAIL(createError(false))), {fname});
output(LIMIT(LIMIT(DG_FetchIndex1(Lname='Anderson',fname<>'nomatch13'),10,ONFAIL(createError(true)),keyed),10,ONFAIL(createError(false))), {fname});

