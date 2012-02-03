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

import Std.File as Fileservices;
//noRoxie

// Following gh-622's recipe
rec := RECORD
    string name;
    string city;
    string phone;
END;

ds := DATASET([{'foo', 'london',    '123456'},
               {'bar', 'cambridge', '321654'},
               {'baz', 'blackburn', '654321'}], rec);
OUTPUT(ds);

small_rec := RECORD
    string name;
END;

small_rec trans(rec r) := TRANSFORM
    SELF.name  := r.name;
END;

// In Thor, this created a file that could not be replicated
ds2 := PROJECT(ds, trans(LEFT)) : PERSIST('~temp::PersistReplicate');
OUTPUT(ds2);

// Now, replicate
FileServices.Replicate('~temp::PersistReplicate');
