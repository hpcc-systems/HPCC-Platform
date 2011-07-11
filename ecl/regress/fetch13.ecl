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

#option ('targetClusterType', 'roxie');

import sq;
sq.declareCommon();

//UseStandardFiles

//Daft test of fetch retrieving a dataset
myPeople := sqSimplePersonBookDs(surname <> '');

recfp := {unsigned8 rfpos, sqSimplePersonBookDs};
recfp makeRec(sqSimplePersonBookDs L, myPeople R) := TRANSFORM
    self.rfpos := R.filepos;
    self := L;
END;

recfp makeRec2(sqSimplePersonBookDs L, myPeople R) := TRANSFORM
    self.rfpos := R.filepos;
    self.books := L.books;
    self := [];
END;

recfp makeRec3(sqSimplePersonBookDs L, myPeople R) := TRANSFORM
    self.rfpos := R.filepos;
    self.books := L.books+R.books;
    self := L;
END;

fetched := fetch(sqSimplePersonBookDs, myPeople, right.filepos, makeRec(left, right));
fetched2 := fetch(sqSimplePersonBookDs, myPeople, right.filepos, makeRec2(left, right));
fetched3 := fetch(sqSimplePersonBookDs, myPeople, right.filepos, makeRec3(left, right));

// temporary hack to get around codegen optimizing platform(),once call into global (and therefore hthor) context.
nononcelib := 
    SERVICE
varstring platform() : library='graph', include='eclhelper.hpp', ctxmethod, entrypoint='getPlatform';
    END;

recordof(sqSimplePersonBookDs) removeFp(recfp l) := TRANSFORM
    SELF := l;
END;
sortIt(dataset(recfp) ds) := FUNCTION
    RETURN IF(nononcelib.platform() = 'thor', PROJECT(SORT(ds, rfpos),removeFp(LEFT)), PROJECT(ds,removeFp(LEFT)));
END;

sequential(
    output(sortIt(fetched)),
    output(sortIt(fetched2)),
    output(sortIt(fetched3))
);
