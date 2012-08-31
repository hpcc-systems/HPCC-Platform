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
