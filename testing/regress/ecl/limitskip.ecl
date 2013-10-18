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

irec := RECORD
    UNSIGNED1 i;
END;

ijrec := RECORD
    UNSIGNED i;
    UNSIGNED j;
END;

ijrec xfm(irec l, irec r) := TRANSFORM
    SELF.j := r.i;
    SELF := l;
END;

vector := DATASET([{1}, {2}, {3}, {4}, {5}], irec);

matrix := JOIN(vector, vector, LEFT.i <= RIGHT.i, xfm(LEFT, RIGHT), ALL);

grpd := GROUP(SORT(matrix, j), j);

lim1 := LIMIT(grpd, 15, SKIP);
lim2 := LIMIT(grpd, 14, SKIP);
lim3 := LIMIT(grpd, 0, SKIP);

OUTPUT(SORT(lim1, -i));
OUTPUT(SORT(lim2, -i));
OUTPUT(SORT(lim3, -i));

ijrec createError := TRANSFORM
    SELF.i := 99999;
    SELF.j := 99999;
END;


lim4 := LIMIT(grpd, 15, ONFAIL(createError));
lim5 := LIMIT(grpd, 14, ONFAIL(createError));
lim6 := LIMIT(grpd, 0, ONFAIL(createError));

OUTPUT(SORT(lim4, -i));
OUTPUT(SORT(lim5, -i));
OUTPUT(SORT(lim6, -i));

ijrec noCreateError := TRANSFORM,skip(true)
    SELF.i := 99999;
    SELF.j := 99999;
END;


lim7 := LIMIT(grpd, 15, ONFAIL(noCreateError));
lim8 := LIMIT(grpd, 14, ONFAIL(noCreateError));
lim9 := LIMIT(grpd, 0, ONFAIL(noCreateError));

OUTPUT(SORT(lim7, -i));
OUTPUT(SORT(lim8, -i));
OUTPUT(SORT(lim9, -i));

