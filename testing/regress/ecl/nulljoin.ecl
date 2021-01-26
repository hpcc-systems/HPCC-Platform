/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

//version fold=false
//version fold=true

import ^ as root;
doFold := #IFDEFINED(root.fold, false);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
            END;

namesRecord2 :=
            RECORD
string20        surname;
string10        forename2;
            END;

joinRecord :=
            RECORD
string20        surname;
string20        surnamex;
string10        forename;
string10        forename2;
boolean         matchedLeft;
boolean         matchedRight;
            END;

namesTableX := dataset([
    {'smith', 'alfred' },
    {'smith', 'albert' },
    {'jones', 'jonnie' }
], namesRecord);

namesTableY := dataset([
    {'smith', 'zorro' },
    {'smith', 'zac' },
    {'jones', 'james' }
], namesRecord2);

storedFalse := FALSE : STORED('storedFalse');
storedTrue := TRUE : STORED('storedTrue');

trueValue := IF(doFold, true, storedTrue);
falseValue := IF(doFold, false, storedFalse);

joinRecord t(namesRecord l, namesRecord2 r) := TRANSFORM
    SELF.surnamex := IF(l.surname <> '', l.surname, r.surname);
    SELF := l;
    SELF := r;
    SELF.matchedLeft := MATCHED(l);
    SELF.matchedRight := MATCHED(r);
END;

doJoin(leftFilter, rightFilter, joinFlags) := MACRO
    OUTPUT(SORT(JOIN(namesTableX(leftFilter), namesTableY(rightFilter), left.surname=right.surname, t(LEFT, RIGHT), #expand(joinFlags)), forename, forename2, surname))
ENDMACRO;

sequential(
    doJoin(falseValue, falseValue, 'inner');
    doJoin(falseValue, falseValue, 'left outer');
    doJoin(falseValue, falseValue, 'right outer');
    doJoin(falseValue, falseValue, 'full outer');

    doJoin(trueValue, falseValue, 'inner');
    doJoin(trueValue, falseValue, 'left outer');
    doJoin(trueValue, falseValue, 'right outer');
    doJoin(trueValue, falseValue, 'full outer');

    doJoin(falseValue, trueValue, 'inner');
    doJoin(falseValue, trueValue, 'left outer');
    doJoin(falseValue, trueValue, 'right outer');
    doJoin(falseValue, trueValue, 'full outer');

    doJoin(trueValue, trueValue, 'inner');
    doJoin(trueValue, trueValue, 'left outer');
    doJoin(trueValue, trueValue, 'right outer');
    doJoin(trueValue, trueValue, 'full outer');
);
