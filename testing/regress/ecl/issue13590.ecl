/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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


idRec := { STRING id; };

one := '1' : stored('one');
two := '2' : stored('two');
three := '3' : stored('three');

ids(STRING base) := NOFOLD(DATASET([one+base,two+base], idRec));

rRec := RECORD
    STRING x;
    DATASET(idRec) y;
END;

ds := DATASET(['92','67','56','23'], idRec);

rRec t(idRec l) := TRANSFORM
    y1 := DATASET([three], idRec);
    ids := ids(l.id);
    y2 := NOFOLD(IF(ids[1].id >= '0', ids[1], ids[2]));
    y3 := NOFOLD(IF(l.id > '60', y2, y1[1]));
    SELF.x := l.id;
    SELF.y := y1 & y3;
END;

OUTPUT(PROJECT(ds, t(LEFT)));
