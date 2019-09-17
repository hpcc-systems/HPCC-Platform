/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

JPEG(INTEGER len) := TYPE
    EXPORT DATA LOAD(DATA Dd) := Dd[1..len];
    EXPORT DATA STORE(DATA Dd) := Dd[1..len];
    EXPORT INTEGER PHYSICALLENGTH(DATA Dd) := len;
END;

rec := RECORD
    string60 id;
    UNSIGNED4 imgLength;
    JPEG(SELF.imgLength) photo;
END;

rec2 := RECORD(rec)
    unsigned8 filepos{virtual(fileposition)};
END;

images__jpeg(integer8 len) := TYPE
 EXPORT data load(data dd) := dd[1..len];
 EXPORT integer8 physicallength(data dd) := len;
 EXPORT data store(data dd) := dd[1..len];
END;

rec3 := RECORD,maxlength(15000)
  string60 id;
  unsigned4 imglength;
  images__jpeg(SELF.imglength) photo;
END;

ds := dataset('images', rec2, thor, __option__(legacy));

output(ds);

x := DATASET([0,1000,10000], { unsigned fpos});
output(fetch(ds, x, RIGHT.fpos, transform(left),hint(layoutTranslation('ecl'))));

output(ds,{ { ds } - filepos } ,'images2');

ds2 := dataset('images2', rec3, thor, __option__(legacy));
output(ds2);
