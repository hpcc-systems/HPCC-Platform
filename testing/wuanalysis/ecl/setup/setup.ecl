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


IMPORT $.^.common.Files as Files;

//nosetup_roxie
//nokey

// Generate Data
layout_person := RECORD
   STRING20 user;
END;

layout_sites := RECORD
   STRING30 url;
END;

persons := DATASET([{'Ned'},{'Robert'}, {'Jaime'}, {'Catelyn'}, {'Cersei'}, {'Daenerys'}, {'Jon'},
                    {'Sansa'}, {'Arya'}, {'Robb'}, {'Theon'}, {'Bran'}, {'Joffrey'}, {'Hound'}, {'Tyrion'}], layout_person );
                   
sites := DATASET([{'www.yahoo.com'}, {'www.amazon.com'}, {'www.cnn.com'}, {'www.yahoo.com'}, {'www.bbc.co.uk'}], layout_sites);

Files.layout_visits f(layout_person l, layout_sites r) := TRANSFORM
  SELF.user := l.user;
  SELF.url := r.url;
  SELF.timestamp := 0;
END;

v := JOIN(persons, sites, true, f(LEFT,RIGHT), ALL);

Files.layout_visits addTime(Files.layout_visits l, INTEGER c) := TRANSFORM
  SELF.timestamp := RANDOM();
  SELF := L;
END;

visits1 := DISTRIBUTE(NORMALIZE(v, 200000, addTime(LEFT, COUNTER)), HASH32(timestamp));
visits2 := DISTRIBUTE(NORMALIZE(v, 200000, addTime(LEFT, COUNTER)), HASH32(url));

sname := ['Abraham', 'Adamson', 'Hawkins', 'Morris', 'Triggs'];

Files.layout_user genUser(Files.layout_visits l, integer c) := TRANSFORM
   SELF.id := c;
   SELF.FirstName := l.user;
   SELF.LastName := sname[c % 5+1];
END;
user := PROJECT(visits1, genUser(LEFT, COUNTER));

OUTPUT(visits1, , Files.testfile1, OVERWRITE);
OUTPUT(visits2, , Files.testfile2, OVERWRITE);
SEQUENTIAL(
   OUTPUT(user, , Files.testfile4, OVERWRITE);
   BUILDINDEX(Files.INDX1, OVERWRITE);
);

