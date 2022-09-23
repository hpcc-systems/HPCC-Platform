/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

leftRecord :=
            RECORD
unsigned6       id1;
unsigned8       id2;
string          id3;
unsigned2       id4;
unsigned4       id5;
string20        surname;
            END;

rightRecord :=
            RECORD
unsigned8       id1;
real            id2;
ebcdic string   id3;
unsigned3       id4;
string          id5;
string20        forename;
            END;

leftTable := dataset('left',leftRecord,FLAT);

rightTable := dataset('left',rightRecord,FLAT);

//output(j);

doJoin(XXX) := FUNCTIONMACRO
   LOCAL myLeft := DISTRIBUTE(leftTable, HASH(XXX));
   LOCAL myRight := DISTRIBUTE(rightTable, HASH(XXX));
   LOCAL j := JOIN(myLeft, myRight, LEFT.XXX=RIGHT.XXX, LOCAL);
   RETURN output(j);
ENDMACRO;

doCastJoin(XXX) := FUNCTIONMACRO
   LOCAL myLeft := DISTRIBUTE(leftTable, HASH(XXX));
   LOCAL myRight := DISTRIBUTE(rightTable, HASH(XXX));
   LOCAL j := JOIN(myLeft, myRight, (unsigned4)LEFT.XXX=(unsigned4)RIGHT.XXX, LOCAL);
   RETURN output(j);
ENDMACRO;

doJoin(id1);        // no warning
doJoin(id2);        // warning
doJoin(id3);        // warning

doCastJoin(id1); // warning
doCastJoin(id4); // no warning
doCastJoin(id5); // warning
