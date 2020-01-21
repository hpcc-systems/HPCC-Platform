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

namesRec := RECORD
 STRING20 lname;
 STRING10 fname;
 UNSIGNED2 age := 25;
 UNSIGNED2 ctr := 0;
END;

namesTable2 := DATASET([{'Flintstone','Fred',35},
 {'Flintstone','Wilma',33},
 {'Jetson','Georgie',10},
 {'Mr. T','Z-man'}], namesRec);

loopBody(SET OF DATASET(namesRec) ds, UNSIGNED4 c) := FUNCTION

 maxAge := MAX(ds[c-1], age);

 RETURN PROJECT(ds[c-1], //ds[0]=original input
         TRANSFORM(namesRec,
                   SELF.age := LEFT.age+c; //c is graph COUNTER
                   SELF.ctr := COUNTER+maxAge;
                   //PROJECT's COUNTER
                   SELF := LEFT));

END;

g1 := GRAPH(namesTable2,3,loopBody(ROWSET(LEFT),COUNTER));
OUTPUT(g1);
