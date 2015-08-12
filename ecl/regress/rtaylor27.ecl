/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
  STRING20       lname;
  STRING10       fname;
  UNSIGNED2    age := 25;
  UNSIGNED2    ctr := 0;
END;

namesTable2 := DATASET([{'Flintstone','Fred',35},

                                                {'Flintstone','Wilma',33},

                                                {'Jetson','Georgie',10},

                                                {'Mr. T','Z-man'}], namesRec);



loopBody(set of DATASET(namesRec) ds, unsigned4 c) :=

            PROJECT(ds[c-1],

                          TRANSFORM(namesRec,

                                                SELF.age := LEFT.age+c;

                                                SELF.ctr := COUNTER ;

                                                SELF := LEFT));


g1 := GRAPH(namesTable2,10,loopBody(RANGE(ROWSET(LEFT), [1,2,3]),COUNTER));
output(g1);
