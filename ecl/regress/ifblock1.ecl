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

a := record
            unsigned1 val1;
end;

b := record
            unsigned1 vala;
            unsigned1 valb;
end;

c := record
            unsigned1 val3;
            unsigned1 val4;
            unsigned1 val5;
end;

r := record
            string1 code;
            ifblock (self.code = 'A')
                        a;
            end;
            ifblock (self.code = 'B')
                        b;
            end;
            ifblock (self.code = 'C')
                        c;
            end;
end;

x := dataset ([{'A',1},{'B',1,2},{'C',1,2,3}],r);

OUTPUT (X);
