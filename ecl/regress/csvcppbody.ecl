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


MyRec := RECORD
    INTEGER a1;
    INTEGER a2;
    REAL a3;
    INTEGER a4;
END;

DATASET(MyRec) xx := DATASET('dataset.ecl', MyRec, CSV(SEPARATOR(',')));

x:=xx(a1<=89999);

INTEGER MyFunc(DATASET(MyRec) a) := BEGINC++
//#include <iostream>
#body

//std::cout<<"size="<<lenA<<std::endl;
return lenA;
ENDC++;

OUTPUT(MyFunc(xx));