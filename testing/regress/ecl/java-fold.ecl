/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

//class=embedded
//class=3rdparty


import ^ as root;
import java;
HPCCBaseDir := #IFDEFINED(root.HPCCBaseDir, '/opt/HPCCSystems/');

string jcat(string a, string b) := IMPORT(java, 'JavaCat.cat:(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;' : classpath(HPCCBaseDir + 'classes'),FOLD);
integer jadd(integer a, integer b) := IMPORT(java, 'JavaCat.add:(II)I' :FOLD);

ASSERT(jcat('Hello',' world')='Hello world', CONST);
ASSERT(jadd(1,2)=3, CONST);
OUTPUT('ok');
