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

zpr :=
            RECORD
string20        per_cms_pname;
string20        per_cms_qname := '';
            END;

zperson := dataset([{'a'}], zpr);


x := choosesets(zperson, per_cms_pname='Gavin'=>100,per_cms_pname='Richard'=>20,per_cms_pname='Mia'=>1000,94);
output(x,,'out1.d00');

y := choosesets(zperson, per_cms_pname='Gavin'=>100,per_cms_pname='Richard'=>20,per_cms_pname='Mia'=>1000,94,EXCLUSIVE);
output(y,,'out1.d00');
