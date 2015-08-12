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

filetype := 'x';

key_id (inf, myID, myFiletype) :=

MACRO

            INDEX(inf(myID != ''),{inf.myID},{inf},'~thor::key::auto'+myFiletype+'_id')

ENDMACRO;



ds := dataset([],{string50 id});

//id_key := key_id(ds, id, 'asdf');
id_key :=             INDEX(ds(id != ''),{ds.id},{ds},'~thor::key::auto'+filetype+'_id');


output(id_key);
