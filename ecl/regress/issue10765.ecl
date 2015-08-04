/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

SHARED Ref_Loadinfo_Lay := RECORD
string2 postalcode;
unsigned4 filenamedate;
string100 filename;
unsigned8 _internal_fpos_;
END;
fileName := '~thor::test::qa::ref_loadinfo';

Ref_Loadinfo_DS:= DATASET([],Ref_Loadinfo_Lay);
Ref_Loadinfo_Indx := INDEX(Ref_Loadinfo_DS,{postalcode},{Ref_Loadinfo_DS},fileName);

COUNT(Ref_Loadinfo_Indx);
