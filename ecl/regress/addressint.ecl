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

set of unsigned6  in_set := [   ] : stored('InputID');

layout := record
    string20	 	acctno := '';
    UNSIGNED6 ValID := '';	
    string1	 ValSrc := '';	
end;

layout_did := record
	unsigned6 id:=0;
end;


dsSet := if(exists(in_set),
            project(dataset(in_set,layout_did),
                    transform(layout, self.acctno := '1';self.ValId:=left.id;self.ValSRC:='S';self:=[])));

nothor(output(dsSet));

