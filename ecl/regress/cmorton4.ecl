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

export Layout_Desc :=
RECORD
    STRING4 hri;
    STRING desc;
END;

r := record
string9 xyz;
string1 valid_xyz;
dataset(Layout_Desc) hri_xyz;
end;

d := dataset([
{'347031328', []},
{'076482587', []},
{'347031328', []},
{'097590172', []},
{'602935256', []},
{'923269240', []},
{'338746452', []},
{'801877227', []},
{'923269240', []},
{'427501145', []},
{'393723262', []}], r);

output(d)
