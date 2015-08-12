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



set of string searchSet := [] : stored('searchSet');
string matchName := '' : stored('matchName');


pattern word := pattern('[a-z]+');
pattern ws := [' ','/t'];
pattern match := validate(word, MATCHTEXT in searchSet or MATCHTEXT = matchName);
pattern S := match ws match;


ds := dataset([{'Gavin Hawthorn'}],{string line});


x := parse(ds, line, S, TRANSFORM(LEFT));

output(x);
