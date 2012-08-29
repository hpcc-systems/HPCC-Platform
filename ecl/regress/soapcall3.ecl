/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


xyzServiceOutRecord :=
    RECORD
        string name{xpath('Name')};
        unsigned6 id{xpath('ADL')};
        real8 score;
    END;

callXyzService(_NAME, _ID, _SCORE, OUTF) := macro
        outF := SOAPCALL('myip','XyzService',
                        {
                            string Name{xpath('Name')} := _NAME,
                            unsigned id{xpath('ADL')} := _ID,
                            real8 score := _SCORE,
                        }, DATASET(xyzServiceOutRecord), xpath('Request'))
    endmacro;



callXyzService('Gavin Hawthorn', 1234567, 3.14159267, results);

output(results);
