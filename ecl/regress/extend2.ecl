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


errorMessageRec := record
        unsigned4 code;
        string text;
    end;

createErrorMessage(unsigned4 _code, string _text) :=
    dataset([{_code, _text}], errorMessageRec);

reportErrorMessage(unsigned4 _code, string _text) :=
    output(createErrorMessage(_code, _text),named('ErrorResult'),extend);

output(createErrorMessage(100, 'Failed'),named('ErrorResult'),extend);
output(createErrorMessage(101, 'Failed again'),named('ErrorResult'),extend);
reportErrorMessage(102, 'And again');

allErrors := dataset(workunit('ErrorResult'), errorMessageRec);
output(allErrors,,'errors.dat');
