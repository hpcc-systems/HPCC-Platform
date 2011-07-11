/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
