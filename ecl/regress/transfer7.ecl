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





checkTransfer(t1, v1) := macro

output(transfer(v1, t1));
output(transfer(v1, t1) = transfer(nofold(v1), t1))

endmacro;

testTransfer(t1) := macro

checkTransfer(t1, '1234567890123456789');
checkTransfer(t1, U'1234567890123456789');
checkTransfer(t1, 1234567890123456789);
checkTransfer(t1, (udecimal16_0)123456789012345)
endmacro;


//testTransfer(unsigned1);

testTransfer(unsigned1);
testTransfer(little_endian unsigned2);
testTransfer(big_endian unsigned2);
testTransfer(string4);
testTransfer(udecimal8_0);
