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
