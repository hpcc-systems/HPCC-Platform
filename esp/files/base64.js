/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

var BASE64_enc = "ABCDEFGHIJKLMNOP" +
            "QRSTUVWXYZabcdef" +
            "ghijklmnopqrstuv" +
            "wxyz0123456789+/" +
            "=";

function base64_encode(str) 
{
    var result = "";
    var c1, c2, c3 = "";
    var e1, e2, e3, e4 = "";
    var i = 0;

    while(i < str.length) 
    {
        c1 = str.charCodeAt(i++);
        c2 = str.charCodeAt(i++);
        c3 = str.charCodeAt(i++);

        e1 = c1 >> 2;
        e2 = ((c1 & 3) << 4) | (c2 >> 4);
        e3 = ((c2 & 15) << 2) | (c3 >> 6);
        e4 = c3 & 63;

        if (isNaN(c2)) 
        {
            e3 = 64;
            e4 = 64;
        } 
        else if (isNaN(c3)) 
        {
            e4 = 64;
        }

        result = result + BASE64_enc.charAt(e1) + BASE64_enc.charAt(e2) + 
                BASE64_enc.charAt(e3) + BASE64_enc.charAt(e4);
        c1 = c2 = c3 = "";
        e1 = e2 = e3 = e4 = "";
    } 

    return result;
}
