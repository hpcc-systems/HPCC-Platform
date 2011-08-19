/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
