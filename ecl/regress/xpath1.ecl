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

Layout_ConsumerName := record
            string75 Fll {xpath('Full')};
            string20 frst {xpath('First')};
            string20 mddl {xpath('Middle')};
            string20 lst         {xpath('Last')};
end;

Layout_SearchBy := record
            Layout_consumerName consumername {xpath('ConsumerName')};
            string120           CompanyName {xpath('CompanyName')};
            string10 PhoneNumber {xpath('PhoneNumber')};
//            layout_address  Address {xpath('Address')};
end;

export Layout_Targus_Out := record
            layout_SearchBy                       SearchBy {xpath('SearchBy')};
//            layout_ResponseHeader ResponseHeader {xpath('ResponseHeader')};
            //layout_WirelessConnectionSearchResult WirelessConnectionSearchResult {xpath('WirelessConnectionSearchResult/')};
            //layout_VerifyExpressResult VerifyExpressResult {xpath('VerifyExpressResult')};
end;



ds := dataset('ds', Layout_Targus_Out, xml);
output(ds);
