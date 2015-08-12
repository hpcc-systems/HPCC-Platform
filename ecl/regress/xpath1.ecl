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
