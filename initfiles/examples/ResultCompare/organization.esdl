/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.

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

////////////////////////////////////////////////////////////

ESPenum AddressType : string
{
    HOME("Home"),
    WORK("Business"),
    HOTEL("Government")
};

ESPStruct NameInfo
{
    string First("Joe");
    string Middle;
    string Last("Doe");
    ESParray<string, Alias> Aliases;
};

ESPStruct AddressInfo
{
    ESPenum AddressType type("Home");
    string Line1;
    string Line2;
    string City;
    string State;
    int Zip(33487);
    int Zip4(4444);
};

ESPStruct PersonInfo
{
     ESPstruct NameInfo Name;
     ESParray<ESPstruct AddressInfo, Address> Addresses;
};

ESPRequest OrganizationInfoRequest
{
    string OrgName;
};

ESPResponse OrganizationInfoResponse
{
    string OrgName;
    ESPstruct AddressInfo Address;
    ESParray<ESPstruct PersonInfo, Member> Members;
    ESParray<ESPstruct PersonInfo, Guest> Guests;
};

ESPservice [version("0.01")] Organizations
{
    ESPmethod OrganizationInfo(OrganizationInfoRequest, OrganizationInfoResponse);
};

