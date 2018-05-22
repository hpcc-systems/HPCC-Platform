/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

IMPORT Std;
// Test various record translations using ifblocks

phonerecord := RECORD
  string5 areacode;
  string7 number;
END;

contactrecord := RECORD
  phonerecord phone;
  boolean hasemail;
  IFBLOCK(SELF.hasemail)
    string email;
    boolean hasSecondEmail;
    IFBLOCK(SELF.hasSecondEmail)
      string secondEmail;
    END;
  END;
END;

secretaryRecord := RECORD
  boolean hasSecretary;
  IFBLOCK(Self.hasSecretary)
    contactRecord secretaryContact;
  END;
END;

mainrec := RECORD
  string surname;
  string forename;
  phonerecord homephone;
  boolean hasmobile;
  IFBLOCK(SELF.hasmobile)
    phonerecord mobilephone;
  END;
  contactrecord contact;
  secretaryRecord secretary;
  string2 endmarker;
END;

outrec := RECORD
  mainrec;
  string1 endmarker2 { default('$')};
END;

d := dataset([
  {'Hallidayaaaaa','Richard','01526','1234567',false,'01943','7654321',false,false,'$$'},
  {'Halliday','Richard','01526','1234567',false,'01943','7654321',true,'me@home',false,false,'$$'},
  {'Holliday','Richard','01526','1234567',false,'01943','7654321',true,'me@home',true,'me@work',false,'$$'},
  {'Hallidayaaaaa','Richard','01526','1234567',false,'01943','7654321',false,true,'123','456',false,'$$'},
  {'Halliday','Richard','01526','1234567',false,'01943','7654321',false,true,'123','456',true,'secretary@office.com',false,'$$'},
  {'Holliday','Richard','01526','1234567',false,'01943','7654321',true,'me@home',true,'me@work',true,'123','456',true,'secretary@office.com',true,'secretary@home','$$'}
  ], mainrec);
d;

Std.Type.vrec(outrec).translate(d);
