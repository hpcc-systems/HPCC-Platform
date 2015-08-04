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

OutputFormat1 := RECORD
  STRING10  fname;
  STRING12  lname;
END;
B := DATASET([{'Fred','Bell'},
              {'George','Blanda'},
              {'Sam',''}
              ], OutputFormat1);
OUTPUT(B,,'RTTEMP::fred.xml', XML); // writes B to the fred.xml file
/* the Fred.XML file looks like this:
<Dataset>
<Row><fname>Fred</fname><lname>Bell</lname></Row>
<Row><fname>George</fname><lname>Blanda</lname></Row>
</Dataset>
*/
OUTPUT(B,,'RTTEMP::fred2.xml',XML('MyRow',
                  HEADING('<?xml version=1.0 ...?>\n<filetag>\n',
                        '</filetag>\n')));
/* the Fred2.XML file looks like this:
<?xml version=1.0 ...?>
<filetag>
<MyRow><fname>Fred      </fname><lname>Bell        </lname></MyRow>
<MyRow><fname>George     </fname><lname>Blanda      </lname></MyRow>
</filetag>
*/
OUTPUT(B,,'RTTEMP::fred3.xml',XML('MyRow',TRIM,OPT));
/* the Fred3.XML file looks like this:
<Dataset>
<MyRow><fname>Fred</fname><lname>Bell</lname></MyRow>
<MyRow><fname>George</fname><lname>Blanda</lname></MyRow>
</Dataset>
*/
