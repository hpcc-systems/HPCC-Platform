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
