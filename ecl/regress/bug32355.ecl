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

rec := RECORD, MAXLENGTH(10000000)
    STRING text;
END;

ds := dataset( [{
 '<XML>'+
         '<OUTER>'+
              '<CHILD>'+
                      '<GRANDCHILD>tagb1, tagc1</GRANDCHILD>'+
                      '<GRANDCHILD>tagb1, tagc2</GRANDCHILD>'+
                    '</CHILD>'+
                    '<CHILD>'+
                        '<GRANDCHILD>tagb2, tagc1</GRANDCHILD>'+
                        '<GRANDCHILD>tagb2, tagc2</GRANDCHILD>'+
                '</CHILD>'+
         '</OUTER>'+
 '</XML>'}],
 rec);

grandchildRec := record, maxlength(500)
  STRING text;
    END;
childRec := record, maxlength(1000)
  string dummy := '';
  DATASET (grandchildRec) paragraphs;
    END;
outerRec := record, maxlength(1000)
    DATASET (childRec) concurs;
  END;


outerRec getIt(rec l) := TRANSFORM
  SELF.concurs := XMLPROJECT('OUTER/CHILD',
                             TRANSFORM(childRec,
                                       SELF.dummy := XMLTEXT('<>');                 // causes XMLTEXT below to be copy of this
                                       SELF.paragraphs := XMLPROJECT('GRANDCHILD',
                                       TRANSFORM(grandchildRec,
                                                 SELF.text:=XMLTEXT('<>'))          // becomes 'dummy'
                                                )
                                      )
                            );
 END;

outerRec getIt2(rec l) := TRANSFORM
  SELF.concurs := XMLPROJECT('OUTER/CHILD',
                             TRANSFORM(childRec,
                                       SELF.paragraphs := XMLPROJECT('GRANDCHILD',
                                       TRANSFORM(grandchildRec,
                                                 SELF.text:=XMLTEXT('<>'))
                                                )
                                      )
                            );
 END;
d1 := PARSE(ds, text, getIt(LEFT), XML('XML'));
d2 := TABLE(d1.concurs.paragraphs, { text });
output(d2); // wrong
d3 := PARSE(ds, text, getIt2(LEFT), XML('XML'));
d4 := TABLE(d3.concurs.paragraphs, { text });
output(d4); // correct

