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

