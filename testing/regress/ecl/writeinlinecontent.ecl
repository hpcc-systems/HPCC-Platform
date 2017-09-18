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

#OPTION('writeInlineContent', true);

inline0Rec := RECORD
   string color;
   string shape {xpath('Shape')};
   string texture;
END;

L0Rec := RECORD
   DATASET(inline0Rec) peeps;
END;

RxRec := RECORD
   STRING cst {XPATH('cst')};
END;

ShapeRec := RECORD
   RxRec shape {XPATH('Shape')};
END;

NestedShapeDS := RECORD
   DATASET(ShapeRec) Shapes {XPATH('L1/L2')};
END;

RxDSRec := RECORD
   DATASET(RxRec) Shapes {XPATH('Shapes/Shape')};
END;

ds := DATASET([{[
        {'<rgb>Red</rgb>','<cst>Circle</cst>','<sss>Sandy</sss>'},
        {'<rgb>Green</rgb>','<cst>Square</cst>','<sss>Smooth</sss>'},
        {'<rgb>Blue</rgb>','<cst>Triangle</cst>','<sss>Spikey</sss>'}
   ]}], L0Rec);

named_dsj := DATASET([{[
        {'{"rgb": "Red"}','{"cst": "Circle"}','{"sss": "Sandy"}'},
        {'{"rgb": "Green"}','{"cst": "Square"}','{"sss": "Smooth"}'},
        {'{"rgb": "Blue"}','{"cst": "Triangle"}','{"sss": "Spikey"}'}
   ]}], L0Rec);

unnamed_dsj := DATASET([{[
        {'"rgb": "Red"','"cst": "Circle"','"sss": "Sandy"'},
        {'"rgb": "Green"','"cst": "Square"','"sss": "Smooth"'},
        {'"rgb": "Blue"','"cst": "Triangle"','"sss": "Spikey"'}
   ]}], L0Rec);

output(ds, named('OrigInline'));
output(ds,,'REGRESS::TEMP::inline_content_orig.xml', overwrite, xml);

readWrittenOrig := dataset(DYNAMIC('REGRESS::TEMP::inline_content_orig.xml'), L0Rec, xml('Dataset/Row'));
output(readWrittenOrig, named('readWrittenOrig'));

output(named_dsj, named('OrigInlineJson'));
output(named_dsj,,'REGRESS::TEMP::inline_content_orig.json', overwrite, json);

readWrittenOrigJson := dataset(DYNAMIC('REGRESS::TEMP::inline_content_orig.json'), L0Rec, json('/Row'));
output(readWrittenOrigJson, named('readWrittenOrigJson'));

inline1Rec := RECORD
   string color;
   string shape {xpath('Shape/<>')};
   string texture;
END;

L1Rec := RECORD
   DATASET(inline1Rec) peeps{xpath('L1/L2/<>')};
END;

x := PROJECT(ds, L1Rec);

output(x, named('NamedInline'));
output(x,,'REGRESS::TEMP::inline_content_named.xml', overwrite, xml);

readWrittenNamed := dataset(DYNAMIC('REGRESS::TEMP::inline_content_named.xml'), L1Rec, xml('Dataset/Row'));
output(readWrittenNamed, named('readWrittenNamed'));

xj := PROJECT(named_dsj, L1Rec);

output(xj, named('NamedInlineJson'));
output(xj,,'REGRESS::TEMP::inline_content_named.json', overwrite, json);

readWrittenNamedJson := dataset(DYNAMIC('REGRESS::TEMP::inline_content_named.json'), NestedShapeDS, json('/Row'));
output(readWrittenNamedJson, named('readWrittenNamedJson'));

inline2Rec := RECORD
   string color;
   string shape {xpath('Shape<>')};
   string texture;
END;

L2Rec := RECORD
   DATASET(inline2Rec) peeps{xpath('L1/L2<>')}; //should be ignored on output
END;

y := PROJECT(x, L2Rec);

output(y, named('UnnamedInline'));
output(y,,'REGRESS::TEMP::inline_content_unnamed.xml', overwrite, xml);

read2Rec := RECORD
   string color;
   string shape {xpath('cst')}; //was written unamed unencoded
   string texture;
END;

R2Rec := RECORD
   DATASET(read2Rec) peeps{xpath('L1/L2')};
END;

readWrittenUnnamed := dataset(DYNAMIC('REGRESS::TEMP::inline_content_unnamed.xml'), R2Rec, xml('Dataset/Row'));
output(readWrittenUnnamed, named('readWrittenUnnamed'));

yj := PROJECT(unnamed_dsj, L2Rec);

output(yj, named('UnnamedInlineJson'));
output(yj,,'REGRESS::TEMP::inline_content_unnamed.json', overwrite, json);

readWrittenUnnamedJson := dataset(DYNAMIC('REGRESS::TEMP::inline_content_unnamed.json'), R2Rec, json('/Row'));
output(readWrittenUnnamedJson, named('readWrittenUnnamedJson'));


inline3Rec := RECORD
   string color;
   string shape {xpath('<>')};
   string texture;
END;

L3Rec := RECORD
   DATASET(inline3Rec) peeps{xpath('L1/<>')}; //for dataset inline part should be ignored
END;

z := PROJECT(ds, L3Rec);

output(z, named('NonameInline'));
output(z,,'REGRESS::TEMP::inline_content_noname.xml', overwrite, xml);

read3Rec := RECORD
   string color;
   string shape {xpath('cst')}; //was written unamed unencoded
   string texture;
END;

R3Rec := RECORD
   DATASET(read3Rec) peeps{xpath('L1')};
END;


readWrittenNoname := dataset(DYNAMIC('REGRESS::TEMP::inline_content_noname.xml'), R3Rec, xml('Dataset/Row'));
output(readWrittenNoname, named('readWrittenNoname'));

zj := PROJECT(unnamed_dsj, L3Rec);

output(zj, named('NonameInlineJson'));
output(zj,,'REGRESS::TEMP::inline_content_noname.json', overwrite, json);

readWrittenNonameJson := dataset(DYNAMIC('REGRESS::TEMP::inline_content_noname.json'), R3Rec, json('/Row'));
output(readWrittenNonameJson, named('readWrittenNonameJson'));

L0SetRec := RECORD
   SET OF STRING shapes;
END;

set_ds := DATASET([{['<cst>Circle</cst>','<cst>Square</cst>','<cst>Triangle</cst>']}], L0SetRec);


output(set_ds, named('setOrigInline'));
output(set_ds,,'REGRESS::TEMP::inline_set_orig.xml', overwrite, xml);

setreadWrittenOrig := dataset(DYNAMIC('REGRESS::TEMP::inline_set_orig.xml'), L0SetRec, xml('Dataset/Row'));
output(setreadWrittenOrig, named('setreadWrittenOrig'));

set_dsj := DATASET([{['{"cst": "Circle"}','{"cst": "Square"}','{"cst": "Triangle"}']}], L0SetRec);

output(set_dsj, named('setOrigInlineJson'));
output(set_dsj,,'REGRESS::TEMP::inline_set_orig.json', overwrite, json);

setreadWrittenOrigJson := dataset(DYNAMIC('REGRESS::TEMP::inline_set_orig.json'), L0SetRec, json('/Row'));
output(setreadWrittenOrigJson, named('setreadWrittenOrigJson'));

L1SetRec := RECORD
   SET OF STRING shapes {XPATH('Shapes/Shape/<>')};
END;

set_x := PROJECT(set_ds, L1SetRec);


output(set_x, named('setNamedInline'));
output(set_x,,'REGRESS::TEMP::inline_set_named.xml', overwrite, xml);

L2SetRec := RECORD
   SET OF STRING shapes {XPATH('Shapes/Shape<>')};
END;

setreadWrittenNamed := dataset(DYNAMIC('REGRESS::TEMP::inline_set_named.xml'), RxDSRec, xml('Dataset/Row'));
output(setreadWrittenNamed, named('setreadWrittenNamed'));

set_xj := PROJECT(set_dsj, L1SetRec);

output(set_xj, named('setNamedInlineJson'));
output(set_xj,,'REGRESS::TEMP::inline_set_named.json', overwrite, json);

setreadWrittenNamedJson := dataset(DYNAMIC('REGRESS::TEMP::inline_set_named.json'), RxDSRec, json('/Row'));
output(setreadWrittenNamedJson, named('setreadWrittenNamedJson'));


set_y := PROJECT(set_x, L2SetRec);

output(set_y, named('setUnnamedInline'));
output(set_y,,'REGRESS::TEMP::inline_set_unnamed.xml', overwrite, xml);

R2SetRec := RECORD
   SET OF STRING shapes {XPATH('Shapes/cst')};
END;

setreadWrittenUnnamed := dataset(DYNAMIC('REGRESS::TEMP::inline_set_unnamed.xml'), R2SetRec, xml('Dataset/Row'));
output(setreadWrittenUnnamed, named('setreadWrittenUnnamed'));

set_yj := PROJECT(set_dsj, L2SetRec);

output(set_yj, named('setUnnamedInlineJson'));
output(set_yj,,'REGRESS::TEMP::inline_set_unnamed.json', overwrite, json);

setreadWrittenUnnamedJson := dataset(DYNAMIC('REGRESS::TEMP::inline_set_unnamed.json'), RxDSRec, json('/Row'));
output(setreadWrittenUnnamedJson, named('setreadWrittenUnnamedJson'));

L3SetRec := RECORD
   SET OF STRING shapes {XPATH('Shapes/<>')};
END;

set_z := PROJECT(set_x, L3SetRec);

output(set_z, named('setNonameInline'));
output(set_z,,'REGRESS::TEMP::inline_set_noname.xml', overwrite, xml);

setreadWrittenNoname := dataset(DYNAMIC('REGRESS::TEMP::inline_set_noname.xml'), R2SetRec, xml('Dataset/Row'));
output(setreadWrittenNoname, named('setreadWrittenNoname'));

set_zj := PROJECT(set_dsj, L3SetRec);

output(set_zj, named('setNonameInlineJson'));
output(set_zj,,'REGRESS::TEMP::inline_set_noname.json', overwrite, json);

setreadWrittenNonameJson := dataset(DYNAMIC('REGRESS::TEMP::inline_set_noname.json'), R2SetRec, json('/Row'));
output(setreadWrittenNonameJson, named('setreadWrittenNonameJson'));
