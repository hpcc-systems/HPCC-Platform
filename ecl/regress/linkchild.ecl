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

#option ('targetClusterType', 'roxie');
#option ('checkRoxieRestrictions', false);
#option ('_Probe', true);

childRecord := RECORD
    unsigned4 kwp;
    unsigned2 wip;
END;

unlinkedRecord := RECORD
    unsigned8 document;
    dataset(childRecord) children{maxcount(200), embedded};
END;

linkedRecord := RECORD
    unsigned8 document;
    dataset(childRecord) children{maxcount(200), _linkCounted_};
END;

//Can't tell the maxlength for this record, so require serialization
xlinkedRecord := RECORD
    unsigned8 document;
    dataset(childRecord) children{_linkCounted_};
END;

unlinkedDataset := DATASET('unlinked', unlinkedRecord, thor);
linkedDataset := DATASET('linked', linkedRecord, thor);
xlinkedDataset := DATASET('xlinked', xlinkedRecord, thor);

TransformAssignments1() := macro
    self.document := IF(left.document <> 0, left.document+1, skip);
    self.children := left.children;
endmacro;

TransformAssignments2() := macro
    self.document := left.document+1;
    self.children := (left.children + left.children);
endmacro;

TransformAssignments3() := macro
    self.document := left.document+1;
    self.children := (left.children + left.children)(kwp < 999999);
endmacro;

TransformAssignments4() := macro
    self.document := left.document+1;
    self.children := sort(left.children, kwp, -wip);
endmacro;

doTests(transformToApply) := macro
#uniquename (transformName)
%transformName% := transformToApply[1..length(transformToApply)-2];
sequential(
    output(project(nofold(unlinkedDataset), transform(unlinkedRecord, #expand(transformToApply))),,named('u2u_'+%transformName%)),
    output(project(nofold(unlinkedDataset), transform(linkedRecord, #expand(transformToApply))),,named('u2l_'+%transformName%)),
    output(project(nofold(linkedDataset), transform(unlinkedRecord, #expand(transformToApply))),,named('l2u_'+%transformName%)),
    output(project(nofold(linkedDataset), transform(linkedRecord, #expand(transformToApply))),,named('l2l_'+%transformName%)),
    output(project(nofold(xlinkedDataset), transform(unlinkedRecord, #expand(transformToApply))),,named('x2u_'+%transformName%)),
    output(project(nofold(xlinkedDataset), transform(linkedRecord, #expand(transformToApply))),,named('x2l_'+%transformName%)),
    output(project(nofold(unlinkedDataset), transform(unlinkedRecord, #expand(transformToApply))),,'~u2u_'+%transformName%),
    output(project(nofold(unlinkedDataset), transform(linkedRecord, #expand(transformToApply))),,'~u2l_'+%transformName%),
    output(project(nofold(linkedDataset), transform(unlinkedRecord, #expand(transformToApply))),,'~l2u_'+%transformName%),
    output(project(nofold(linkedDataset), transform(linkedRecord, #expand(transformToApply))),,'~l2l_'+%transformName%),
    output(project(nofold(xlinkedDataset), transform(unlinkedRecord, #expand(transformToApply))),,'~x2u_'+%transformName%),
    output(project(nofold(xlinkedDataset), transform(linkedRecord, #expand(transformToApply))),,'~x2l_'+%transformName%),
    output('done')
);

endmacro;


doTests('TransformAssignments1()');
doTests('TransformAssignments2()');
doTests('TransformAssignments3()');
doTests('TransformAssignments4()');
