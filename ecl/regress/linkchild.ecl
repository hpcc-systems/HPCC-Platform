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
