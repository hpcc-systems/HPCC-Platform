/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "jliball.hpp"

#include "hqlexpr.hpp"
#include "fvwugen.ipp"
#include "fvsource.ipp"

/*
The format of the generated query is as follows:

unsigned8 startPos := 0 : stored('__startPos__');
unsigned4 numRecords := 100 : stored('__numRecords__');

src := dataset(..., rec);                   //Need to add holepos or filepos to structure

filter := src(curPos >= startPos);          //curPos == holepos or filepos depending on source.

limited := choosen(filter, numRecords);

simple := table(src, {simplify-record});

output(simple);

  */

IIdAtom * fileposId;
IIdAtom * recordlenName;
IAtom * insertedAtom;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    fileposId = createIdAtom("__filepos__");
    recordlenName = createIdAtom("__recordlen__");
    insertedAtom = createLowerCaseAtom("inserted");
    return true;
}

IHqlExpression * addFilter(IHqlExpression * dataset, IHqlExpression * limitField)
{
    IHqlExpression * lower = createConstant(limitField->queryType()->castFrom(true, (__int64)0));
    lower = createValue(no_colon, lower, createValue(no_stored, createConstant(LOWER_LIMIT_ID)));
    lower = createSymbol(createIdAtom(LOWER_LIMIT_ID), lower, ob_private);
    dataset = createDataset(no_filter, LINK(dataset), createBoolExpr(no_ge, LINK(limitField), lower));

    IHqlExpression * upper = createConstant((int)DISKREAD_PAGE_SIZE);
    upper = createValue(no_colon, upper, createValue(no_stored, createConstant(RECORD_LIMIT_ID)));
    upper = createSymbol(createIdAtom(RECORD_LIMIT_ID), upper, ob_private);
    dataset = createDataset(no_choosen, dataset, upper);
    dataset = createSymbol(createIdAtom("_Filtered_"), dataset, ob_private);
    return dataset;
}

IHqlExpression * addOutput(IHqlExpression * dataset)
{
    return createValue(no_output, makeVoidType(), LINK(dataset));
}


IHqlExpression * addSimplifyProject(IHqlExpression * dataset)
{
    IHqlExpression * record = dataset->queryRecord();
    IHqlExpression * projectRecord = getFileViewerRecord(record, false);
    if (!projectRecord)
        return LINK(dataset);

    projectRecord = createSymbol(createIdAtom("_TargetRecord_"), projectRecord, ob_private);
    return createDataset(no_newusertable, LINK(dataset), createComma(projectRecord, getSimplifiedTransform(projectRecord, record, dataset)));
}




IHqlExpression * buildWorkUnitViewerEcl(IHqlExpression * record, const char * wuid, unsigned sequence, const char * name)
{
    OwnedHqlExpr newRecord = createSymbol(createIdAtom("_SourceRecord_"), LINK(record), ob_private);
    IHqlExpression * arg = name ? createConstant(name) : createConstant((int)sequence);
    OwnedHqlExpr dataset = createDataset(no_workunit_dataset, newRecord.getLink(), createComma(createConstant(wuid), arg));
    OwnedHqlExpr projected = addSimplifyProject(dataset);
    OwnedHqlExpr output = addOutput(projected);
    return output.getClear();
}


IHqlExpression * buildDiskFileViewerEcl(const char * logicalName, IHqlExpression * record)
{
    //Add filepos to the incomming record structure...
    IHqlExpression * filePosAttr = createAttribute(virtualAtom, createAttribute(filepositionAtom));
    OwnedHqlExpr filepos = createField(fileposId, makeIntType(8, false), NULL, filePosAttr);
    IHqlExpression * sizeofAttr = createAttribute(virtualAtom, createAttribute(sizeofAtom));
    OwnedHqlExpr reclen = createField(recordlenName, makeIntType(2, false), NULL, sizeofAttr);
    HqlExprArray fields;
    unwindChildren(fields, record);
    fields.append(*filepos.getLink());
    fields.append(*reclen.getLink());

    OwnedHqlExpr newRecord = createRecord(fields);
    newRecord.setown(createSymbol(createIdAtom("_SourceRecord_"), newRecord.getLink(), ob_private));

    OwnedHqlExpr dataset = createNewDataset(createConstant(logicalName), newRecord.getLink(), createValue(no_thor), NULL, NULL, NULL);
    OwnedHqlExpr filtered = addFilter(dataset, filepos);
    OwnedHqlExpr projected = addSimplifyProject(filtered);
    OwnedHqlExpr output = addOutput(projected);
    return output.getClear();
}

//---------------------------------------------------------------------------

IHqlExpression * buildDiskOutputEcl(const char * logicalName, IHqlExpression * record)
{
    OwnedHqlExpr dataset = createNewDataset(createConstant(logicalName), LINK(record), createValue(no_thor), NULL, NULL, NULL);
    return addOutput(dataset);
}

//---------------------------------------------------------------------------

//Add holepos/filepos/sizeof to the query, so that the browse has something to work on.
static HqlTransformerInfo positionTransformerInfo("PositionTransformer");
PositionTransformer::PositionTransformer()  : NewHqlTransformer(positionTransformerInfo)
{ 
    insertedAttr.setown(createAttribute(insertedAtom));
}

IHqlExpression * PositionTransformer::createTransformed(IHqlExpression * _expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(_expr);

    switch (transformed->getOperator())
    {
    case no_table:
        {
            IHqlExpression * mode = transformed->queryChild(2);
            HqlExprArray fields;
            HqlExprArray args;

            if (mode->getOperator() == no_thor)
            {
                unwindChildren(fields, transformed->queryChild(1));
                IHqlExpression * filePosAttr = createComma(createAttribute(virtualAtom, createAttribute(filepositionAtom)), insertedAttr.getLink());
                IHqlExpression * sizeofAttr = createComma(createAttribute(virtualAtom, createAttribute(sizeofAtom)), insertedAttr.getLink());
                fields.append(*createField(fileposId, makeIntType(8, false), NULL, filePosAttr));
                fields.append(*createField(recordlenName, makeIntType(2, false), NULL, sizeofAttr));

                unwindChildren(args, transformed);
                args.replace(*createRecord(fields), 1);
                return transformed->clone(args);
            }
        }
        break;
    case no_iterate:
    case no_hqlproject:
        {
            HqlExprArray args;
            HqlExprArray assigns;
            IHqlExpression * transform = transformed->queryChild(1);
            unwindChildren(args, transformed);
            unwindChildren(assigns, transform);
            IHqlExpression * inRecord = transformed->queryChild(0)->queryRecord();
            IHqlExpression * outRecord = transform->queryRecord();

            HqlExprArray fields;
            unwindChildren(fields, outRecord);
            ForEachChild(idx, inRecord)
            {
                IHqlExpression * child = inRecord->queryChild(idx);
                if (child->hasAttribute(insertedAtom))
                {
                    IHqlExpression * newTarget = createField(child->queryId(), child->getType(), LINK(child), insertedAttr.getLink());
                    fields.append(*newTarget);
                    assigns.append(*createValue(no_assign, makeVoidType(), newTarget, createSelectExpr(createValue(no_left), LINK(newTarget))));
                }
            }
            IHqlExpression * newRecord = createRecord(fields);
            args.replace(*createValue(no_transform, newRecord->getType(), assigns), 1);
            return transformed->clone(args);
        }
        break;
    case no_join:
        //only ok if join first
    case no_rollup:
    case no_newaggregate:
    case no_aggregate:
        fail();
        break;
    case no_usertable:
    case no_selectfields:
        {
            IHqlExpression * grouping = transformed->queryChild(2);
            if (grouping && (grouping->getOperator() != no_attr))
                fail();
            IHqlExpression * record = transformed->queryRecord();
            HqlExprArray fields;
            unwindChildren(fields, transformed->queryChild(1));
            ForEachChild(idx, record)
            {
                IHqlExpression * child = record->queryChild(idx);
                if (child->hasAttribute(insertedAtom))
                    fields.append(*createField(child->queryId(), child->getType(), LINK(child), insertedAttr.getLink()));
            }

            HqlExprArray args;
            unwindChildren(args, transformed);
            args.replace(*createRecord(fields), 1);
            return transformed->clone(args);
        }
    case no_output:
        {
            IHqlExpression * file = transformed->queryChild(2);
            if (file && (file->getOperator() != no_attr))
            {
                IHqlExpression * child = transformed->queryChild(0);
                assertex(child->getOperator() == no_selectfields);
                HqlExprArray args;
                unwindChildren(args, child);

                HqlExprArray fields;
                IHqlExpression * record = child->queryChild(1);
                if (record->getOperator() == no_null)
                {
                    //MORE: This might will not work for ifblocks, and may not cope with 
                    //      alien(self.x), or nested records.
                    IHqlExpression * record = child->queryRecord();
                    ForEachChild(idx, record)
                    {
                        IHqlExpression * child = record->queryChild(idx);
                        if (!child->hasAttribute(insertedAtom))
                            fields.append(*createField(child->queryId(), child->getType(), LINK(child)));
                    }
                }
                else
                {
                    ForEachChild(idx, record)
                    {
                        IHqlExpression * child = record->queryChild(idx);
                        if (!child->hasAttribute(insertedAtom))
                            fields.append(*LINK(child));
                    }
                }

                args.replace(*createRecord(fields), 1);
                IHqlExpression * dataset = createRecord(args);

                args.kill();
                unwindChildren(args, transformed);
                args.replace(*dataset, 0);
                return transformed->clone(args);
            }
        }
        break;

    default:
        if (definesColumnList(transformed))
            throw 2;
        break;
    }


    return transformed.getClear();
}


void PositionTransformer::fail()
{
    throw 1;
}


IHqlExpression * addQueryPositionFields(IHqlExpression * selectFields)
{
    PositionTransformer transformer;
    try
    {
        return transformer.transformRoot(selectFields);
    }
    catch (int)
    {
        return NULL;
    }
}


IHqlExpression * buildQueryViewerEcl(IHqlExpression * selectFields)
{
    OwnedHqlExpr transformed = addQueryPositionFields(selectFields);
    if (!transformed)
        return NULL;
    IHqlSimpleScope * scope = transformed->queryRecord()->querySimpleScope();
    OwnedHqlExpr filterField = scope->lookupSymbol(fileposId);
    OwnedHqlExpr filtered = addFilter(transformed, filterField);
    OwnedHqlExpr output = addOutput(filtered);
    return output.getClear();
}

