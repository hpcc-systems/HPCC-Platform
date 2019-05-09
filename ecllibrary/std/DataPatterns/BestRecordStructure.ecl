/***
 * Function macro that leverages DataPatterns to return a string defining the
 * best ECL record structure for the input data.
 *
 * @param   inFile          The dataset to process; REQUIRED
 * @param   sampling        A positive integer representing a percentage of
 *                          inFile to examine, which is useful when analyzing a
 *                          very large dataset and only an estimatation is
 *                          sufficient; valid range for this argument is
 *                          1-100; values outside of this range will be
 *                          clamped; OPTIONAL, defaults to 100 (which indicates
 *                          that the entire dataset will be analyzed)
 * @param   emitTransform   Boolean governing whether the function emits a
 *                          TRANSFORM function that could be used to rewrite
 *                          the dataset into the 'best' record definition;
 *                          OPTIONAL, defaults to FALSE.
 * @param   textOutput      Boolean governing the type of result that is
 *                          delivered by this function; if FALSE then a
 *                          recordset of STRINGs will be returned; if TRUE
 *                          then a dataset with a single STRING field, with
 *                          the contents formatted for HTML, will be
 *                          returned (this is the ideal output if the
 *                          intention is to copy the output from ECL Watch);
 *                          OPTIONAL, defaults to FALSE
 *
 * @return  A recordset defining the best ECL record structure for the data.
 *          If textOutput is FALSE (the default) then each record will contain
 *          one field declaration, and the list of declarations will be wrapped
 *          with RECORD and END strings; if the emitTransform argument was
 *          TRUE, there will also be a set of records that that comprise a
 *          stand-alone TRANSFORM function.  If textOutput is TRUE then only
 *          one record will be returned, containing an HTML-formatted string
 *          containing the new field declarations (and optionally the
 *          TRANSFORM); this is the ideal format if the intention is to copy
 *          the result from ECL Watch.
 */
EXPORT BestRecordStructure(inFile, sampling = 100, emitTransform = FALSE, textOutput = FALSE) := FUNCTIONMACRO
    LOADXML('<xml/>');
    #EXPORTXML(bestInFileFields, RECORDOF(inFile));
    #UNIQUENAME(bestFieldStack);
    #UNIQUENAME(bestStructType);
    #UNIQUENAME(bestLayoutType);
    #UNIQUENAME(bestCapturedPos);
    #UNIQUENAME(bestPrevCapturedPos);
    #UNIQUENAME(bestLayoutName);
    #UNIQUENAME(bestNeedsDelim);
    #UNIQUENAME(bestNamePrefix);

    IMPORT Std;

    LOCAL OutRec := {STRING s};

    // Build a dataset containing information about embedded records and
    // child datasets; we need to track the beginning and ending positions
    // of the fields defined within those structures
    LOCAL ChildRecInfoLayout := RECORD
        STRING      layoutType;
        STRING      layoutName;
        STRING      fieldName;
        UNSIGNED2   startPos;
        UNSIGNED2   endPos;
    END;

    LOCAL childRecInfo := DATASET
        (
            [
                #SET(bestFieldStack, '')
                #SET(bestNeedsDelim, 0)
                #FOR(bestInFileFields)
                    #FOR(Field)
                        #IF(%{@isRecord}% = 1 OR %{@isDataset}% = 1)
                            #IF(%{@isRecord}% = 1)
                                #SET(bestStructType, 'r')
                            #ELSE
                                #SET(bestStructType, 'd')
                            #END
                            #IF(%'bestFieldStack'% != '')
                                #SET(bestFieldStack, ';' + %'bestFieldStack'%)
                            #END
                            #SET(bestFieldStack, %'bestStructType'% + ':' + %'@position'% + ':' + %'@ecltype'% + %'bestFieldStack'%)
                        #ELSEIF(%{@isEnd}% = 1)
                            #SET(bestLayoutType, %'bestFieldStack'%[1])
                            #SET(bestCapturedPos, REGEXFIND('.:(\\d+)', %'bestFieldStack'%, 1))
                            #SET(bestLayoutName, REGEXFIND('.:\\d+:([^;]+)', %'bestFieldStack'%, 1))
                            #SET(bestFieldStack, REGEXFIND('^[^;]+;(.*)', %'bestFieldStack'%, 1))

                            #IF(%bestNeedsDelim% = 1) , #END
                            {%'bestLayoutType'%, %'bestLayoutName'%, %'@name'%, %bestCapturedPos%, %bestPrevCapturedPos%}
                            #SET(bestNeedsDelim, 1)
                        #ELSE
                            #SET(bestPrevCapturedPos, %@position%)
                        #END
                    #END
                #END
            ],
            ChildRecInfoLayout
        );

    // Extract the original data type and position of the fields within the
    // input dataset
    LOCAL FieldInfoLayout := RECORD
        STRING      eclType;
        STRING      name;
        UNSIGNED2   position;
        STRING      fullName;
        BOOLEAN     isRecord;
        BOOLEAN     isDataset;
    END;

    LOCAL fieldInfo := DATASET
        (
            [
                #SET(bestFieldStack, '')
                #SET(bestNeedsDelim, 0)
                #SET(bestNamePrefix, '')
                #FOR(bestInFileFields)
                    #FOR(Field)
                        #IF(%@isEnd% != 1)
                            #IF(%bestNeedsDelim% = 1) , #END
                            {
                                %'@ecltype'%,
                                %'@name'%,
                                %@position%,
                                %'bestNamePrefix'% + %'@name'%,
                                #IF(%@isRecord% = 1) TRUE #ELSE FALSE #END,
                                #IF(%@isDataset% = 1) TRUE #ELSE FALSE #END
                            }
                            #SET(bestNeedsDelim, 1)
                        #END

                        #IF(%{@isRecord}% = 1 OR %{@isDataset}% = 1)
                            #APPEND(bestNamePrefix, %'@name'% + '.')
                        #ELSEIF(%{@isEnd}% = 1)
                            #SET(bestNamePrefix, REGEXREPLACE('\\w+\\.$', %'bestNamePrefix'%, ''))
                        #END
                    #END
                #END
            ],
            FieldInfoLayout
        );

    // Get the best data types from the Profile() function
    LOCAL patternRes := DataPatterns.Profile(inFile, features := 'best_ecl_types', sampleSize := sampling);

    // Append the derived 'best' data types to the field information we
    // already collected
    LOCAL fieldInfo2 := JOIN
        (
            fieldInfo,
            patternRes,
            LEFT.fullName = RIGHT.attribute,
            TRANSFORM
                (
                    {
                        RECORDOF(LEFT),
                        STRING      bestAttributeType
                    },
                    SELF.bestAttributeType := IF(RIGHT.best_attribute_type != '', Std.Str.ToUpperCase(RIGHT.best_attribute_type), LEFT.eclType),
                    SELF := LEFT
                ),
            LEFT OUTER
        );

    LOCAL ChildRecLayout := RECORD
        STRING              layoutName;
        UNSIGNED2           startPos;
        UNSIGNED2           endPos;
        DATASET(OutRec)     items;
    END;

    // Extract the named embedded child records; the fields within the records
    // will be converted to a string version of an ECL declaration; take care
    // to ensure that the result defines the named child records in reverse
    // order
    LOCAL namedChildRecs := DENORMALIZE
        (
            SORT(childRecInfo(layoutType = 'r' AND layoutName != '<unnamed>'), -startPos),
            fieldInfo2,
            RIGHT.position BETWEEN LEFT.startPos + 1 AND LEFT.endPos,
            GROUP,
            TRANSFORM
                (
                    ChildRecLayout,
                    SELF.items := DATASET([Std.Str.ToUpperCase(LEFT.layoutName) + ' := RECORD'], OutRec)
                        + PROJECT
                            (
                                SORT(ROWS(RIGHT), position),
                                TRANSFORM
                                    (
                                        OutRec,
                                        SELF.s := '    ' + Std.Str.ToUpperCase(LEFT.bestAttributeType) + ' ' + LEFT.name + ';'
                                    )
                            )
                        + DATASET(['END;'], OutRec),
                    SELF := LEFT
                ),
            ALL, ORDERED(TRUE)
        ) : ONWARNING(4531, IGNORE);

    // Remove the named child record items from the field list
    LOCAL fieldInfo3 := JOIN
        (
            fieldInfo2,
            namedChildrecs,
            LEFT.position BETWEEN RIGHT.startPos + 1 AND RIGHT.endPos,
            TRANSFORM(LEFT),
            LEFT ONLY, ALL
        ) : ONWARNING(4531, IGNORE);

    // Extract the anonymous embedded child records; the fields within the
    // records will be converted to a string version of an ECL declaration and
    // rolled up into a single string
    LOCAL anonChildRecs := DENORMALIZE
        (
            childRecInfo(layoutType = 'r' AND layoutName = '<unnamed>'),
            fieldInfo3,
            RIGHT.position BETWEEN LEFT.startPos + 1 AND LEFT.endPos,
            GROUP,
            TRANSFORM
                (
                    ChildRecLayout,
                    SELF.items := ROLLUP
                        (
                            PROJECT(SORT(ROWS(RIGHT), position), TRANSFORM(OutRec, SELF.s := Std.Str.ToUpperCase(LEFT.bestAttributeType) + ' ' + LEFT.name)),
                            TRUE,
                            TRANSFORM
                                (
                                    OutRec,
                                    SELF.s := LEFT.s + ', ' + RIGHT.s
                                )
                        ),
                    SELF := LEFT
                )
        ) : ONWARNING(4531, IGNORE);

    // Remove the anonymous child record items from the field list
    LOCAL fieldInfo4 := JOIN
        (
            fieldInfo3,
            anonChildRecs,
            LEFT.position BETWEEN RIGHT.startPos + 1 AND RIGHT.endPos,
            TRANSFORM(LEFT),
            LEFT ONLY, ALL
        ) : ONWARNING(4531, IGNORE);

    // Replace the best field type for anonymous child records with the
    // specific fields we built earlier, wrapping the entire thing in braces
    LOCAL fieldInfo5 := JOIN
        (
            fieldInfo4,
            anonChildRecs,
            LEFT.position = RIGHT.startPos,
            TRANSFORM
                (
                    RECORDOF(LEFT),
                    SELF.bestAttributeType := IF(RIGHT.items[1].s != '', '{' + RIGHT.items[1].s + '}', LEFT.bestAttributeType),
                    SELF := LEFT
                ),
            LEFT OUTER
        ) : ONWARNING(4531, IGNORE);

    // Remove, for now, child datasets; they are not supported by the profiler
    // anyway
    LOCAL fieldInfo6 := fieldInfo5(NOT isDataset);

    // Create the top-level record definition
    LOCAL topLevel := DATASET(['NewLayout := RECORD'], OutRec)
        & PROJECT
            (
                SORT(fieldInfo6, position),
                TRANSFORM
                    (
                        OutRec,
                        SELF.s := '    ' + IF(Std.Str.Contains(LEFT.bestAttributeType, '{', FALSE), LEFT.bestAttributeType, Std.Str.ToUpperCase(LEFT.bestAttributeType)) + ' ' + LEFT.name + ';'
                    )
            )
        & DATASET(['END;'], OutRec);

    // Final output includes the named child records and the top-level record
    // definition we just built
    LOCAL layoutRes := namedChildRecs.items + topLevel;

    // Helper function for determining if old and new data types need
    // explicit type casting
    LOCAL NeedCoercion(STRING oldType, STRING newType) := FUNCTION
        GenericType(STRING theType) := MAP
            (
                theType[..6] = 'string'                 =>  'string',
                theType[..13] = 'EBCDIC string'         =>  'string',
                theType[..7] = 'qstring'                =>  'string',
                theType[..9] = 'varstring'              =>  'string',
                theType[..3] = 'utf'                    =>  'string',
                theType[..7] = 'unicode'                =>  'string',
                theType[..10] = 'varunicode'            =>  'string',
                theType[..4] = 'data'                   =>  'data',
                theType[..7] = 'boolean'                =>  'boolean',
                theType[..7] = 'integer'                =>  'numeric',
                theType[..18] = 'big_endian integer'    =>  'numeric',
                theType[..4] = 'real'                   =>  'numeric',
                theType[..7] = 'decimal'                =>  'numeric',
                theType[..8] = 'udecimal'               =>  'numeric',
                theType[..8] = 'unsigned'               =>  'numeric',
                theType[..19] = 'big_endian unsigned'   =>  'numeric',
                theType
            );

        oldGenericType := GenericType(oldType);
        newGenericType := GenericType(newType);

        RETURN oldGenericType != newGenericType;
    END;

    // Subset of fields that need explicit type casting
    LOCAL differentTypes := patternRes(NeedCoercion(given_attribute_type, best_attribute_type));

    // Explicit type casting statements
    LOCAL coercedTransformStatements := PROJECT
        (
            differentTypes,
            TRANSFORM
                (
                    OutRec,
                    SELF.s := '    SELF.' + LEFT.attribute + ' := (' + Std.Str.ToUppercase(LEFT.best_attribute_type) + ')r.' + LEFT.attribute + ';';
                )
        );

    // Final transform step, if needed
    LOCAL coercedTransformStatementSet := IF
        (
            COUNT(patternRes) != COUNT(differentTypes),
            SET(coercedTransformStatements, s) + ['    SELF := r;'],
            SET(coercedTransformStatements, s)
        );

    // Final transform function
    LOCAL transformSet := IF
        (
            (BOOLEAN)emitTransform,
            ['//----------', 'NewLayout MakeNewLayout(OldLayout r) := TRANSFORM'] + coercedTransformStatementSet + ['END;'],
            []
        );

    // Combine optional transform
    LOCAL allOutput := layoutRes & DATASET(transformSet, OutRec);

    // Roll everything up to one string with HTML line breaks
    LOCAL htmlString := ROLLUP
        (
            allOutput,
            TRUE,
            TRANSFORM
                (
                    RECORDOF(LEFT),
                    SELF.s := LEFT.s + '<br/>' + RIGHT.s
                )
        );

    // Stuff the HTML result into a single record, wrapped with <pre> so it
    // looks right in the browser
    LOCAL htmlResult := DATASET(['<pre>' + htmlString[1].s + '</pre>'], {STRING result__html});

    // Choose the result (dataset with each line a string, or a text blob)
    LOCAL finalResult := #IF((BOOLEAN)textOutput) htmlResult #ELSE allOutput #END;

    RETURN finalResult;
ENDMACRO;
