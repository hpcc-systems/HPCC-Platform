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
    #UNIQUENAME(recLevel);

    IMPORT DataPatterns;
    IMPORT Std;

    // Attribute naming note:  In order to reduce symbol collisions with calling
    // code, all LOCAL attributes are prefixed with two underscore characters;
    // normally, a #UNIQUENAME would be used instead, but there is apparently
    // a problem with using that for ECL attributes when another function
    // macro is called (namely, Profile); using double underscores is not an
    // optimal solution but the chance of symbol collision should at least be
    // reduced

    LOCAL __DATAREC_NAME := 'DataRec';
    LOCAL __LAYOUT_NAME := 'Layout';

    LOCAL __StringRec := {STRING s};

    // Helper function for determining if old and new data types need
    // explicit type casting
    LOCAL __NeedCoercion(STRING oldType, STRING newType) := FUNCTION
        GenericType(STRING theType) := MAP
            (
                theType[..6] = 'string'                 =>  'string',
                theType[..13] = 'ebcdic string'         =>  'string',
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

        oldGenericType := GenericType(Std.Str.ToLowerCase(oldType));
        newGenericType := GenericType(Std.Str.ToLowerCase(newType));

        RETURN oldGenericType != newGenericType;
    END;

    // Build a dataset containing information about embedded records and
    // child datasets; we need to track the beginning and ending positions
    // of the fields defined within those structures
    LOCAL __ChildRecInfoLayout := RECORD
        STRING      layoutType;
        STRING      layoutName;
        STRING      fieldName;
        UNSIGNED2   startPos;
        UNSIGNED2   endPos;
    END;

    LOCAL __childRecInfo := DATASET
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

                            {
                                %'bestLayoutType'%,
                                %'bestLayoutName'%,
                                %'@name'%,
                                %bestCapturedPos%,
                                %bestPrevCapturedPos%
                            }

                            #SET(bestNeedsDelim, 1)
                        #ELSE
                            #SET(bestPrevCapturedPos, %@position%)
                        #END
                    #END
                #END
            ],
            __ChildRecInfoLayout
        );

    // Extract the original data type and position of the fields within the
    // input dataset
    LOCAL __FieldInfoLayout := RECORD
        STRING      eclType;
        STRING      name;
        STRING      fullName;
        BOOLEAN     isRecord;
        BOOLEAN     isDataset;
        UNSIGNED2   depth;
        UNSIGNED2   position;
    END;

    LOCAL __fieldInfo0 := DATASET
        (
            [
                #SET(bestFieldStack, '')
                #SET(bestNeedsDelim, 0)
                #SET(bestNamePrefix, '')
                #SET(recLevel, 0)
                #FOR(bestInFileFields)
                    #FOR(Field)
                        #IF(%@isEnd% != 1)
                            #IF(%bestNeedsDelim% = 1) , #END

                            {
                                %'@ecltype'%,
                                %'@name'%,
                                %'bestNamePrefix'% + %'@name'%,
                                #IF(%@isRecord% = 1) TRUE #ELSE FALSE #END,
                                #IF(%@isDataset% = 1) TRUE #ELSE FALSE #END,
                                %recLevel%,
                                %@position%
                            }

                            #SET(bestNeedsDelim, 1)
                        #END

                        #IF(%{@isRecord}% = 1 OR %{@isDataset}% = 1)
                            #APPEND(bestNamePrefix, %'@name'% + '.')
                            #SET(recLevel, %recLevel% + 1)
                        #ELSEIF(%{@isEnd}% = 1)
                            #SET(bestNamePrefix, REGEXREPLACE('\\w+\\.$', %'bestNamePrefix'%, ''))
                            #SET(recLevel, %recLevel% - 1)
                        #END
                    #END
                #END
            ],
            __FieldInfoLayout
        );

    // Attach the record end positions for embedded records and child datasets
    LOCAL __fieldInfo10 := JOIN
        (
            __fieldInfo0,
            __childRecInfo,
            LEFT.name = RIGHT.fieldName AND LEFT.position = RIGHT.startPos,
            TRANSFORM
                (
                    {
                        RECORDOF(LEFT),
                        UNSIGNED2   endPosition
                    },
                    SELF.endPosition := RIGHT.endPos,
                    SELF := LEFT
                ),
            LEFT OUTER
        );

    // Get the best data types from the Profile() function
    LOCAL __patternRes := DataPatterns.Profile(inFile, features := 'best_ecl_types', sampleSize := sampling);

    // Append the derived 'best' data types to the field information we
    // already collected
    LOCAL __fieldInfo15 := JOIN
        (
            __fieldInfo10,
            __patternRes,
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

    // Determine fields that must have explicit coercion if we are supplying
    // transform information
    LOCAL __fieldInfo20 := PROJECT
        (
            __fieldInfo15,
            TRANSFORM
                (
                    {
                        RECORDOF(LEFT),
                        STRING      bestAssignment
                    },
                    shouldRewriteType := ((LEFT.isDataset OR LEFT.isRecord) AND LEFT.bestAttributeType IN ['<unnamed>', 'table of <unnamed>']);
                    tempDSName := __DATAREC_NAME + '_' + INTFORMAT(COUNTER, 4, 1);
                    SELF.eclType := IF(NOT shouldRewriteType, Std.Str.ToUpperCase(LEFT.eclType), tempDSName),
                    SELF.bestAttributeType := IF(NOT shouldRewriteType, LEFT.bestAttributeType, tempDSName),
                    SELF.bestAssignment := IF
                        (
                            __NeedCoercion(SELF.eclType, SELF.bestAttributeType),
                            '    SELF.' + LEFT.name + ' := (' + Std.Str.ToUppercase(SELF.bestAttributeType) + ')r.' + LEFT.name + ';',
                            ''
                        ),
                    SELF := LEFT
                )
        );

    LOCAL __LayoutItems := RECORD(__StringRec)
        STRING                      fullName {DEFAULT('')};
        STRING                      bestAssignment {DEFAULT('')};
    END;

    LOCAL __ChildRecLayout := RECORD
        STRING                      layoutName;
        UNSIGNED2                   startPos;
        UNSIGNED2                   endPos;
        UNSIGNED2                   depth;
        DATASET(__LayoutItems)      items;
    END;

    // Function for creating ECL TRANSFORM assignment statements
    LOCAL __MakeRecDefinition(DATASET(RECORDOF(__fieldInfo20)) ds, STRING layoutName, BOOLEAN useBest = TRUE) := FUNCTION
        displayPrefix := IF(useBest, 'New', 'Old');
        displayedLayoutName := displayPrefix + layoutName;
        RETURN DATASET([{displayedLayoutName + ' := RECORD'}], __LayoutItems)
            & PROJECT
                (
                    DISTRIBUTE(SORT(ds, position), 0),
                    TRANSFORM
                        (
                            __LayoutItems,
                            attrType := IF(useBest, LEFT.bestAttributeType, LEFT.eclType);
                            attrPrefix := IF(LEFT.isDataset OR LEFT.isRecord, displayPrefix, '');
                            fullAttrType := attrPrefix + attrType;
                            namedDataType := IF(NOT LEFT.isDataset, fullAttrType, 'DATASET(' + fullAttrType + ')');
                            SELF.s := '    ' + namedDataType + ' ' + LEFT.name + ';',
                            SELF.bestAssignment := MAP
                                (
                                    LEFT.bestAssignment != ''   =>  LEFT.bestAssignment,
                                    LEFT.isRecord   =>  '    SELF.' + LEFT.name + ' := ROW(Make_' + fullAttrType + '(r.' + LEFT.name + '));',
                                    LEFT.isDataset  =>  '    SELF.' + LEFT.name + ' := PROJECT(r.' + LEFT.name + ', Make_' + fullAttrType + '(LEFT));',
                                    ''
                                ),
                            SELF := LEFT
                        )
                )
            & DATASET([{'END;'}], __LayoutItems);
    END;

    // Iteratively process embedded records and child dataset definitions,
    // extracting each into its own record
    LOCAL __ProcessChildRecs(DATASET(__ChildRecLayout) layoutDS, UNSIGNED2 aDepth, BOOLEAN useBest = TRUE) := FUNCTION
        __bestNamedChildRecs := DENORMALIZE
            (
                __fieldInfo20(depth = (aDepth - 1) AND (isRecord OR isDataset)),
                __fieldInfo20(depth = aDepth),
                RIGHT.position BETWEEN LEFT.position + 1 AND LEFT.endPosition,
                GROUP,
                TRANSFORM
                    (
                        __ChildRecLayout,
                        SELF.layoutName := LEFT.bestAttributeType,
                        SELF.items := __MakeRecDefinition(ROWS(RIGHT), SELF.layoutName, useBest),
                        SELF.startPos := LEFT.position,
                        SELF.endPos := LEFT.endPosition,
                        SELF.depth := aDepth,
                        SELF := LEFT
                    ),
                ALL, ORDERED(TRUE)
            ) : ONWARNING(4531, IGNORE);

        RETURN layoutDS + __bestNamedChildRecs;
    END;

    // Create a list of embedded records and child dataset definitions for the
    // original input dataset
    LOCAL __oldNamedChildRecs0 := LOOP
        (
            DATASET([], __ChildRecLayout),
            MAX(__fieldInfo20, depth),
            __ProcessChildRecs(ROWS(LEFT), MAX(__fieldInfo20, depth) + 1 - COUNTER, FALSE)
        );

    LOCAL __oldNamedChildRecs := SORT(__oldNamedChildRecs0, endPos, -startPos);

    LOCAL __topLevelOldRecDef := DATASET
        (
            [
                {
                    __LAYOUT_NAME,
                    0,
                    0,
                    0,
                    __MakeRecDefinition(__fieldInfo20(depth = 0), __LAYOUT_NAME, FALSE)
                }
            ],
            __ChildRecLayout
        );

    LOCAL __allOldRecDefs := __oldNamedChildRecs & __topLevelOldRecDef;

    // Create a list of embedded records and child dataset definitions using the
    // the recommended ECL datatypes
    LOCAL __bestNamedChildRecs0 := LOOP
        (
            DATASET([], __ChildRecLayout),
            MAX(__fieldInfo20, depth),
            __ProcessChildRecs(ROWS(LEFT), MAX(__fieldInfo20, depth) + 1 - COUNTER, TRUE)
        );

    LOCAL __bestNamedChildRecs := SORT(__bestNamedChildRecs0, endPos, -startPos);

    LOCAL __topLevelBestRecDef := DATASET
        (
            [
                {
                    __LAYOUT_NAME,
                    0,
                    0,
                    0,
                    __MakeRecDefinition(__fieldInfo20(depth = 0), __LAYOUT_NAME, TRUE)
                }
            ],
            __ChildRecLayout
        );

    LOCAL __allBestRecDefs := __bestNamedChildRecs & __topLevelBestRecDef;

    // Creates an ECL TRANSFORM function based on the collected information
    // about a record definition
    LOCAL __MakeTransforms(__ChildRecLayout recInfo) := FUNCTION
        RETURN DATASET(['New' + recInfo.layoutName + ' Make_New' + recInfo.layoutName + '(Old' + recInfo.layoutName + ' r) := TRANSFORM'], __StringRec)
            & PROJECT
                (
                    DISTRIBUTE(recInfo.items, 0),
                    TRANSFORM
                        (
                            __StringRec,
                            assignment := LEFT.bestAssignment;
                            SELF.s := IF(assignment != '', assignment, SKIP)
                        )
                )
            & DATASET(['    SELF := r;'], __StringRec)
            & DATASET(['END;'], __StringRec);
    END;

    LOCAL __allTransforms := PROJECT
        (
            __allBestRecDefs,
            TRANSFORM
                (
                    {
                        DATASET(__StringRec)  lines
                    },
                    SELF.lines := __MakeTransforms(LEFT)
                )
        );

    // Create a dataset of STRINGS that contain record definitions for the
    // input dataset, TRANSFORMs for converting between the old and new
    // definitions, and a sample PROJECT for kicking it all off
    LOCAL __conditionalBR := #IF((BOOLEAN)textOutput) '<br/>' #ELSE '' #END;

    LOCAL __oldRecDefsPlusTransforms := DATASET(['//----------' + __conditionalBR], __StringRec)
        & PROJECT(__allOldRecDefs.items, __StringRec)
        & DATASET(['//----------' + __conditionalBR], __StringRec)
        & __allTransforms.lines
        & DATASET(['//----------' + __conditionalBR], __StringRec)
        & DATASET(['oldDS := DATASET([], OldLayout);' + __conditionalBR], __StringRec)
        & DATASET(['newDS := PROJECT(oldDS, Make_NewLayout(LEFT));' + __conditionalBR], __StringRec);

    // Combine old definitions and transforms conditionally
    LOCAL __conditionalOldStuff :=
        #IF((BOOLEAN)emitTransform)
            __oldRecDefsPlusTransforms
        #ELSE
            DATASET([], __StringRec)
        #END;

    LOCAL __allOutput := PROJECT(__allBestRecDefs.items, __StringRec) & __conditionalOldStuff;

    // Roll everything up to one string with HTML line breaks
    LOCAL __htmlString := ROLLUP
        (
            __allOutput,
            TRUE,
            TRANSFORM
                (
                    RECORDOF(LEFT),
                    rightString := IF(RIGHT.s = 'END;', RIGHT.s + '<br/>', RIGHT.s);
                    SELF.s := LEFT.s + '<br/>' + rightString
                )
        );

    // Stuff the HTML result into a single record, wrapped with <pre> so it
    // looks right in the browser
    LOCAL __htmlResult := DATASET(['<pre>' + __htmlString[1].s + '</pre>'], {STRING result__html});

    // Choose the result (dataset with each line a string, or a text blob)
    LOCAL __finalResult := #IF((BOOLEAN)textOutput) __htmlResult #ELSE __allOutput #END;

    RETURN __finalResult;
ENDMACRO;
