IMPORT Std;

ParentMod := MODULE, VIRTUAL

    SHARED DEFAULT_GENERATION_CNT := 3; // Update Init() docs if changed
    SHARED MIN_GENERATION_CNT := 2;
    SHARED SUPERFILE_SUFFIX := 'gen_';
    SHARED SUBFILE_SUFFIX := 'file_';

    SHARED _BuildSuperfilePathPrefix(STRING parent) := Std.Str.ToLowerCase(parent) + '::' + SUPERFILE_SUFFIX;

    SHARED _BuildSuperfilePath(STRING parent, UNSIGNED1 generationNum) := _BuildSuperfilePathPrefix(parent) + generationNum;

    SHARED _CreateSuperfilePathDS(STRING parent, UNSIGNED1 numGenerations) := DATASET
        (
            numGenerations,
            TRANSFORM
                (
                    {
                        UNSIGNED1   n, // Generation number
                        STRING      f  // Superfile path
                    },
                    SELF.n := COUNTER,
                    SELF.f := _BuildSuperfilePath(parent, COUNTER)
                )
        );

    EXPORT _NumGenerationsAvailable(STRING dataStorePath) := FUNCTION
        generationPattern := _BuildSuperfilePathPrefix(REGEXREPLACE('^~', dataStorePath, '')) + '*';
        foundGenerationPaths := NOTHOR(Std.File.LogicalFileList(generationPattern, FALSE, TRUE));
        expectedPaths := _CreateSuperfilePathDS(REGEXREPLACE('^~', dataStorePath, ''), COUNT(foundGenerationPaths));
        joinedPaths := JOIN(foundGenerationPaths, expectedPaths, LEFT.name = RIGHT.f);
        numJoinedPaths := COUNT(joinedPaths) : INDEPENDENT;
        numExpectedPaths := COUNT(expectedPaths) : INDEPENDENT;
        isSame := numJoinedPaths = numExpectedPaths;
        isNumGenerationsValid := numJoinedPaths >= MIN_GENERATION_CNT;

        RETURN WHEN(numJoinedPaths, ASSERT(isSame AND isNumGenerationsValid, 'Invalid structure: Unexpected superfile structure found for ' + dataStorePath, FAIL));
    END;

    //-----------------------------

    EXPORT Init(STRING dataStorePath, UNSIGNED1 numGenerations = DEFAULT_GENERATION_CNT) := FUNCTION
        clampedGenerations := MAX(MIN_GENERATION_CNT, numGenerations);
        generationPaths := _CreateSuperfilePathDS(dataStorePath, clampedGenerations);
        createParentAction := Std.File.CreateSuperFile(dataStorePath);
        createGenerationsAction := NOTHOR(APPLY(generationPaths, Std.File.CreateSuperFile(f)));
        appendGenerationsAction := NOTHOR(APPLY(generationPaths, Std.File.AddSuperFile(dataStorePath, f)));

        RETURN ORDERED
            (
                createParentAction;
                createGenerationsAction;
                Std.File.StartSuperFileTransaction();
                appendGenerationsAction;
                Std.File.FinishSuperFileTransaction();
            );
    END;

    EXPORT NumGenerationsAvailable(STRING dataStorePath) := FUNCTION
        numGens := _NumGenerationsAvailable(dataStorePath) : INDEPENDENT;

        RETURN numGens;
    END;

    EXPORT NumGenerationsInUse(STRING dataStorePath) := FUNCTION
        numPartitions := NumGenerationsAvailable(dataStorePath);
        generationPaths := _CreateSuperfilePathDS(dataStorePath, numPartitions);
        generationsUsed := NOTHOR
            (
                PROJECT
                    (
                        generationPaths,
                        TRANSFORM
                            (
                                {
                                    RECORDOF(LEFT),
                                    BOOLEAN     hasFiles
                                },
                                SELF.hasFiles := Std.File.GetSuperFileSubCount(LEFT.f) > 0,
                                SELF := LEFT
                            )
                    )
            );

        RETURN MAX(generationsUsed(hasFiles), n);
    END;

END; // ParentMod

//----------------------------

ChildMod := MODULE(ParentMod)

    EXPORT Tests := MODULE

        SHARED dataStoreName := '~err::test::' + Std.System.Job.WUID();
        SHARED numGens := 3;

        EXPORT DoTest := SEQUENTIAL
            (
                Init(dataStoreName, numGens);
                //ASSERT(ParentMod.NumGenerationsInUse(dataStoreName) = 0);
                ASSERT(NumGenerationsInUse(dataStoreName) = 0);
                TRUE;
            );

    END;

END; // ChildMod

//----------------------------

ChildMod.Tests.DoTest;
