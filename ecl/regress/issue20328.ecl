BaseModule := MODULE, VIRTUAL
    EXPORT AppendUniqueID(VIRTUAL DATASET ds) := FUNCTION
        ds1 := PROJECT
            (
                ds,
                TRANSFORM
                    (
                        {
                            UNSIGNED6   gid,
                            RECORDOF(LEFT)
                        },
                        SELF.gid := COUNTER,
                        SELF := LEFT
                    )
            );

        RETURN ds1;
    END;
END;

ChildModule := MODULE(BaseModule), VIRTUAL

END;

x := NOFOLD(DATASET(['a', 'b', 'c'], {STRING1 s}));
ChildModule.AppendUniqueID(x);
