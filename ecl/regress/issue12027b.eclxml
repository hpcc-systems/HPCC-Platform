<Archive legacyImport="0"
         legacyWhen="0">
    <Query attributePath="a.test"/>
    <Module key="a" name="a">
        <Attribute key="test" name="test" sourcePath="a/test.ecl">
            //This is invalid - check that accessing the parent of a local module doesn't resolve to the global
            import $.x.y.z.^.b;
            b.x;
        </Attribute>
        <Attribute key="x" name="x" sourcePath="a/x.ecl">
            EXPORT x := MODULE
                EXPORT y := MODULE
                    EXPORT z := MODULE
                        export c := 1;
                    END;
                END;
            END;
        </Attribute>
    </Module>
    <Module key="b" name="b">
        <Attribute key="x" name="x" sourcePath="b/x.ecl">
            output('hello');
        </Attribute>
    </Module>
</Archive>
