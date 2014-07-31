<Archive legacyImport="0"
         legacyWhen="0">
    <Query attributePath="a.test"/>
    <Module key="a" name="a">
        <Attribute key="test" name="test" sourcePath="a/test.ecl">
            //Parent of root is illegal
            import $.^.^.b;
            b.x;
        </Attribute>
    </Module>
    <Module key="b" name="b">
        <Attribute key="x" name="x" sourcePath="b/x.ecl">
            output('hello');
        </Attribute>
    </Module>
</Archive>
