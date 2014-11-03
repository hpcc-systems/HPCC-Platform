<Archive>
    <Definition name="gch.val1" value="10"/>
    <Definition name="val2" value="'hello'"/>
    <Module name="gch">
        <Attribute name="val1">
            export val1 := 1234;
        </Attribute>
    </Module>
    <Module name="blah.blah">
        <Attribute name="val1">
            export val1 := 3.1415;
        </Attribute>
    </Module>
    <Module name="">
        <Attribute name="val2">
            export val2 := 5678;
        </Attribute>
    </Module>
    <Query>

        import gch;
        import ^ as root;
        import blah;
        gch.val1;
        root.val2;
        blah.blah.val1;

    </Query>
</Archive>
