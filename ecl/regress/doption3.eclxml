<Archive>
    <Definition name="blah.blah.blah.blah" value="'blah'"/>
    <Module name="definitions">
        <Attribute name="val1">
            export val1 := 'hi';
        </Attribute>
    </Module>
    <Module name="">
        <Attribute name="val2">
            export val2 := 'defined';
        </Attribute>
    </Module>
    <Query>

        import definitions;
        import ^ as root;
        val1 := 'me';
        #IFDEFINED(val1, 'undefined');
        #IFDEFINED(definitions.val1, 'undefined');
        #IFDEFINED(root.val2, 'undefined');
        import blah;
        blah.blah.blah.blah;

    </Query>
</Archive>
