# This awk file converts a windows ini format file to an XML file
# with root CustomNameTranslation, section names as XML tags and 
# included key=value pairs as <key>value</key> under those tags.
#
BEGIN {
    print "<?xml version='1.0' encoding='UTF-8'?>"
    print "<CustomNameTranslation>"
    tag=""
}
/^#/{ next }
/^$/{ next }
/\[.*\]/{ 
    if (tag != "")
        print " </" tag ">";

    gsub(/\[/, "");
    gsub(/\]/, "");
    tag = $1;
    print " <" tag ">";
}
/.*=.*/ { 
    gsub(/=/, " ");
    print "     <" $1 ">" $2 "</" $1 ">"; 
}
END {
    if (tag != "")
        print " </" tag ">";
    print "</CustomNameTranslation>"
}
