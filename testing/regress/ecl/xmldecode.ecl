// Some non-error cases
XMLDECODE('Cats &amp; dogs');
XMLDECODE('&lt;html&gt;');
XMLDECODE('Cats &quot;love&quot; dog&apos;s dinner');
XMLDECODE('&#65; is ascii for A');
XMLDECODE('&#x42; is ascii for B');
XMLDECODE('This ---&gt;&nbsp;&lt;---- is a non-breaking space');

// Some error cases - leave the original unchanged
XMLDECODE('Cats & dogs');
XMLDECODE('&amp');  // missing ;
XMLDECODE('&nosuch');  // missing ; on unknown entity
XMLDECODE('&#;');  // missing digits
XMLDECODE('&#65 is not valid');  // missing semi
XMLDECODE('&#d;');  // invalid digits
XMLDECODE(u'&AMP;');  // invalid entity
XMLDECODE('&;');  // missing contents
XMLDECODE('&');  // missing contents

// Now the same in unicode

// Some non-error cases
XMLDECODE(u'Cats &amp; dogs');
XMLDECODE(u'&lt;html&gt;');
XMLDECODE(u'Cats &quot;love&quot; dog&apos;s dinner');
XMLDECODE(u'&#65; is ascii for A');
XMLDECODE(u'&#x42; is ascii for B');
XMLDECODE(u'This ---&gt;&nbsp;&lt;---- is a non-breaking space');

// Some error cases - leave the original unchanged
XMLDECODE(u'Cats & dogs');
XMLDECODE(u'&amp');  // missing ;
XMLDECODE(u'&nosuch');  // missing ; on unknown entity
XMLDECODE(u'&#;');  // missing digits
XMLDECODE(u'&#65 is not valid');  // missing semi
XMLDECODE(u'&#d;');  // invalid digits
XMLDECODE(u'&AMP;');  // invalid entity
XMLDECODE(u'&;');  // missing contents
XMLDECODE(u'&');  // missing contents
