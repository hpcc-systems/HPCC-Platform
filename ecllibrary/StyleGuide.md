# ECL standard library style guide

The ECL code in the standard library should follow the following style
guidelines:

-   All ECL keywords in upper case
-   ECL reserved types in upper case
-   Public attributes in camel case with leading upper case
-   Private attributes in lower case with underscore as a separator
-   Field names in lower case with underscore as a separator
-   Standard indent is 2 spaces (no tabs)
-   Maximum line length of 120 characters
-   Compound statements have contents indented, and END is aligned with
    the opening statement
-   Field names are not indented to make them line up within a record
    structure
-   Parameters are indented as necessary
-   Use javadoc style comments on all functions/attributes (see [Writing
    Javadoc
    Comments](http://java.sun.com/j2se/javadoc/writingdoccomments/))

For example:

```ecl
my_record := RECORD
    INTEGER4 id;
    STRING firstname{MAXLENGTH(40)};
    STRING lastname{MAXLENGTH(50)};
END;

/**
  * Returns a dataset of people to treat with caution matching a particular lastname.  The
  * names are maintained in a global database of undesirables.
  *
  * @param  search_lastname    A lastname used as a filter
  * @return                    The list of people
  * @see                       NoFlyList
  * @see                       MorePeopleToAvoid
  */

EXPORT DodgyCharacters(STRING search_lastname) := FUNCTION
    raw_ds := DATASET(my_record, 'undesirables', THOR);
    RETURN raw_ds(last_name = search_lastname);
END;
```

Some additional rules for attributes in the library:

-   Services should be SHARED and EXPORTed via intermediate attributes
-   All attributes must have at least one matching test. If you\'re not
    on the test list you\'re not coming in.
