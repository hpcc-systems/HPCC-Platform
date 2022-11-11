Quantile 1 - What is it?
========================

This series of blog posts started life as a series of walk-throughs and brainstorming sessions at a team offsite. This
series will look at adding a new activity to the system. The idea is to give an walk through of the work involved, to
highlight the different areas that need changing, and hopefully encourage others to add their own activities. In
parallel with the description in this blog there is a series of commits to the github repository that correspond to the
different stages in adding the activity. Once the blog is completed, the text will also be checked into the source
control tree for future reference.

The new activity is going to be a QUANTILE activity, which can be used to find the records that split a dataset into
equal sized blocks. Two common uses are to find the median of a set of data (split into 2) or percentiles (split into
100). It can also be used to split a dataset for distribution across the nodes in a system. One hope is that the classes
used to implement quantile in Thor can also be used to improve the performance of the global sort operation.

It may seem fatuous, but the first task in adding any activity to the system is to work out what that activity is going
to do! You can approach this in an iterative manner - starting with a minimal set of functionality and adding options as
you think of them - or start with a more complete initial design. We have used both approaches in the past to add
capabilities to the HPCC system, but on this occasion we will be starting from a more complete design - the conclusion
of our initial design discussion:

\"What are the inputs, options and capabilities that might be useful in a QUANTILE activity?\"

The discussion produced the following items:

-   Which dataset is being processed?

    This is always required and should be the first argument to the activity.

-   How many parts to split the dataset into?

    This is always required, so it should be the next argument to the activity.

-   Which fields are being used to order (and split) the dataset?

    Again this is always required, so the list of fields should follow the number of partitions.

-   Which fields are returned?

    Normally the input row, but often it would be useful for the output to include details of which quantile a row
    corresponds to. To allow this an optional transform could be passed the input row as LEFT and the quantile number as
    COUNTER.

-   How about first and last rows in the dataset?

    Sometimes it is also useful to know the first and last rows. Add flags to allow them to be optionally returned.

-   How do you cope with too few input rows (including an empty input)?

    After some discussion we decided that QUANTILE should always return the number of parts requested. If there were
    fewer items in the input they would be duplicated as appropriate. We should provide a DEDUP flag for the situations
    when that is not desired. If there is an empty dataset as input then the default (blank) row will be created.

-   Should all rows have the same weighting?

    Generally you want the same weighting for each row. However, if you are using QUANTILE to split your dataset, and
    the cost of the next operation depends on some feature of the row (e.g., the frequency of the firstname) then you
    may want to weight the rows differently.

-   What if we are only interested in the 5th and 95th centiles?

    We could optionally allow a set of values to be selected from the results.

There were also some implementation details concluded from the discussions:

-   How accurate should the results be?

    The simplest implementation of QUANTILE (sort and then select the correct rows) will always produce accurate
    results. However, there may be some implementations that can produce an approximate answer more quickly. Therefore
    we could add a SKEW attribute to allow early termination.

-   Does the implementation need to be stable?

    In other words, if there are rows with identical values for the ordering fields, but other fields not part of the
    ordering with different values, does it matter which of those rows are returned? Does the relative order within
    those matching rows matter?

    The general principle in the HPCC system is that sort operations should be stable, and that where possible
    activities return consistent, reproducible results. However, that often has a cost - either in performance or memory
    consumption. The design discussion highlighted the fact that if all the fields from the row are included in the sort
    order then the relative order does not matter because the duplicate rows will be indistinguishable. (This is also
    true for sorts, and following the discussion an optimization was added to 5.2 to take advantage of this.) For the
    QUANTILE activity we will add an ECL flag, but the code generator should also aim to spot this automatically.

-   Returning counts of the numbers in each quantile might be interesting.

    This has little value when the results are exact, but may be more useful when a SKEW is specified to allow an
    approximate answer, or if a dataset might have a vast numbers of duplicates. It is possibly something to add to a
    future version of the activity. For an approximate answer, calculating the counts is likely to add an additional
    cost to the implementation, so the target engine should be informed if this is required.

-   Is the output always sorted by the partition fields?

    If this naturally falls out of the implementations then it would be worth including it in the specification.
    Initially we will assume not, but will revisit after it has been implemented.

After all the discussions we arrived at the following syntax:

    QUANTILE(<dataset>, <number-of-ranges>, { sort-order } [, <transform>(LEFT, COUNTER)]
            [,FIRST][,LAST][,SKEW(<n>)][,UNSTABLE][,SCORE(<score>)][,RANGE(set)][,DEDUP][,LOCAL]

    FIRST - Match the first row in the input dataset (as quantile 0)
    LAST -  Match the last row in the input dataset (as quantile <n>)
    SKEW -  The maximum deviation from the correct results allowed.  Defaults to 0.
    UNSTABLE - Is the order of the original input values unimportant?
    SCORE - What weighting should be applied for each row.  Defaults to 1.
    RANGE - Which quantiles should actually be returned.  (Defaults to ALL).
    DEDUP - Avoid returning a match for an input row more than once.

We also summarised a few implementation details:

-   The activity needs to be available in GLOBAL, LOCAL and GROUPED variants.
-   The code generator should derive UNSTABLE if no non-sort fields are returned.
-   Flags to indicate if a score/range is required.
-   Flag to indicate if a transform is required.

Finally, deciding on the name of the activity took almost as long as designing it!

The end result of this process was summarised in a JIRA issue: <https://track.hpccsystems.com/browse/HPCC-12267>, which
contains details of the desired syntax and semantics. It also contains some details of the next blog topic - test cases.

Incidentally, a question that arose from of the design discussion was \"What ECL can we use if we want to annotate a
dataset with partition points?\". Ideally the user needs a join activity which walks through a table of rows, and
matches against the first row that contains key values less than or equal to the values in the search row. There are
other situations where that operation would also be useful. Our conclusion was that the system does not have a simple
way to achieve that, and that it was a deficiency in the current system, so another JIRA was created (see
<https://track.hpccsystems.com/browse/HPCC-13016>). This is often how the design discussions proceed, with discussions
in one area leading to new ideas in another. Similarly we concluded it would be useful to distribute rows in a dataset
based on a partition (see <https://track.hpccsystems.com/browse/HPCC-13260>).

Quantile 2 - Test cases
=======================

When adding new features to the system, or changing the code generator, the first step is often to write some ECL test
cases. They have proved very useful for several reasons:

-   Developing the test cases can help clarify issues, and other details that the implementation needs to take into
    account. (E.g., what happens if the input dataset is empty?)
-   They provide something concrete to aim towards when implementing the feature.
-   They provide a set of milestones to show progress.
-   They can be used to check the implementation on the different engines.

As part of the design discussion we also started to create a list of useful test cases (they follow below in the order
they were discussed). The tests perform varying functions. Some of the tests are checking that the core functionality
works correctly, while others check unusual situations and that strange boundary cases are covered. The tests are not
exhaustive, but they are a good starting point and new tests can be added as the implementation progresses.

The following is the list of tests that should be created as part of implementing this activity:

1.  Compare with values extracted from a SORT. Useful to check the implementation, but also to ensure we clearly define
    which results we are expecting.
2.  QUANTILE with a number-of-ranges = 1, 0, and a very large number. Should also test the number of ranges can be
    dynamic as well as a constant.
3.  Empty dataset as input.
4.  All input entries are duplicates.
5.  Dataset smaller than number of ranges.
6.  Input sorted and reverse sorted.
7.  Normal data with small number of entries.
8.  Duplicates in the input dataset that cause empty ranges.
9.  Random distribution of numbers without duplicates.
10. Local and grouped cases.
11. SKEW that fails.
12. Test scoring functions.
13. Testing different skews that work on the same dataset.
14. An example that uses all the keywords.
15. Examples that do and do not have extra fields not included in the sort order. (Check that the unstable flag is
    correctly deduced.)
16. Globally partitioned already (e.g., globally sorted). All partition points on a single node.
17. Apply quantile to a dataset, and also to the same dataset that has been reordered/distributed. Check the resulting
    quantiles are the same.
18. Calculate just the 5 and 95 centiles from a dataset.
19. Check a non constant number of splits (and also in a child query where it depends on the parent row).
20. A transform that does something interesting to the sort order. (Check any order is tracked correctly.)
21. Check the counts are correct for grouped and local operations.
22. Call in a child query with options that depend on the parent row (e.g., num partitions).
23. Split points that fall in the middle of two items.
24. No input rows and DEDUP attribute specified.

Ideally any test cases for features should be included in the runtime regression suite, which is found in the
testing/regress directory in the github repository. Tests that check invalid syntax should go in the compiler regression
suite (ecl/regress). Commit <https://github.com/ghalliday/HPCC-Platform/commit/d75e6b40e3503f851265670a27889d8adc73f645>
contains the test cases so far. Note, the test examples in that commit do not yet cover all the cases above. Before the
final pull request for the feature is merged the list above should be revisited and the test suite extended to include
any missing tests.

In practice it may be easier to write the test cases in parallel with implementing the parser -since that allows you to
check their syntax. Some of the examples in the commit were created before work was started on the parser, others
during, and some while implementing the feature itself.

Quantile 3 - The parser
=======================

The first stage in implementing QUANTILE will be to add it to the parser. This can sometimes highlight issues with the
syntax and cause revisions to the design. In this case there were two technical issues integrating the syntax into the
grammar. (If you are not interested in shift/reduce conflicts you may want to skip a few paragraphs and jump to the
walkthrough of the changes.)

Originally, the optional transform was specified inside an attribute, e.g., something like OUTPUT(transform). However,
this was not very consistent with the way that other transforms were implemented, so the syntax was updated so it became
an optional transform following the partition field list.

When the syntax was added to the grammar we hit another problem: Currently, a single production (sortList) in the
grammar is used for matching sort orders. As well as accepting fields from a dataset the sort order production has been
extended to accept any named attribute that can follow a sort order (e.g., LOCAL). This is because (with one token
lookahead) it is ambiguous where the sort order finishes and the list of attributes begins. Trying to include transforms
in those productions revealed other problems:

-   If a production has a sortList followed by a transform (or attribute) then it introduces a shift/reduce error on
    \',\'. To avoid the ambiguity all trailing attributes or values need to be included in the sortList.
-   Including a transform production in the sortList elements causes problems with other transform disambiguation (e.g.,
    DATASET\[x\] and AGGREGATE).
-   We could require an attribute around the transform e.g., OUTPUT(transform), but that does not really fit in with
    other activities in the language.
-   We could change the parameter order, e.g., move the transform earlier, but that would make the syntax
    counter-intuitive.
-   We could require { } around the list - but this is inconsistent with some of the other sort orders.

In order to make some progress I elected to choose the last option and require the sort order to be included in curly
braces. There are already a couple of activities - subsort and a form of atmost that similarly require them (and if
redesigning ECL from scratch I would be tempted to require them everywhere). The final syntax is something that will
need further discussion as part of the review of the pull request though, and may need to be revisited.

Having decided how to solve the ambiguities in the grammar, the following is a walkthrough of the changes that were made
as part of commit <https://github.com/ghalliday/HPCC-Platform/commit/3d623d1c6cd151a0a5608aa20ae4739a008f6e44>:

-   no\_quantile in hqlexpr.hpp

    The ECL query is represented by a graph of \"expression\" nodes - each has a \"kind\" that comes from the
    enumeration \_node\_operator. The first requirement is to add a new enumeration to represent the new activity - in
    this case we elected to reuse an unused placeholder. (These placeholders correspond to some old operators that are
    no longer supported. They have not been removed because the other elements in the enumeration need to keep the same
    values since they are used for calculating derived persistent values e.g., the hashes for persists.)

-   New attribute names in hqlatoms.

    The quantile activity introduces some new attribute names that have not been used before. All names are represented
    in an atom table, so the code in hqlatoms.hpp/cpp is updated to define the new atoms.

-   Properties of no\_quantile

    There are various places that need to be updated to allow the system to know about the properties of the new
    operator:

    -   hqlattr

        This contains code to calculate derived attributes. The first entry in the case statement is currently unused
        (the function should be removed). The second, inside calcRowInformation(), is used to predict how many rows are
        generated by this activity. This information is percolated through the graph and is used for optimizations, and
        input counts can be used to select the best implementation for a particular activity.

    -   hqlexpr

        Most changes are relatively simple including the text for the operator, whether it is constant, and the number
        of dataset arguments it has. One key function is getChildDatasetType() that indicates the kind of dataset
        arguments the operator has, which in turn controls how LEFT/RIGHT are interpreted. In this case some of the
        activity arguments (e.g., the number of quantiles) implicitly use fields within the parent dataset, and the
        transform uses LEFT, so the operator returns childdataset\_datasetleft.

    -   hqlir

        This entry is used for generating an intermediate representation of the graph. This can be useful for debugging
        issues. (Running eclcc with the logging options \"\--logfile xxx\" and \"\--logdetail 999\" will include details
        of the expression tree at each point in the code generation process in the log file. Also defining -ftraceIR
        will output the graphs in the IR format.)

    -   hqlfold

        This is the constant folder. At the moment the only change is to ensure that fields that are assigned constants
        within the transform are processed correctly. Future work could add code to optimize quantile applied to an
        empty dataset, or selecting 1 division.

    -   hqlmeta

        Similar to the functions in hqlattr that calculate derived attributes, these functions are used to calculate how
        the rows coming out of an activity are sorted, grouped and distributed. It is vital to only preserve information
        that is guaranteed to be true - otherwise invalid optimizations might be performed on the rest of the expression
        tree.

    -   reservedwords.cpp

        A new entry indicating which category the keyword belongs to.

Finally we have the changes to the parser to recognise the new syntax:

-   hqllex.l

    This file contains the lexer that breaks the ecl file into tokens. There are two new tokens - QUANTILE and SCORE.

-   Hqlgram.y

    This file contains the grammar that matches the language. There are two productions - one that matches the version
    of QUANTILE with a transform and one without. (Two productions are used instead of an optional transform to avoid
    shift/reduce errors.)

-   hqlgram2.cpp

    This contains the bulk of the code that is executed by the productions in the grammar. Changes here include new
    entries added to a case statement to get the text for the new tokens, and a new entry in the simplify() call. This
    helps reduce the number of valid tokens that could follow when reporting a syntax error.

Looking back over those changes, one reflection is that there are lots of different places that need to be changed. How
does a programmer know which functions need to change, and what happens if some are missed? In this example, the
locations were found by searching for an activity with a similar syntax e.g., no\_soapcall\_ds or no\_normalize.

It is too easy to miss something, especially for somebody new to the code - although if you do then you will trigger a
runtime internal error. It would be much better if the code was refactored so that the bulk of the changes were in one
place. (See JIRA <https://track.hpccsystems.com/browse/HPCC-13434> that has been added to track improvement of the
situation.)

With these changes implemented the examples from the previous pull request now syntax check. The next stage in the
process involves thinking through the details of how the activity will be implemented.

Quantile 4 - The engine interface.
==================================

The next stage in adding a new activity to the system is to define the interface between the generated code and the
engines. The important file for this stage is rtl/include/eclhelper.hpp, which contains the interfaces between the
engines and the generated code. These interfaces define the information required by the engines to customize each of the
different activities. The changes that define the interface for quantile are found in commit
<https://github.com/ghalliday/HPCC-Platform/commit/06534d8e9962637fe9a5188d1cc4ab32c3925010>.

Adding a quantile activity involves the following changes:

-   ThorActivityKind - TAKquantile

    Each activity that the engines support has an entry in this enumeration. This value is stored in the graph as the
    \_kind attribute of the node.

-   ActivityInterfaceEnum - TAIquantilearg\_1

    This enumeration in combination with the selectInterface() member of IHThorArg provides a mechanism for helper
    interfaces to be extended while preserving backwards compatibility with older workunits. The mechanism is rarely
    used (but valuable when it is), and adding a new activity only requires a single new entry.

-   IHThorArg

    This is the base interface that all activity interfaces are derived from. This interface does not need to change,
    but it is worth noting because each activity defines a specialized version of it. The names of the specialised
    interfaces follow a pattern; in this case the new interface is IHThorQuantileArg.

-   IHThorQuantileArg

    The following is an outline of the new member functions, with comments on their use:

    -   getFlags()

        Many of the interfaces have a getFlags() function. It provides a concise way of returning several Boolean
        options in a single call - provided those options do not change during the execution of the activity. The flags
        are normally defined with explicit values in an enumeration before the interface. The labels often follow the
        pattern T\<First-letter-of-activity\>F\<lowercase-name\>, i.e. TQFxxx \~= Thor-Quantile-Flag-XXX.

    -   getNumDivisions()

        Returns how many parts to split the dataset into.

    -   getSkew()

        Corresponds to the SKEW() attribute.

    -   queryCompare()

        Returns an implementation of the interface used to compare two rows.

    -   createDefault(rowBuilder)

        A function used to create a default row - used if there are no input rows.

    -   transform(rowBuilder, \_left, \_counter)

        The function to create the output record from the input record and the partition number (passed as counter).

    -   getScore(\_left)

        What weighting should be given to this row?

    -   getRange(isAll, tlen, tgt)

        Corresponds to the RANGE attribute.

Note that the different engines all use the same specialised interface - it contains a superset of the functions
required by the different targets. Occasionally some of the engines do not need to use some of the functions (e.g., to
serialize information between nodes) so the code generator may output empty implementations.

For each interface defined in eclhelper.hpp there is a base implementation class defined in eclhelper\_base.hpp. The
classes generated for each activity in a query by the code generator are derived from one of these base classes.
Therefore we need to create a corresponding new class CThorQuantileArg. It often provides default implementations for
some of the helper functions to help reduce the size of the generated code (e.g., getScore returning 1).

Often the process of designing the helper interface is dynamic. As the implementation is created, new options or
possibilities for optimizations appear. These require extensions and changes to the helper interface in order to be
implemented by the engines. Once the initial interface has been agreed, work on the code generator and the engines can
proceeded in parallel. (It is equally possible to design this interface before any work on the parser begins, allowing
more work to overlap.)

There are some more details on the contents of thorhelper.hpp in the documentation ecl/eclcc/WORKUNIT.rst within the
HPCC repository.

Quantile 5 - The code generator
===============================

Adding a new activity to the code generator is (surprisingly!) a relatively simple operation. The process is more
complicated if the activity also requires an implementation that generates inline C++, but that only applies to a small
subset of very simple activities, e.g., filter, aggregate. Changes to the code generator also tend to be more
substantial if you add a new type, but that is also not the case for the quantile activity.

For quantile, the only change required is to add a function that generates an implementation of the helper class. The
code for all the different activities follows a very similar pattern - generate input activities, generate the helper
for this activity, and link the input activities to this new activity. It is often easiest to copy the boiler-plate code
from a similar activity (e.g., sort) and then adapt it. (Yes, some of this code could also be refactored\... any
volunteers?) There are a large number of helper functions available to help generate transforms and other member
functions, which also simplifies the process.

The new code is found in commit
<https://github.com/ghalliday/HPCC-Platform/commit/47f850d827f1655fd6a78fb9c07f1e911b708175>.

Most of the code should be self explanatory, but one item is worth highlighting. The code generator builds up a
structure in memory that represents the C++ code that is being generated. The BuildCtx class is used to represent a
location within that generated code where new code can be inserted. The instance variable contains several BuildCtx
members that are used to represent locations to generate code within the helper class (classctx, nestedctx, createctx
and startctx). They are used for different purposes:

-   classctx

    Used to generate any member functions that can be called as soon as the helper object has been created, e.g.,
    getFlags().

-   nestedctx

    Used to generate nested member classes and objects - e.g., comparison classes.

-   startctx

    Any function that may return a value that depends on the context/parent activity. For example if QUANTILE is used
    inside the TRANSFORM of a PROJECT, the number of partition points may depend on a field in the LEFT row of the
    PROJECT. Therefore the getNumDivisions() member function needs to be generated inside instance-\>startctx. These
    functions can only be called by the engine after onCreate() and onStart() have been called to set up the current
    context.

-   createctx

    Really, this is a historical artefact from many years ago. It was originally used for functions that could be
    dependent on a global expression, but not a parent row. Almost all such restrictions have since been removed, and
    those that remain should probably be replaced with either classctx or startctx.

The only other change is to extend the switch statement in common/thorcommon/thorcommon.cpp to add a text description of
the activity.

Quantile 6 - Roxie
==================

With the code generator outputting all the information we need, we can now implement the activity in one of the engines.
(As I mentioned previously, in practice this is often done in parallel with adding it to the code generator.) Roxie and
hThor are the best engines to start with because most of their activities run on a single node - so the implementations
tend to be less complicated. It is also relatively easy to debug them, by compiling to create a stand-alone executable,
and then running that executable inside a debugger. The following description walks-through the roxie changes:

The changes have been split into two commits to make the code changes easier to follow. The first commit
(<https://github.com/ghalliday/HPCC-Platform/commit/30da006df9ae01c9aa784e91129457883e9bb8f3>) adds the simplest
implementation of the activity:

Code is added to ccdquery to process the new TAKquantile activity kind, and create a factory object of the correct type.
The implementation of the factory class is relatively simple - it primarily contains a method for creating an instance
of the activity class. Some factories create instances of the helper and cache any information that never changes (in
this case the value returned by getFlags(), which is a very marginal optimization).

The classes that implement the existing sort algorithms are extended to return the sorted array in a single call. This
allows the quicksort variants to be implemented more efficiently.

The class CRoxieServerQuantileActivity contains the code for implementing the quantile activity. It has the following
methods:

-   Constructor

    Extracts any information from the helper that does not vary, and initializes all member variables.

-   start()

    This function is called before the graph is executed. It evaluates any helper methods that might vary from execution
    to execution (e.g., getRange(), numDivisions()), but which do not depend on the current row.

-   reset()

    Called when a graph has finished executing - after an activity has finished processing all its records. It is used
    to clean up any variables, and restore the activity ready for processing again (e.g., if it is inside a child
    query).

-   needsAllocator()

    Returns true if this activity creates new rows.

-   nextInGroup()

    The main function in the activity. This function is called by whichever activity is next in the graph to request a
    new row from the quantile activity. The functions should be designed so they return the next row as quickly as
    possible, and delay any processing until it is needed. In this case the input is not read and sorted until the first
    row is requested.

    Note, the call to the helper.transform() returns the size of the resulting row, and returns zero if the row should
    be skipped. The call to finaliseRowClear() after a successful row creation is there to indicate that the row can no
    longer be modified, and ensures that any child rows will be correctly freed when the row is freed.

    The function also contains extra logic to ensure that groups are implemented correctly. The end of a group is marked
    by returning a single NULL row, the end of the dataset by two contiguous NULL rows. It is important to ensure that a
    group that has all its output rows skipped doesn\'t return two NULLs in a row - hence the checks for anyThisGroup.

With those changes in place, the second commit
<https://github.com/ghalliday/HPCC-Platform/commit/aeaa209092ea1af9660c6908062c1b0b9acff36b> adds support for the RANGE,
FIRST, and LAST attributes. It also optimizes the cases where the input is already sorted, and the version of QUANTILE
which does not include a transform. (If you are looking at the change in github then it is useful to ignore whitespace
changes by appending ?w=1 to the URL). The main changes are

-   Extra helper methods called in start() to obtain the range.
-   Optimize the situation where the input is known to be sorted by reading the input rows directly into the \"sorted\"
    array.
-   Extra checks to see if this quantile should be included in the output (FIRST,LAST,RANGE,DEDUP)
-   An optimization to link the incoming row if the transform does not modify it, by testing the TQFneedtransform flag.

Quantile 7 - Possible roxie improvements
========================================

TBD\...

hthor - trivial,sharing code and deprecated.

Discussion, of possible improvements.

Hoares\' algorithm.

Ln2(n) \< 4k?

SKEW and Hoares

Ordered RANGE. Calc offsets from the quantile (see testing/regress/ecl/xxxxx?)

SCORE

Quantile 8 - Thor
=================

TBD

Basic activity structure

Locally sorting and allowing the inputs to spill.

The partitioning approach

Classes

Skew

Optimizations
