====================
Eclcc/Code generator
====================

************
Introduction
************

Purpose
=======
The primary purpose of the code generator is to take an ECL query and convert it into a work unit
that is suitable for running by one of the engines.

Aims
====
The code generator has to do its job accurately.  If the code generator does not correctly map the
ecl to the workunit it can lead to corrupt data and invalid results.  Problems like that can often be
very hard and frustrating for the ECL users to track down.

There is also a strong emphasis on generating output that is as good as possible.  Eclcc contains
many different optimizations stages, and is extensible to allow others to be easily added.

Eclcc needs to be able to cope with reasonably large jobs.  Queries that contain several megabytes of
ECL, and generate tens of thousands of activies, and 10s of Mb of C++ are routine.  These queries
need to be processed relatively quickly.

Key ideas
=========
Nearly all the processing of ecl is done using an expression graph.  The representation of the
expression graph has some particular characteristics:

* Once the nodes in the expression graph have been created they are NEVER modified.
* Nodes in the expression graph are ALWAYS commoned up if they are identical.
* Each node in the expression graph is link counted (see below) to track its lifetime.
* If a modified graph is required a new graph is created (sharing nodes from the old one)

The ecl language is a declarative language, and in general is assumed to be pure - i.e. there are no
side-effects, expressions can be evaluated lazily and re-evaluating an expression causes no
problems.  This allows eclcc to transform the graph in lots of interesting ways.  (Life is never that
simple so there are mechanisms for handling the exceptions.)

From declarative to imperative
==============================
One of the main challenges with eclcc is converting the declarative ecl code into imperative C++
code.  One key problem is it needs to try to ensure that code is only evaluated when it is required,
but that it is also only evaluated once.  It isn't always possible to satisfy both constraints - for
example a global dataset expression used within a child query.  Should it be evaluated once before
the activity containing the child query is called, or each time the child query is called?  If it is
called on demand then it may not be evaluated as efficiently...

This issue complicates many of the optimizations and transformations that are done to the queries.
Long term the plan is to allow the engines to support more delayed lazy-evaluation, so that whther
something is evaluated is more dynamic rather than static.

Flow of processing
==================
The idealised view of the processing within eclcc follows the following stages:

* Parse the ECL into an expression graph.
* Expand out function calls.
* Normalize the expression graph so it is in a consistent format.
* Normalize the references to fields within datasets to tie them up with their scopes.
* Apply various global optimizations.
* Translate logical operations into the activities that will implement them.
* Resource and generate the global graph
* For each activity, resource, optimize and generate its child graphs.

In practice the progression is not so clear cut.  There tends to be some overlap between the
different stages, and some of them may occur in slightly different orders.  However the order broadly
holds.

***********
Expressions
***********
Expression Graph representation
===============================
The key data structure within eclcc is the graph representation.  The design has some key elements.

* Once a node is created it is never modified.

  Some derived information (e.g., sort order, number of records, unique hash, ...) might be
  calculated and stored in the class after it has been created, but that doesn't change what the node
  represents in any way.
  Some nodes are created in stages - e.g., records, modules.  These nodes are marked as fully
  completed when closeExpr() is called, after which they cannot be modified.

* Nodes are always commoned up.

  If the same operator has the same arguments and type then there will be a unique IHqlExpression to
  represent it. This helps ensure that graphs stay as graphs and don't get converted to trees.  It
  also helps with optimizations, and allows code duplicated in two different contexts to be brought
  together.

* The nodes are link counted.

  Link counts are used to control the lifetime of the expression objects.  Whenever a reference to an
  expression node is held, its link count is increased, and decreased when no longer required.  The
  node is freed when there no more references.  (This generally works well, but does give us problems 
  with forward references.)

* The access to the graph is through interfaces.

  The main interfaces are IHqlExpression, IHqlDataset and IHqlScope.  They are all defined in
  hqlexpr.hpp.  The aim of the interfaces is to hide the implementation of the expression nodes so
  they can be restructured and changed without affecting any other code.

* The expression classes use interfaces and a type field rather than polymorphism.
  This could be argued to be bad object design...but.
  
  There are more than 500 different possible operators.  If a class was created for each of them the
  system would quickly become unwieldy.  Instead there are several different classes which model the
  different types of expression (dataset/expression/scope).
  
  The interfaces contain everything needed to create and interrogate an expression tree, but they do
  not contain functionality for directly processing the graph.
  
  To avoid some of the shortcomings of type fields there are various mechanisms for accessing derived attributes which avoid interrogating the type field.

* Memory consumption is critical.

It is not unusual to have 10M or even 100M nodes in memory as a query is being processed.  At that
scale the memory consumption of each node matter - so great care should be taken when considering
increasing the size of the objects.  The node classes contain a class hierarchy which i- s there
purely to reduce the memory consumption - not to reflect the functionality.  With no memory
constraints they wouldn't be there, but removing a single pointer per node can save 1Gb of memory
usage for very complex queries.

IHqlExpression
--------------
This is the interface that is used to walk and interrogate the expression graph once it has been created.  Some of the main functions are:
getOperator()	What does this node represent?  It returns a member of the node_operator enumerated type.
numChildren()	How many arguments does node have?
queryChild(unsigned n)	What is the nth child?  If the argument is out of range it returns NULL.
queryType()	The type of this node.
queryBody()	Used to skip annotations (see below)
queryProperty()	Does this node have a child which is an attribute that matches a given name.  (see below for more about attributes).
queryValue()	For a no_constant return the value of the constant.  It returns NULL otherwise.

The nodes in the expression graph are create through factory functions.  Some of the expression types
have specialised functions - e.g., createDataset, createRow, createDictionary, but scalar expressions
and actions are normally created with createValue().

Note: Generally ownership of the arguments to the createX() functions are assumed to be taken over by
the newly created node.

The values of the enumeration constants in node_operator are used to calculate "crcs" which are used
to check if the ECL for a query matches, and if disk and index record formats match.  It contains
quite a few legacy entries no_unusedXXX which can be used for new operators (otherwise new operators
must be added to the end.)

IHqlSimpleScope
---------------
This interface is implemented by records, and is used to map names to the fields within the records. 
If a record contains IFBLOCKs then each of the fields in the ifblock is defined in the
IHqlSimpleScope for the containing record.

IHqlScope
---------
Normally obtained by calling IHqlExpression::queryScope().  It is primarily used in the parser to
resolve fields from within modules.

The ecl is parsed on demand so as the symbol is looked up it may cause a cascade of ecl to be
compiled.  The lookup context (HqlLookupContext ) is passed to IHqlScope::lookupSymbol() for several
reasons:

* It contains information about the active repository - the source of the ecl which will be dynamically parsed.
* It contains caches of expanded functions - to avoid repeating expansion transforms.
* Some members are used for tracking definitions that are read to build dependency graphs, or archives of submitted queries.

This interface IHqlScope currently has some members that are used for creation; this should be
refactored and placed in a different interface.

IHqlDataset
-----------
This is normally obtained by calling IHqlExpression::queryDataset().  It has shrunk in size over
time, and could quite possibly be folded into IHqlExpression with little pain.

There is a distinction in the code generator between "tables" and "datasets".  A table
(IHqlDataset::queryTable()) is a dataset operation that defines a new output record.  Any operations
that has a transform or record that defines an output record (e.g., PROJECT,TABLE) is a table, whilst
those that don't (e.g., a filter, dedup) are not.  There are a few apparent exceptions -e.g., IF
(This is controlled by definesColumnList() which returns true the operator is a table.)

Properties and attributes
-------------------------
There are two related by slightly different concepts.  An attribute refers to the explicit flags that
are added to operators (e.g., , LOCAL, KEEP(n) etc. specified in the ECL or some internal attributes
added by the code generator).  There are a couple of different functions for creating attributes. 
createExtraAttribute() should be used by default.  createAttribute() is reserved for an attribute
that never has any arguments, or in unusual situations where it is important that the arguments are
never transformed.  They are tested using queryAttribute()/hasAttribute() and represented by nodes of
kind no_attr/no_expr_attr.

The term "property" refers to computed information (e.g., record counts) that can be derived from the
operator, its arguments and attributes.   They are covered in more detail below.

Field references
================
Fields can be selected from active rows of a dataset in three main ways:

* Some operators define LEFT/RIGHT to represent an input or processed dataset.  Fields from these
  active rows are referenced with LEFT.<field-name>.  Here LEFT or RIGHT is the "selector".
  
* Other operators use the input dataset as the selector.  E.g., myFile(myFile.id != 0).  Here the
  input dataset is the "selector".
  
* Often when the input dataset is used as the selector it can be omitted.  E.g., myFile(id != 0).
  This is implicitly expanded by the PARSER to the second form.
  A reference to a field is always represented in the expression graph as a node of kind no_select
  (with createSelectExpr).  The first child is the selector, and the second is the field.  Needless
  to say there are some complications...

* LEFT/RIGHT.

  The problem is that the different uses of LEFT/RIGHT need to be disambiguated since ther may be
  several different uses of LEFT in a query.  This is especially true when operations are executed in
  child queries.  LEFT is represented by a node no_left(record, selSeq).  Often the record is
  sufficient to disambiguate the uses, but there are situations where it isn't enough.  So in
  addition no_left has a child which is a selSeq (selector sequence) which is added as a child
  attribute of the PROJECT or other operator.  At parse time it is a function of the input dataset. 
  That is later normalized to a unique id to reduce the transformation work.

* Active datasets.  It is slightly more complicated - because the dataset used as the selector can
  be any upstream dataset up to the nearest table. So the following ecl code is legal:

  ::

    x := DATASET(...)
    y := x(x.id != 0);
    z := y(x.id != 100);

Here the reference to x.id in the definition of z is referring to a field in the input dataset.

Because of these semantics the selector in a normalized tree is actually
inputDataset->queryNormalizedSelector() rather than inputDatset.  This function currently returns the
table expression node (but it may change in the future see below).

Attribute "new"
---------------
In some situations ECL allows child datasets to be treated as a dataset without an explicit
NORMALIZE.  E.g., EXISTS(myDataset.ChildDataset);

This is primarily to enable efficient aggregates on disk files to be generated, but it adds some
complications with an expression of the form dataset.childdataset.grandchild.  E.g.,::

  EXISTS(dataset(EXISTS(dataset.childdataset.grandchild))

Or::

  EXISTS(dataset.childdataset(EXISTS(dataset.childdataset.grandchild))

In the first example dataset.childdataset within the dataset.childdataset .grandchild is a reference
to a dataset that doesn't have an active cursor and needs to be iterated), whilst in the second it
refers to an active cursor.

To differentiate between the two, all references to fields within datasets/rows that don't have
active selectors have an additional attribute("new") as a child of the select.  So a no_select with a
"new" attribute requires the dataset to be created, one without is a member of an active dataset
cursor.

If you have a nested row, the new attribute is added to the selection from the dataset, rather than
the selection from the nested row.  The functions queryDatasetCursor() and querySelectorDataset())
are used to help interpret the meaning.

(An alternative would be to use a different node from no_select - possibly this should be considered
- it would be more space efficient.)

The expression graph generated by the ECL parser doesn't contain any new attributes.  These are added
as one of the first stages of normalizing the expression graph.  Any code that works on normalized
expressions needs to take care to interpret no_selects correctly.

Transforming selects
--------------------
When an expression graph is transformed and none of the records are change then the representation of
LEFT/RIGHT remains the same.  This means any no_select nodes in the expression tree will also stay
the same.

However if the transform modifies a table (highly likely) it means that the selector for the second
form of field selector will also change.  Unfortunately this means that transforms often cannot be
short-circuited.

It could significantly reduce the extent of the graph that needs traversing, and the number of nodes
replaced in a transformed graph if this could be avoided.  One possibility is to use a different
value for dataset->queryNormalizedSelector() using a unique id associated with the table.  I think it
would be a good long term change, but it would require unique ids (similar to the selSeq) to be added
to all table expressions, and correctly preserved by any optimization.

Annotations
===========
Sometimes it is useful to add information into the expression graph (e.g., symbol names, position
information) that doesn't change the meaning, but should be preserved.  Annotations allow information
to be added in this way.

An annotation's implementation of IHqlExpression generally delegates the majority of the methods
through to the annotated expression.  This means that most code that interrogates the expression
graph can ignore their presence, which simplifies the caller significantly.  However transforms need
to be careful (see below).

Information about the annotation can be obtained by calling IHqlExpression:: getAnnotationKind() and
IHqlExpression:: queryAnnotation().

Associated side-effects
=======================
In legacy ecl you will see code like the following\:::

  EXPORT a(x) := FUNCTION
     Y := F(x);
     OUTPUT(Y);
     RETURN G(Y);
  END;

The assumption is that whenever a(x) is evaluated the value of Y will be output.  However that
doesn't particularly fit in with a declarative expression graph.   The code generator creates a
special node (no_compound) with child(0) as the output action, and child(1) as the value to be
evaluated (g(Y)).

If the expression ends up being included in the final query then the action will also be included
(via the no_compound).  At a later stage the action is migrated to a position in the graph where
actions are normally evaluated.

Derived properties
==================
There are many pieces of information it is useful to know about a node in the expression graph - many
of which would be expensive to recomputed each time there were required.  Eclcc has several
mechanisms for caching derived information so it is available efficiently.

* Boolean flags - getInfoFlags()/getInfoFlags2().

  There are many Boolean attributes of an expression that it is useful to know - e.g., is it
  constant, does it have side-effects, does it reference any fields from a dataset etc. etc.  The
  bulk of these are calculated and stored in a couple of members of the expression class.  They are
  normally retrieved via accessor functions e.g., containsAssertKeyed(IHqlExpression*).

* Active datasets - gatherTablesUsed().

  It is very common to want to know which datasets an expression references.  This information is
  calculated and cached on demand and accessed via the IHqlExpression::gatherTablesUsed() functions. 
  There are a couple of other functions IHqlExpression::isIndependentOfScope() and
  IHqlExpression::usesSelector() which provide efficient functions for common uses.

* Information stored in the type.

  Currently datasets contain information about sort order, distribution and grouping as part of the
  expression type.  This information should be accessed through the accessor functions applied to the
  expression (e.g., isGrouped(expr)).  At some point in the future it is planned to move this
  information as a general derived property (see next).

* Other derived property.

  There is a mechanism (in hqlattr) for calculating and caching an arbitrary derived property of an
  expression.  It is currently used for number of rows, location-independent representation, maximum
  record size etc. .  There are typically accessor functions to access the cached information (rather
  than calling the underlying IHqlExpression::queryAttribute() function).

* Helper functions.

  Some information doesn't need to be cached because it isn't expensive to calculate, but rather than
  duplicating the code, a helper function is provided.  E.g., queryOriginalRecord(),
  hasUnknownTransform().  They are not part of the interface because the number would make the
  interface unwieldy and they can be completely calculated from the public functions.

  However it can be very hard to find the function you are looking for, and they would greatly
  benefit from being grouped e.g., into namespaces.

Transformations
===============
One of the key processes in eclcc is walking and transforming the expression graphs.  Both of these
are covered by the term transformations.  One of the key things to bear in mind is that you need to
walk the expression graph as a graph, not as a tree.  If you have already examined a node one you
shouldn't repeat the work - otherwise the execution time may be exponential with node depth.

Other things to bear in mind

* If a node isn't modified don't create a new one - return a link to the old one.
* You generally need to walk the graph and gather some information before creating a modified graph. 
  Sometimes creating a new graph can be short-circuited if no changes will be required.
* Sometimes you can be tempted to try and short-circuit transforming part of a graph (e.g., the
  arguments to a dataset activity), but because of the way references to fields within dataset work
  that often doesn't work.
* If an expression is moved to another place in the graph you need to be very careful to check if the
  original context was conditional and the new context is not.
* The meaning of expressions can be context dependent.  E.g., References to active datasets can be
  ambiguous.
* Never walk the expressions as a tree, always as a graph!
* Be careful with annotations.

It is essential that an expression that is used in different contexts with different annotations
(e.g., two different named symbols) is consistently transformed.  Otherwise it is possible for a
graph to be converted into a tree.  E.g.,::

  A := x; B := x; C = A + B;

must not be converted to::

  A' := x'; B' := X'';  C' := A' + B';

For this reason most transformers will check if expr->queryBody() matches expr, and if not will
transform the body (the unannotated expression), and then clone any annotations.

Some examples of the work done by transformations are:

* Constant folding.
* Expanding function calls.
* Walking the graph and reporting warnings.
* Optimizing the order and removing redundant  activities.
* Reducing the fields flowing through the generated graph.
* Spotting common sub expressions
* Calculating the best location to evaluate an expression (e.g., globally instead of in a child query).
* Many, many others.

Some more details on the individual transforms are given below..

**********
Key Stages
**********
Parsing
=======
The first job of eclcc is to parse the ECL into an expression graph.  The source for the ecl can come
from various different sources (archive, source files, remote repository).  The details are hidden
behind the IEclSource/IEclSourceCollection interfaces.  The createRepository() function is then used
to resolve and parse the various source files on demand.

Several things occur while the ECL is being parsed:

* Function definitions are expanded inline.

  A slightly unusual behaviour.  It means that the expression tree is a fully expanded expression -
  which is better suited to processing and optimizing.

* Some limited constant folding occurs.
  
  When a function is expanded, often it means that some of the
  test conditions are always true/false.  To reduce the transformations the condition may be folded
  early on.  
  
* When a symbol is referenced from another module that will recursively cause the ecl for that module
  (or definition within that module) to be parsed.

* Currently the semantic checking is done as the ECL is parsed.

  If we are going to fully support template functions and delayed expansion of functions this will
  probably have to change so that a syntax tree is built first, and then the semantic checking is
  done later.

Normalizing
===========
There are various problems with the expression graph that comes out of the parser:

* Records can have values as children (e.g., { myField := infield.value} ), but it causes chaos if
  record definitions can change while other transformations are going on.  So the normalization
  removes values from fields.
* Some activities use records to define the values that output records should contain (e.g., TABLE). 
  These are now converted to another form (e.g., no_newusertable).
* Sometimes expressions have multiple definition names.  Symbols and annotations are rationalized and
  commoned up to aid commoning up other expressions.
* Some PATTERN definitions are recursive by name.  They are resolved to a form that works if all
  symbols are removed.
* The CASE/MAP representation for a dataset/action is awkward for the transforms to process.  They
  are converted to nested Ifs.
  
  (At some point a different representation might be a good idea.)
* EVALUATE is a weird syntax.  Instances are replaced with equivalent code which is much easier to
  subsequently process.
* The datasets used in index definitions are primarily there to provide details of the fields.  The
  dataset itself may be very complex and may not actually be used.  The dataset input to an index is
  replaced with a dummy "null" dataset to avoid unnecessary graph transforming, and avoid introducing
  any additional incorrect dependencies.

Scope checking
==============
Generally if you use LEFT/RIGHT then the input rows are going to be available wherever they are
used.  However if they are passed into a function, and that function uses them inside a definition
marked as global then that is invalid (since by definition global expressions don't have any context).

Similarly if you use syntax <dataset>.<field>, its validity and meaning depends on whether <dataset>
is active.  The scope transformer ensures that all references to fields are legal, and adds a "new"
attribute to any no_selects where it is necessary.

Constant folding: foldHqlExpression
===================================
This transform simplifies the expression tree.  Its aim is to simplify scalar expressions, and
dataset expressions that are valid whether or not the nodes are shared.  Some examples are:

* 1 + 2 => 3 and any other operation on scalar constants.
* IF(true, x, y) => x
* COUNT(<empty-dataset>) => 0
* IF (a = b, 'c', 'd') = 'd'  => IF(a=b, false, true) => a != b
* Simplifying sorts, projects filters on empty datasets

Most of the optimizations are fairly standard, but a few have been added to cover more esoteric
examples which have occurred in queries over the years.

This transform also supports the option to percolate constants through the graph.  E.g., if a project
assigns the value 3 to a field, it can substitute the value 3 wherever that field is used in
subsequent activities.  This can often lead to further opportunities for constant folding (and
removing fields in the implicit project).

Expression optimizer: optimizeHqlExpression
===========================================
This transformer is used to simplify, combine and reorder dataset expressions.  The transformer takes
care to count the number of times each expression is used to ensure that none of the transformations
cause duplication.  E.g., swapping a filter with a sort is a good idea, but if there are two filters
of the same sort and they are both swapped you will now be duplicating the sort.

Some examples of the optimizations include:

* COUNT(SORT(x)) => COUNT(x)
* Moving filters over projects, joins, sorts.
* Combining adjacent projects, projects and joins.
* Removing redundant sorts or distributes
* Moving filters from JOINs to their inputs.
* Combining activities e.g., CHOOSEN(SORT(x)) => TOPN(x)
* Sometimes moving filters into IFs
* Expanding out a field selected from a single row dataset.
* Combine filters and projects into compound disk read operations.

Implicit project: insertImplicitProjects
========================================
ECL tends to be written as general purpose definitions which can then be combined.  This can lead to
potential inefficiencies - e.g., one definition may summarise some data in 20 different ways, this is
then used by another definition which only uses a subset of those results.  The implicit project
transformer tracks the data flow at each point through the expression graph, and removes any fields
that are not required.

This often works in combination with the other optimizations.  For instance the constant percolation
can remove the need for fields, and removing fields can sometimes allow a left outer join to be
converted to a project.

*********
Workunits
*********
is this the correct term?  Should it be a query? This should really be independent of this document...)
=======================================================================================================

The code generator ultimately creates workunits.  A workunit completely describes a generated query.
It consists of two parts.  There is an xml component - this contains the workflow information, the
various execution graphs, and information about options.  It also describes which inputs can be
supplied to the query and what results are generated.  The other part is the generated shared object
compiled from the generated C++.  This contains functions and classes that are used by the engines to
execute the queries.  Often the xml is compressed and stored as a resource within the shared object -
so the shared object contains a complete workunit.

Workflow
========

The actions in a workunit are divided up into individual workflow items.  Details of when each
workflow item is executed, what its dependencies are stored in the <Workflow> section of the xml. 
The generated code also contains a class definition, with a method perform() which is used to execute
the actions associated with a particular workflow item. (The class instances are created by calling
the exported createProcess() factory function).

The generated code for an individual workflow item will typically call back into the engine at some
point to execute a graph.

Graph
=====
The activity graphs are stored in the xml.  The graph contains details of which activities are
required, how those activities link together, what dependencies there are between the activities. 
For each activity it might the following information:

* A unique id.
* The "kind" of the activity (from enum ThorActivityKind in eclhelper.hpp)
* The ecl that created the activity.
* Name of the original definition
* Location (e.g., file, line number) of the original ecl.
* Information about the record size, number of rows, sort order etc.
* Hints which control options for a particular activity (e.g,, the number of threads to use while sorting).
* Record counts and stats once the job has executed.

Each activity in a graph also has a corresponding helper class instance in the generated code.  (The
name of the class is cAc followed by the activity number, and the exported factory method is fAc
followed by the activity number.)  The classes implement the interfaces defined in eclhelper.hpp.

The engine uses the information from the xml to produce a graph of activities that need to be
executed.  It has a general purpose implementation of each activity kind, and it uses the class
instance to tailor that general activity to the specific use e.g., what is the filter condition, what
fields are set up, what is the sort order?

Inputs and Results
==================
The workunit xml contains details of what inputs can be supplied when that workunit is run.  These
correspond to STORED definitions in the ecl.  The result xml also contains the schema for the results
that the workunit will generate.

Once an instance of the workunit has been run, the values of the results may be written back into
dali's copy of the workunit so they can be retrieved and displayed.

Generated code
==============
Aims for the generated C++ code:

* Minimal include dependencies.

  Compile time is an issue - especially for small on-demand queries.  To help reduce compile times
  (and dependencies with the rest of the system) the number of header files included by the generated
  code is kept to a minimum.  In particular references to jlib, boost and icu are kept within the
  implementation of the runtime functions, and are not included in the public dependencies.

* Thread-safe.

  It should be possible to use the members of an activity helper from multiple threads without
  issue.  The helpers may contain some context dependent state, so different instances of the helpers
  are needed for concurrent use from different contexts (e.g., expansions of a graph.)

* Concise.

  The code should be broadly readable, but the variable names etc. are chosen to generate compact code.

* Functional.

  Generally the generated code assigns to a variable once, and doesn't modify it afterwards.  Some
  assignments may be conditional, but once the variable is evaluated it isn't updated.  (There are of
  course a few exceptions - e.g., dataset iterators)

**********************
Implementation details
**********************
First a few pointers to help understand the code within eclcc:

* It makes extensive use of link counting.  You need understand that concept to get very far.
* If something is done more than once then that is generally split into a helper function.

  The helper functions aren't generally added to the corresponding interface (e.g., IHqlExpression)
  because the interface would become bloated.  Instead they are added as global functions.  The big
  disadvantage of this approach is they can be hard to find.  Even better would be for them to be
  rationalised and organised into namespaces.

* The code is generally thread-safe unless there would be a significant performance implication.  In
  generally all the code used by the parser for creating expressions is thread safe.  Expression
  graph transforms are thread-safe, and can execute in parallel if a constant
  (NUM_PARALLEL_TRANSFORMS) is increased.  The data structures used to represent the generated code
  are NOT thread-safe.
* Much of the code generation is structured fairly procedurally, with classes used to process the
  stages within it.
* There is a giant "God" class HqlCppTranslator - which could really do with refactoring.

Parser
======
The ECLCC parser uses the standard tools bison and flex to process the ecl and convert it to a
 expression graph.  There are a couple of idiosyncrasies with the way it is implemented.

* Macros with fully qualified scope.

  Slightly unusually macros are defined in the same way that other definitions are - in particular to
  can have references to macros in other modules.  This means that there are references to macros
  within the grammar file (instead of being purely handled by a pre-processor).  It also means the
  lexer keeps an active stack of macros being processed.

* Attributes on operators.

  Many of the operators have optional attributes (e.g., KEEP, INNER, LOCAL, ...).  If these were all
  reserved words it would remove a significant number of keywords from use as symbols, and could also
  mean that when a new attribute was added it broke existing code.  To avoid this the lexer looks
  ahead in the parser tables (by following the potential reductions) to see if the token really could
  come next.  If it can't then it isn't reserved as a symbol.

**************
Generated code
**************
As the workunit is created the code generator builds up the generated code and the xml for the
workunit.  Most of the xml generation is encapsulated within the IWorkUnit interface.  The xml for
the graphs is created in an IPropertyTree, and added to the workunit as a block.

C++ Output structures
=====================
The C++ generation is ultimately controlled by some template files (thortpl.cpp).  The templates are
plain text and contain references to allow named sections of code to be expanded at particular points.

The code generator builds up some structures in memory for each of those named sections.  Once the
generation is complete some peephole optimization is applied to the code.  This structure is walked
to expand each named section of code as required.

The BuildCtx class provides a cursor into that generated C++.  It will either be created for a given
named section, or more typically from another BuildCtx.  It has methods for adding the different
types of statements.  Some are simple (e.g., addExpr()), whilst some create a compound statement
(e.g., addFilter).  The compound statements change the active selector so any new statements are
added within that compound statement.

As well as building up a tree of expressions, this data structure also maintains a tree of
associations.  For instance when a value is evaluated and assigned to a temporary variable, the
logical value is associated with that temporary.  If the same expression is required later, the
association is matched, and the temporary value is used instead of recalculating it.  The
associations are also use to track the active datasets, classes generated for row-meta information,
activity classes etc. etc.

Activity Helper
===============
Each activity in an expression graph will have an associated class generated in the c++.  Each
different activity kind expects a helper that implements a particular IHThorArg interface.  E.g., a
sort activity of kind TAKsort requires a helper that implements IHThorSortArg.  The associated
factory function is used to create instances of the helper class.

The generated class might take one of two forms:

* A parameterised version of a library class.  These are generated for simple helpers that don't have
  many variations (e.g., CLibrarySplitArg for TAKsplit), or for special cases that occur very
  frequently (CLibraryWorkUnitReadArg for internal results).
* A class derived from a skeleton implementation of that helper (typically CThorXYZ implementing
  interface IHThorXYZ).  The base class has default implementations of some of the functions, and any
  exceptions are implemented in the derived class.

Meta helper
===========
This is a class that is used by the engines to encapsulate all the information about a single row -
e.g., the format that each activity generates.  It is an implementation of the IOutputMeta
interface.  It includes functions to

* Return the size of the row.
* Serialize and deserialize from disk.
* Destroy and clean up row instances.
* Convert to xml.
* Provide information about the contained fields.

Building expressions
====================
The same expression nodes are used for representing expressions in the generated C++ as the original
ECL expression graph.  It is important to keep track of whether an expression represents untranslated
ECL, or the "translated" C++.  For instance ECL has 1 based indexes, while C++ is zero based.  If you
processed the expression x[1] it might get translated to x[0] in C++.  Translating it again would
incorrectly refer to x[-1].

There are two key classes used while building the C++ for an ECL expression:

CHqlBoundExpr.

  This represents a value that has been converted to C++.  Depending on the type, one or more of the
  fields will be filled in.

CHqlBoundTarget.

  This represents the target of an assignment -C++ variable(s) that are going to be assigned the
  result of evaluating an expression.  It is almost always passed as a const parameter to a function
  because the target is well-defined and the function needs to update that target.

  A C++ expression is sometimes converted back to a ecl pseudo-expression by calling
  getTranslatedExpr().  This creates an expression node of kind no_translated to indicate the child
  expression has already been converted.

Scalar expressions
------------------
The generation code for expressions has a hierarchy of calls.   Each function is there to allow
optimal code to be generated - e.g., not creating a temporary variable if none are required.  A
typical flow might be:

* buildExpr(ctx, expr, bound).

  Evaluate the ecl expression "expr" and save the C++ representation in the class bound.  This might
  then call through to...

* buildTempExpr(ctx, expr, bound);

  Create a temporary variable, and evaluate expr and assign it to that temporary variable.... Which
  then calls.

* buildExprAssign(ctx, target, expr);

  evaluate the expression, and ensure it is assigned to the C++ target "target".

  The default implementation might be to call buildExpr....

An operator must either be implemented in buildExpr() (calling a function called doBuildExprXXX) or
in buildExprAssign() (calling a function called doBuildAssignXXX).  Some operators are implemented in
both places if there are different implementations that would be more efficient in each context.

Similarly there are several different assignment functions:

* buildAssign(ctx, <ecl-target>, <ecl-value>);
* buildExprAssign(ctx, <c++-target>, <ecl-value>);
* assign(ctx, <C++target>, <c++source>)

The different varieties are there depending on whether the source value or targets have already been
translated.  (The names could be rationalised!)

Datasets
--------
Most dataset operations are only implemented as activities (e.g., PARSE, DEDUP).  If these are used
within a transform/filter then eclcc with generate a call to a child query.  An activity helper for the
appropriate operation will then be generated.

However a subset of the dataset operations can also be evaluated inline without calling a child query. 
Some examples are filters, projects, simple aggregation.  It removes the overhead of the child query
call in the simple cases, and often generates more concise code.

When datasets are evaluated inline there is a similar hierarchy of function calls:

* buildDatasetAssign(ctx, target, expr);

  Evaluate the dataset expression, and assign it to the target (a builder interface).
  This may then call....

* buildIterate(ctx, expr)

  Iterate through each of the rows in the dataset expression in turn.
  Which may then call...

* buildDataset(ctx, expr, target, format)

  Build the entire dataset, and return it as a single value.

Some of the operations (e.g., aggregating a filtered dataset) can be done more efficiently by summing and
filtering an iterator, than forcing the filtered dataset to be evaluated first.

Dataset cursors
---------------
The interface IHqlCppDatasetCursor allows the code generator to iterate through a dataset, or select
a particular element from a dataset.  It is used to hide the different representation of datasets,
e.g.,

* Blocked - the rows are in a contiguous block of memory appended one after another.
* Array - the dataset is represented by an array of pointers to the individual rows.
* Link counted - similar to array, but each element is also link counted.
* Nested.  Sometimes the cursor may iterate through multiple levels of child datasets.

Generally rows that are serialized (e.g., on disk) are in blocked format, and they are stored as link
counted rows in memory.

Field access classes
--------------------
The IReferenceSelector interface and the classes in hqltcppc[2] provide an interface for getting and
setting values within a row of a dataset.  They hide the details of the layout - e.g., csv/xml/raw
data, and the details of exactly how each type is represented in the row.

Key filepos weirdness
---------------------
The current implementation of keys in HPCC uses a format which uses a separate 8 byte integer field
which was historically used to store the file position in the original file.  Other complications are
that the integer fields are stored big-endian, and signed integer values are biased.

This introduces some complication in the way indexes are handled.  You will often find that the
logical index definition is replaced with a physical index definition, followed by a project to
convert it to the logical view.  A similar process occurs for disk files to support
VIRTUAL(FILEPOSITION) etc.

***********
Source code
***********
The following are the main directories used by the ecl compiler.

+------------------+-------------------------------------------------------------------------------------+
| Directory        | Contents                                                                            |
+==================+=====================================================================================+
| rtl/eclrtpl      | Template text files used to generate the C++ code                                   |
+------------------+-------------------------------------------------------------------------------------+
| rtl/include      | Headers that declare interfaces implemented by the generated code                   |
+------------------+-------------------------------------------------------------------------------------+
| common/deftype   | Interfaces and classes for scalar types and values.                                 |
+------------------+-------------------------------------------------------------------------------------+
| common/workunit  | Code for managing the representation of a work unit.                                |
+------------------+-------------------------------------------------------------------------------------+
| ecl/hql          | Classes and interfaces for parsing and representing an ecl expression graph         |
+------------------+-------------------------------------------------------------------------------------+
| ecl/hqlcpp       | Classes for converting an expression graph to a work unit (and C++)                 |
+------------------+-------------------------------------------------------------------------------------+
| ecl/eclcc        | The executable which ties everything together.                                      |
+------------------+-------------------------------------------------------------------------------------+

**********
Challenges
**********
From declarative to imperative
==============================
As mentioned at the start of this document, one of the main challenges with eclcc is converting the
declarative ecl code into imperative C++ code.  The direction we are heading in is to allow the
engines to support more lazy-evaluation so possibly in this instance to evaluate it the first time it
is used (although that may potentially be much less efficient).  This will allow the code generator
to relax some of its current assumptions.

There are several example queries which are already producing pathological behaviour from eclcc,
causing it to generate C++ functions which are many thousands of lines long.

The parser
==========
Currently the grammar for the parser is too specialised.  In particular the separate productions for
expression, datasets, actions cause problems - e.g., it is impossible to properly allow sets of
datasets to be treated in the same way as other sets.

The semantic checking (and probably semantic interpretation) is done too early.  Really the parser
should build up a syntax tree, and then disambiguate it and perform the semantic checks on the syntax
tree.

The function calls should probably be expanded later than they are.  I have tried in the past and hit
problems, but I can't remember all the details.  Some are related to the semantic checking.
