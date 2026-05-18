# Roxie Query Execution: Invariants and Parsing Analysis

This document summarizes findings regarding execution efficiency in the Roxie codebase, specifically concerning XML parsing, pattern matching on the hot path, and query-invariant optimizations.

## 1. Hot Path Parsing and Pattern Matching

### Identified Bottleneck: `CRoxieServerXmlParseActivity::nextRow`
In `roxie/ccd/ccdserver.cpp`, the `CRoxieServerXmlParseActivity` processes incoming rows from an input stream, extracting a text field (XML/JSON) to parse. For **every single input row**, it extracts the invariant XPath text from the helper and creates a new parser instance from scratch:

```cpp
            size32_t srchLen;
            helper.getSearchText(srchLen, srchStr, in);
            OwnedRoxieString xmlIteratorPath(helper.getXmlIteratorPath());
            xmlParser.setown(createXMLParse(srchStr, srchLen, xmlIteratorPath, *this));
```

Internally, `createXMLParse` (in `common/thorhelper/thorxmlread.cpp`) instantiates a new `CXMLParse` object. The constructor calls `init()`, which creates and initializes a `CXMLMaker` or `CJSONMaker` object. This initialization process parses the `xmlIteratorPath` (the XPath string) and sets up a pattern-matching state machine to traverse the structural hierarchy. 

**Impact:** The setup and teardown overhead of string-parsing the XPath and initializing these parser tree structures occurs on the execution engine's hot path, multiplying the CPU cycles expended rather than securely processing the payload sequence.

### Minimal RegExpr Impact on Execution
- `RegExpr` (from `jregexp.hpp`) is notably used in `roxie/ccd/ccdlistener.cpp` inside `AccessTableEntry` for query ACL constraints, but the compilation via `init()` occurs at initialization time. (It does use a spinlock `crappyUnsafeRegexLock` around `queries.find(query)` making it thread-unsafe, which can contend, but isn't row-level execution). 
- Metrics topology patterns are parsed during server startup (e.g. `roxie/ccd/ccdsnmp.cpp`). Boost regex is largely avoided in Roxie.

## 2. Query Invariant Optimizations

### Successfully Hoisted Invariants
Roxie already implements effective invariant hoisting by moving repetitive initialization from Activity instances (`CRoxieServerActivity`) into their respective Factories (`CRoxieServerActivityFactory`).

- **Tomita / NLP Parsing:** In `roxie/ccd/ccdserver.cpp`, the Tomita/NLP parse state machine is successfully isolated. `CRoxieServerParseActivityFactory` builds `Owned<INlpParseAlgorithm> algorithm = createThorParser(rc, *helper);` in its constructor. The state machine is generated once when the query graph is prepared and passed down to `CRoxieServerStrandedParseActivity` instances via a pointer.
- **Index Key Hints (`memKeyHint`):** In `roxie/ccd/ccdactivities.cpp`, XML hints for preloading indexes are parsed with `createPTreeFromXMLString` inside the constructor of `CRoxieDiskBaseActivityFactory` rather than during record processing.

### Missed Hoisting Opportunities
As established above, the XML/JSON XPath configuration in `CRoxieServerXmlParseActivity` and `XmlRecordProcessor` (in `roxie/ccd/ccdactivities.cpp`) is query-invariant. The `helper.getXmlIteratorPath()` results in an unchanging string for a given factory context.

If `CXMLMaker`/`CJSONMaker` configurations were compiled once inside `CRoxieServerXmlParseActivityFactory` (or `CRoxieXmlReadActivityFactory`) similar to the NLP algorithm, the system could omit recompiling string filters every invocation/row.