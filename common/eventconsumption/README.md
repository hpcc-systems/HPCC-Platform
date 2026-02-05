# Event Consumption

# IEventVisitor

The `readEvents` function declared in `jevent.hpp` follows a push-parsing model for parsing event files created by `EventRecorder`, declared in the same file. Given a file path and an instance of a visitor implementation, processing consists of these steps:
- The function notifies the visitor that processing of a new file is beginning. The visitor may abort processing at this time.
- The function notifies the visitor of zero or more events. The visitor may abort processing during any of these visits.
- The function notifies the visitor that processing is complete.

## IEventVisitationLink

An extension of `IEventVisitor`, implementations can be combined in a singly linked list of visitors. Each node in the list chooses whether to propagate notification to the next node in the list. When propagating, each node controls what data is passed on.

### IEventFilter

An extension of `IEventVisitationLink`, this abstraction describes visitors whose purpose is to block unwanted events from propagating to the next visitor in the chain. Implementations define the filtering logic. Generally, an implementation will only prevent individual event visitations from propagating.

#### CEventFilter

A concrete implementation of `IEventFilter` that classifies all incoming events as either wanted or unwanted. Events that are wanted are propagates, unchanged, to the next linked visitor. Unwanted events are not propagated.

Filter terms define conditions used to identify wanted events. Conditions may be applied based on event type and event attribute values. All events are considered wanted until at least one filter term is applied.

##### Filter by Event

An event type term identifies either a single event type or a single event context, implicitly identifying every event type associated with that context.

Event terms may be specified by the `EventType` enumeration, the `EventCtx` enumeration, or a comma-delimited list of event type and/or context names. The list format is described as follows:

        term-list ::= term ( ',' term )*
        term ::= ( [ type-comparison ] type-term | [ context-comparison ] context-term )
        type-comparison ::= '[' ( "eq" | "neq" | "except" ) ']'
        type-term ::= <event-type-name>
        context-comparison ::= '[' ( "in" | "out" | "except" ) ']'
        context-term ::= <event-context-name>

Values of `type-comparison`, `type-term`, `context-comparison`, and `context-term` are case insensitive.

By default, all identified types are accumulated into a collection of accepted event types. Optional comparison tokens alter how the term value is interpreted. Events with types in the accumulated type collection are retained, while others are suppressed. Accepted comparisons include:
- `eq` (default): the term type is accumulated
- `neq`: all types except the term type are accumulated
- `in` (default): all event types with the term context are accumulated
- `out`: all event types without the term context are accumulated
- `except`: removes the term value from the accumulated collection
  - If the term value is a type, the type is removed if it was previously accumulated.
  - If the term value is a context, all previously accumulated types of the context are removed.

An example using `out` might be `[not]dali`. The effect is to accept all events not in the dali event context.

An example using `except` might be `index,[except]IndexPayload`. The effect of this list is to accept all index context events except for IndexPayload.

##### Filter by Attribute

An event attribute term identifies a single attribute, a set of expected values, and an optional condition relating an event value to expected values. Multiple terms are permitted for one attribute, in which case at least one term must be satisfied. Terms are permitted for multiple attributes, in which case a term for each attribute must be satisfied.

Terms related to attributes not present in an event are implicitly satisfied. Rules for satisfying terms vary by the attribute data type.

###### Text

A comma-delimited list of terms is defined for text attributes, using the following format:

        term-list ::= term ( ',' term )*
        term ::= [ comparison ] <pattern>
        comparison ::= '[' ( "wild" | "eq" | "neq" | "lt" | "lte" | "gte" | "gt" ) ']'

In the absence of `comparison`, the term text is used as a wilcard matching pattern. Case-insensitive comparisons include:
- `wild` (default): attribute value is a wildcard match of the term pattern
- `eq`: attribute value matches pattern
- `neq`: attribute value does not match pattern
- `lt`: attribute value is less than the term pattern
- `lte`: attribute value is less than or equal to the term pattern
- `gte`: attribute value is greater than or equal to the term pattern
- `gt`: attribute value is greater than the term pattern

The default `wild` comparison is the only comparison that considers wildcard substitutions. All others perform simple case-insensitive string comparisons.

###### Boolean

A comma-delimited list of terms is defined for text attributes, using the following format:

        term-list ::= term ( ',' term )
        term ::= <value>

A term list is supported by common code. With only two possible values, nothing is gained by adding two terms with different value.

###### Numeric

A comma-delimited list of terms is defined for numeric attributes, using the following format:

        term-list ::= term ( ',' term )*
        term ::= [ comparison ] ( single-value | range | at-most | at-least )
        comparison ::= '[' ( "eq" | "neq" | "lt" | "lte" | "gte" | "gt" | "in" | "out" ) ']'
        single-value ::= <number>
        range ::= single-value '-' single-value
        at-most ::= '-' single-value
        at-least ::= single-value '-'

Every term is treated implicitly treated as a range defined as `[term-minimum..term-maximum]`. The `term-minimum` must be less than or equal to the `term-maximum`. Four forms of range specification are accepted:
- `single-value`: the value is the range minimum and maximum
- `range`: the left value is the range minimum and the right value is the range maximum
- `at-most`: the range minimum is 0 and the range maximum is the value
- `at-least`: the range minimum is the value and the range maximum is `UINT64_MAX`

In the absence of `comparison`, the attribute value must not be less than the term range minimum and not more than the term range maximum. Comparisons include:
- `eq`: attribute value is equal to the single-value term range.
- `neq`: attribute value is not equal to the single-value term range
- `lt`: attribute value is less than term-minimum
- `lte`: attribute value is less than or equal to term-minimum
- `gte`: attribute value is greater than or equal to term-maximum
- `gt`: attribute value is greater than term-maximum
- `in` (default): attribute value is a member of the term range
- `out`: attribute value is not a member of the term range

###### Timestamp

A comma-delimited list of terms is defined for timestamp attributes, using an variation of the [numeric](#numeric) format:

        single-value ::= ( <nanoseconds> | <timestamp> )

All numeric comparisons are supported. A `timestamp` value will be converted to a `nanosecond` value using `CDateTime`, then numeric comparisons are applied.

###### Special Cases

1. Index Events

All index context events include the `FileId` attribute. The `FileInformation` meta event includes the `Path` attribute. All index events can filter `FileId` using either the actual numeric value or the related `Path` value from a previously seen `FileInformation` meta event. A `FileId` term with a file path pattern follows [text](#text) rules while a term with a file identifier follows [numeric](#numeric) rules.

2. NodeKind Attribute

Events using `NodeKind` can be filtered by either the numerical value (0, 1, or 2) or the text equivalent (*branch*, *leaf*, *blob*, respectively). Text is converted to the numeric equivalent, and numeric values are compared. With only three valid values, number ranges are not supported but all other numeric comparisons are available.

Remember that `IndexPayload` does not use `NodeKind` but has an implied logical value of `LeafNode`. Likewise for `FileInformation`, even though the value has no meaning for the meta event. `NodeKind` cannot, by itself, be used to block all index events for leaf nodes.

3. TraceID Attribute

All events may include a `TraceId` attribute, subject to options specified when recording started. Many, but not all, events are recorded within the scope of `QueryStart` and `QueryStop` events, which means the `TraceId` is associated with a `ServiceName` attribute. `TraceId` filter terms can include service names, resulting in the acceptance of all `TraceId` values associated with the named services.

Where the distinction between `FileId` and `Path` is naturally disambiguated by their data types, both `TraceId` and `ServiceName` are text. Term tokens are not disambiguated. A token pattern that matches both a trace ID and a service name will accept both the ID and name.

### IEventModel

An extension of `IEventVisitationLink`, this abstraction describes visitors whose purpose is to more broadly transform the event stream. Possible changes include:
- event suppression, as with a filter (e.g., an `IndexEviction` might be discarded); or
- attribute updates (e.g., an `ExpandTime` may be replaced with an estimate assuming a different decompression algorithm); or
- event reclassification (e.g., an `IndexCacheHit` may become an `IndexCacheMiss`); or
- event creation (e.g., an `IndexLoad` may be required after reclassifying `IndexCacheHit` to `IndexCacheMiss`).

#### CIndexEventModel

A concrete implementation of `IEventModel` that acts on events in the `EventCtxIndex` event context. All other events are propagated without change.

The options for stream transformation are extensive.
- `ReadTime` values are replaced with estimates defined for alternate storage planes. Branch and leaf nodes for each file may be assigned a storage plane. _This is currently mandatory, but is so only because it was the first transformation to be implemented. Should it remain mandatory, or should it become optional like other transformations?_
- `ReadTime` values may be replaced instead by an estimate assuming the use of a page cache.
- `InMemorySize` and `ExpandTime` values may be replaced with estimates as if a different compression algorithm had been used in the original event. One set of estimates per index node kind is supported, so branch and leaf nodes can be compressed with different algorithms.
- `InMemorySize` and `ExpandTime` values may be changed to simulate different decompression strategies. For events recorded when node decompression is reported with `IndexLoad` events, the effects of decompression may be deferred to `IndexPayload` events. This results in reduced node cache size and avoids unnecessary decompression, but with the risk of repeated decompressions. This can be enabled separately for branch and leaf nodes.
- The effects of changing the node cache size can be simulated, with different limits for branch and leaf nodes. This option radically alters the event stream:
  - An `IndexCacheHit` event may be replaced by an `IndexCacheMiss`, `IndexLoad`, and zero or more `IndexEviction` events.
  - An `IndexCacheMiss` event may be replaced by an `IndexCacheHit` event. The next `IndexLoad` event for the same node will then be suppressed.
  - And `IndexLoad` event may be followed by one or more `IndexEviction` events.
  - An input `IndexEviction` event is always suppressed.

Some events may be discarded by the model if they are observed when unexpected. Modeling and *on-demand* node expansion from *on-load* events and custom node cache sizes imposes a need for information made available in `IndexCacheHit` and `IndexLoad` events. `IndexLoad` events are expected to follow `IndexCacheMiss` events. `IndexLoad` events not preceded by either a cache hit or miss for the same node are discarded. `IndexPayload` and `IndexEviction` events not preceded by either a cache hit or the combination of a cache miss and load for the same node are discarded.

Model options are specified using a property tree. The configuration conforms to this format:

    @kind

An optional identifier that must be `index-events` when present.

    storage/

Required container for storage plane and page cache configuration settings.

    storage/
        @cacheReadTime
        @cacheCapacity

Optional page cache controls. Setting a positive read time enables the page cache. Setting a positive capacity limits the total size of cached pages. Note that unlike a real page cache, which would cache the entire page, the model merely tracks which 8 KB pages are cached based on file ID and offset.

**If not zero, the capacity must be at least 8 KB. It is an error to configure the page cache such that it cannot cache at least one page.**

    storage/
        @dynamicCacheCapacity

The page cache capacity can be dynamically configured relative to the combined sizes of all in-memory node caches. When `@cacheReadTime` enables the page cache, when `@cacheCapacity` is omitted (or zero), and the sum of `//memory/node/@cacheCapacity` is less than `@dynamicCacheCapacity`, the page cache capacity is set to the difference of `@dynamicCacheCapacity` and the sum of `//memory/node/@cacheCapacity`. This property cannot enable the page cache, nor can it override an explicitly defined capacity. If the capacity would be set to a non-zero value less than the size of a page, making the cache incapable of holding a page, the capacity instead remains zero.

Consider this example:
```yaml
storage:
  cacheReadTime: 10000
  dynamicCacheCapacity: 40 MiB
memory:
  node:
  - kind: 0
    cacheCapacity: 0.5 MiB
  - kind: 1
    cacheCapacity: 20 MiB
```
In this example, the node caches are allotted 20.5 MiB. The page cache is dynamically allotted the balance of 40 MiB, or 19.5 MiB.

    storage/
        plane/

Required and repeatable element container for a single storage plane. The first occurrence becomes the default plane for any file that is not explicitly assigned a plane.

    storage/
        plane/
            @name

Required unique identifier through which files identify the plane or planes they use.

    storage/
        plane/
            @readTime

Required number of nanoseconds needed to load a single 8KB page from the storage device.

    storage/
        file/

Optional repeatable element enabling one or all files to be explicitly associated with a storage plane.

    storage/
        file/
            @path

Optional unique file path identifying the physical file being configured. When present, the value must match the value presented in the`Path` attribute of a `FileInformation` meta event. Omission, or empty, applies this configuration to all files not explicitly configured in a sibling element.

    storage/
        file/
            @plane

Optional storage plane name identifies the storage device on which the file is stored. When present, the value must match one of the configured storage plane names. Omission, or empty, associates the file or files to the default plane

    storage/
        file/
            @branchPlane
            @leafPlane
            @blobPlane

Optional storage plane names associating all nodes of a given kind to a storage plane. When present, the value must match one of the configured storage plane names. Omission, or empty, with the default plane for the file.

    memory/

Optional container for decompression performance estimates, decompression strategies, and node cache settings.

    memory/
        node/

The optional configuration of memory expansion modeling for each index node kind:
- Expansion mode modeling may override the default `expansionMode` by including a `expansionMode` property. The default is inherited from the parent element.
- Expansion estimation must include both `sizeFactor` and `sizeToTimeFactor` properties. No defaults are defined. These properties must be specified for every node kind to enable estimation.
- Node cache size control must include a `cacheCapacity` property.

    memory/
        node/
            @kind

One of zero or "branch" for branch nodes. One of one or "leaf" for leaf nodes. One of two or "blob" for blob nodes.

In all cases, the numeric value takes precedence over the text equivalent. A `node` without `@kind` is ignored.

    memory/
        node/
            @sizeFactor
            @sizeToTimeFactor

Optional controls for decompression performance estimates for the desginated node kind. To enable compression estimates, both values must be specified for all node types. Both values must be omitted for all node types to proceed without estimations. Every other combination of these values is an error.

Both values are positive decimal (i.e., floating point) numbers. The estimated `InMemorySize` for a decompressed node is the product of the size factor and 8 KB. The estimated `ExpandTime` for the same node is the product of the estimated `InMemorySize` and the size to time factor.

    memory/
        node/
            @expansionMode

Optional choice of:
- `ll`: The model input and output are both assumed to represent node expansion on-load. This is the default.
- `ld`: The model input is assumed to represent node expansion on-load, and the model output represents node-expansion on-demand.
- `dd`: The model input and output are both assumed to represent node expansion on-demand.

This does not apply to branch nodes, which always use *on-load* expansion.

    memory/
        node/
            @cacheCapacity

Optional upper limit, in bytes, for a node cache for nodes of the desginated kind. Omission, empty, or zero, prevents the model from caching. Note the discrepancy between this capacity and the page cache capacity reflects the page cache's requirement for a separately configured time duration. Note that unlike a real node cache, which would cache the entire page, the model caches the size, in bytes, of the node as it is stored in memory.

**If not zero, the capacity must be at least the size of the largest node size. It is an error to configure the page cache such that it cannot cache at least one page, but the error will not be detected until an offending node is observed.**

    memory/
        observed/

Optional and repeatable element to be used by unit tests to pre-populate the memory model's internal data. Its use simplifies the establishment of cache preconditions when evaluating the effects of the model's cache on the event stream.

Each instance represents one loaded page. The data has value for both alternate decompression strategies and node caching.

It is worth noting that element attributes follow the title case naming convention instead of the camel case used in the rest of the configuration. This is intentional, as the names match the event attribute names from which the model would normally be updated.

    memory/
        observed/
            @NodeKind
            @FileId
            @FileOffset

These identify the node and the cache to which it belongs. The node may be the same as or differ from nodes referenced by input events.

    memory/
        observed/
            @ExpandTime

The number of nanoseconds required to expand the node one time. For nodes also referenced in input events, this value may differ from what is used in those events. The model may always replace the value with an estimated value. If not replaced, the model accepts that decompression times can vary based on system load and always keeps the last observed time.

    memory/
        observed/
            @InMemorySize

If algorithm estimation is enabled, the value is replaced. If the node will not be referenced by model input events, the value can be any positive integral value. Otherwise, the value must match the first non-zero `InMemorySize` value observed in an input event. Node sizes are not expected to change between observations, and the model does not account for unexpected changes. Once a node is in the cache with size `X`, it must always have size `X`.

### CMetaInfoState::CCollector

`CMetaInfoState` provides a runtime repository of event-defined state information, including values from the `FileInformation` and `QueryStart` events. As a concrete implementation of `IEventVisitationLink`, `CCollector` is a companion class that extracts event data for the repository.

To avoid data loss, this link must precede all other links. It is the responsibility of event iterating code to ensure this requirement is satisfied.

The repository is available to all operations and all other visitors. The collector itself is not.

# CEventVisitationLinkTester

A concrete implementation of `IEventVisitor` used to test iterated events. Given an *expected* event iterator, each visited event is compared with the next expected event. Any discrepancy is a unit test failure.

# CEventConsumingOp

An abstract base class for handling requests to traverse and act upon an event data file. It provides common handling of an input event file name, an output stream for text output, an optional single occurrence of event filters, an optional single event model, and one CMetaInfoState. When using the traverseEvents method, the meta state instance is guaranteed to be the first visitor in the visitation chain, before the optional event filter.

Whether events are filtered before or after an event model is hard coded. Filtering applies after a model so model-generated events can be filtered. Subclasses needing more control are responsible for hiding the relevant base methods.

## CIndexPlotOp

Produces one or more 2D or 3D chart data sets from an input event data file using a configured index event model and explicit model modifications.

A 2D chart data set accepts model modifications for the X-axis. A 3D chart data set accepts model modifications for the Y-axis. The value of each data set cell is computed by traversing the input file after applying cell-specific configuration changes to the model. A 2D chart with 5 X values will traverse the input file 5 times. A 3D chart with 5 X values and 6 Y values will traverse the input file 30 times.

Most plot options are specified using a property tree. This includes optional event filtering and an index events model. The inherited filter and model handling is suppressed. The configuration format is:

    @name

Recommended identifier denoting the operation for which the configuration applies. As the initial operation supporting this configuration scheme, the value is optional. Given the potential for other operations to use this scheme, the value `index.plot` should be used. Any other value is invalid.

    @input

Optional file path to a binary event file. Event input is required, and may be specified here or using the base class interface.

    @valueSelector

Required designation of which value will be accumulated for each plot. Must be one of these case insensitive choices:
- readTime: the ReadTime attribute from IndexLoad events is accumulated
- expandTime: the ExpandTime attribute from IndexLoad events is accumulated
- elapsedTime: the sum of the ReadTime and ExpandTime attributes from IndexLoad events is accumulated
- cacheMisses: the number of IndexCacheMiss events is counted

    link/

Required repeatable element specifying a valid visitation link configuration. Each supported visitiation link configuration is described elsewhere in this document. This section focuses on values needed by the operation.

    link/@kind
    link/@id

A link kind controls which visitation link class to create. Its id distinguishes instances of link configurations with the same kind. Although unlikely, configuring both pre-model and post-model event filters is permitted. Both values are conditionally required.

If `@kind` is omitted, an [index model](#cindexeventmodel) configuration is assumed. To configure an [event filter](#ceventfilter), the value must be given as `event-filter`.

An `@id` is required only when `@kind` does not uniquely identify the link configuration instance and the operation intends to manipulate the configuration.

A link configuration node is identified by combining the two values using a dotted notation like `<kind> [ '.' <id> ]`.

    plot/

Optional and repeatable element where each instance describes link configuration changes to be made for every value in a plot. This enables one invocation of the operation to produce multiple chart data sets, each showing the same data relationship but with different modeled pre-conditions. While a 2D chart configuration could be converted to a 3D chart to show the same data in one chart, a 3D chart cannot add another axis.

    plot/
        delta/

Required and repeatable element where each instance describes a single link configuration change to be made for every cell in a chart data set.

    plot/
        delta/
            @linkId

Optional designation of the link configuration targeted by the change. If omitted, `index-events` is assumed.

    plot/
        delta/
            @xpath

Required link configuration datum targeted by the change.

    plot/
        delta/
            @value

Optional changed value. Omission removes the targeted datum, while presence updates it.

    x-axis/

Required element describing link configuration changes required to compute each X-axis value in a chart data set.

    x-axis/
        iteration/

Required element describing link configuration changes required to compute a single X-axis value of a chart data set.

    x-axis/
        iteration/
            @label

Optional human readable description of the changes. When not-empty, the value is used as a column header in chart data sets.

Optional human readable description of the changes.
- When not-empty, the value is used as a row header in chart data sets.
- When empty, the row header text depends on the described change:
  - If one `delta` is defined, `delta/@value` is used as the header text. If the value is can be converted to a byte count, the byte count is used.
  - The use of multiple `delta`s makes the header text "ambiguous".

    x-axis/
        iteration/
            delta/

Required and repeatable element where each instance describes a single link configuration change to be made for each cell in a column.

    x-axis/
        iteration/
            delta/
                @linkId

Optional designation of the link configuration targeted by the change. If omitted, `index-events` is assumed.

    x-axis/
        iteration/
            delta/
                @xpath

Required link configuration datum targeted by the change.

    x-axis/
        iteration/
            delta/
                @value

Optional changed value. Omission removes the targeted datum, while presence updates it.

    y-axis/

Optional element describing link configuration changes required to compute each Y-axis value in a 3D chart data set.

    y-axis/
        iteration/

Required element describing link configuration changes required to compute a single Y-axis value of a chart data set.

    y-axis/
        iteration/
            @label

Optional human readable description of the changes.
- When not-empty, the value is used as a row header in chart data sets.
- When empty, the row header text depends on the described change:
  - If one `delta` is defined, `delta/@value` is used as the header text. If the value is can be converted to a byte count, the byte count is used.
  - The use of multiple `delta`s makes the header text "ambiguous".

    y-axis/
        iteration/
            delta/

Required and repeatable element where each instance describes a single link configuration change to be made for each cell in a row.

    y-axis/
        iteration/
            delta/
                @linkId

Optional designation of the link configuration targeted by the change. If omitted, `index-events` is assumed.

    y-axis/
        iteration/
            delta/
                @xpath

Required link configuration datum targeted by the change.

    y-axis/
        iteration/
            delta/
                @value

Optional changed value. Omission removes the targeted datum, while presence updates it.

## CIndexFileSummary

A concrete implementation of `CEventConsumingOp` that collects statistics about the index events it visits. As the previous statement suggests, it is also an event visitor although this is a private implementation detail.

Statistics are collected per index node. Values include memory usage, event counters, read timing (total, minimum, maximum, and average read time), and expansion timing (total, minimum, maximum, and average).

The index node statistics can be aggregated by file ID or node kind. Grouping by file ID reveals branch and leaf activity for every observed file. Grouping by node kind provides model configuration guidance for cache sizes, expansion estimations, and storage plane performance.

## TestOp

A concrete implementations of `CEventConsumingOp` used with unit tests. Defined within `testEventVisitationLinks`, the `doOp` method assembles the configured visitation chain and iterates the configured input events. The visitation chain begins with the inherited [meta state](#cmetainfostate) link, any explicitly configured links, and ends with the [test visitor](#ceventvisitationlinktester).

The only base functionality used by this class is the meta state link. The model and filter instances are ignored, as are the input file path and output stream.

## StubOp

A concrete implementation of `CEventConsumingOp` that does nothing. It exists as a placeholder in unit tests that directly instantiate visitation links that require an operation. Attempting to use the operation is an error that throws an exception.

# IEventIterator

Where `IEventVisitor` has events pushed to it, this interface allows its user to pull events from a source on demand. The `IEventReader` interface in `/system/jlib/jevent.hpp` describes an event pulling mechanism. This is an abstraction modeled on `IEventReader` for pulling events from arbitrary event sources.

Iteration occurs using the `nextEvent` method. Other methods provide access to information provided by `IEventVisitor::visitFile` and `IEventVisitor::departFile`.

Whereas standard library iteratorss tend to feature begin and end semantics, and jlib iterators use first and next, the event iterator relies solely on `nextEvent`. Event file consumers, of which the file iterator is one example, offer only a `nextEvent` option because they don't go back to previously read content. Iteration, then, can be implemented as

```cpp
    while (iter.nextEvent(event))
        visitor.visitEvent(event);
```
Note that this snippet assumes the visitor will not attempt to abort event traversal by returning *false*. A check for *false* can easily be included if needed.

## CEventMultiplexer

Implementation of `IEventIterator` that uses other iterators as event sources, merging events from each source into a single output event stream in chronological order.

Merging events from multiple sources is not intuitive, at least not when index events are included. The file ID associated with an index file path cannot be predicted. Assuming two event inputs that reference paths `foo` and `bar`, the file IDs associated with `foo` may not be the same in each input. In fact, the ID's for `foo` and `bar` could be swapped between inputs.

To address this, within each input each distinct FileId attribute value is replaced with a unique number. In other words, within an input file every occurrence of FileId `N` is replaced with `M`, and across all inputs substitute value `M` is only used with one index file.
