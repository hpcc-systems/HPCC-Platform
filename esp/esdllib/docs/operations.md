### Operations

If an ESDL script is a sequence of instructions to produce a desired outcome based on a given input, an operation is one instruction in the sequence. Operations are provided to steer script execution in the right direction, manipulate the environment in which other operations are processed, as well as to manipulate the data used in a transaction.

> In this context, the term *transaction* is used to refer to the transformation of input data to output data. An ESP generating a response to an external request, a task commonly referred to as a transaction, is but one example of the broader meaning intended here.

Operations are defined as XML elements, with or without children. An operation described as *childless* expects and accepts no child elements. Each operation accepting children will specify not only which children are permitted, but also any interactions between itself and its children.

All operations may include a small number of standard attributes used for debugging.

| Property | Count | Description |
| :- | :-: | :- |
| optional | 0..1 | Provided for legacy support, this controls whether failures initializing an operation are merely errors logged to trace output (*true*) or also cause exceptions (*false*).<br/><br/>Default is *false*.<br/><br/>Use of this flag is strongly discouraged, as undetected errors can make script results unpredictable. The recommended best practice is to keep the exceptions enabled so script syntax errors can be found and fixed as early as possible. |
| trace | 0..1 | Label used in trace log output messages.<br/><br/>The default is empty, although some operations may override with a different property value, such as a name. |
