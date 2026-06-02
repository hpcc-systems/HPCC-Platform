# Selector and Scope Mechanics

## Overview

In eclcc, **selectors** are expressions that identify which dataset row is being accessed. They are the foundation of ECL's field-access semantics: when you write `LEFT.name`, the compiler represents this as a `no_select` node whose left-hand-side is a `no_left` selector. Understanding selectors is essential because nearly every transform pass must correctly propagate, rebind, or normalize them.

**Key source files:**
- `ecl/hql/hqlexpr.cpp` — Selector creation, normalized selectors, `createSelectExpr`
- `ecl/hql/hqlexpr.ipp` — `CHqlSelectBaseExpression`, `CHqlSelectExpression`, `CHqlNormalizedSelectExpression`
- `ecl/hql/hqltrans.cpp` — `transformSelector`, `createTransformedActiveSelect`, `initializeActiveSelector`, `ScopedTransformer`
- `ecl/hql/hqltrans.ipp` — `ScopeInfo`, `ScopedTransformer` class definition
- `ecl/hql/hqlexpr.hpp` — `querySelSeq`, `USE_SELSEQ_UID`

---

## Selector Types

### LEFT / RIGHT / SELF / TOP

These are the **pseudo-dataset** selectors that name the "active" row within an operation:

| Selector | Operator | Usage |
|----------|----------|-------|
| `no_left` | LEFT | Input row in PROJECT, TRANSFORM, left side of JOIN |
| `no_right` | RIGHT | Right side of JOIN, DENORMALIZE |
| `no_self` | SELF | Target row being constructed in TRANSFORM |
| `no_selfref` | SELF (ref) | Self-reference in recursive contexts |
| `no_top` | TOP | Parent dataset in nested operations |
| `no_activetable` | (implicit) | Anonymous active table selector |

Each is created as a **row** expression:
```cpp
IHqlExpression * createSelector(node_operator op, IHqlExpression * ds, IHqlExpression * seq)
{
    IHqlExpression * record = ds->queryRecord()->queryBody();
    switch (op) {
    case no_left:
    case no_right:
    case no_top:
        assertex(seq && seq->isAttribute());
        return createRow(op, LINK(record), LINK(seq));  // row typed by record + selector sequence
    case no_self:
        return createRow(op, LINK(record), LINK(seq));
    case no_activetable:
        return LINK(cachedActiveTableExpr);             // singleton
    }
}
```

### The `selSeq` (Selector Sequence)

The **selector sequence** (`_selectorSequence_Atom`) is a unique attribute attached to operators that introduce LEFT/RIGHT scope (JOINs, PROJECTs, ITERATE, etc.). It disambiguates nested uses of LEFT/RIGHT:

```ecl
// Without selSeq, the inner LEFT would conflict with the outer LEFT
JOIN(ds1, ds2,
    LEFT.id = RIGHT.id,        // outer LEFT/RIGHT (selSeq #1)
    TRANSFORM(
        JOIN(LEFT.children, RIGHT.children,   // inner LEFT/RIGHT (selSeq #2)
            LEFT.key = RIGHT.key,
            ...
        )
    )
)
```

```cpp
// Always enabled:
#define USE_SELSEQ_UID

IHqlExpression * createSelectorSequence()
{
    return createUniqueSelectorSequence();  // monotonically increasing unique ID
}

// Retrieved from an expression via:
inline IHqlExpression * querySelSeq(IHqlExpression * expr)
{
    return expr->queryAttribute(_selectorSequence_Atom);
}
```

**Key property:** Two LEFT selectors with different `selSeq` values are *different expressions* — they will not be commoned up by hash-consing. This is what allows nested scopes to coexist.

---

## The `no_select` Expression

A field access like `LEFT.name` is represented as:

```
no_select(LEFT_selector, field_expr [, newAtom])
```

Where:
- Operand 0: the dataset/row selector (e.g., a `no_left` node)
- Operand 1: the field expression (`no_field`)
- Optional operand 2: `attr(newAtom)` — marks this as a "new" selector (accessing out-of-scope dataset)

### Specialized Select Classes

To save memory (selects are among the most numerous nodes), `no_select` has its own class hierarchy:

```
CHqlSelectBaseExpression (no type pointer — type derived from field)
├── CHqlNormalizedSelectExpression  (selector IS already normalized)
└── CHqlSelectExpression            (has separate `normalized` member)
```

```cpp
IHqlExpression * CHqlSelectBaseExpression::makeSelectExpression(IHqlExpression * left, IHqlExpression * right, IHqlExpression * attr)
{
    IHqlExpression * normalizedLeft = left->queryNormalizedSelector();
    bool needNormalize = (normalizedLeft != left) || (attr && attr->queryName() == newAtom);

    CHqlSelectBaseExpression * select;
    if (needNormalize)
        select = new CHqlSelectExpression;        // stores normalized form separately
    else
        select = new CHqlNormalizedSelectExpression;  // IS its own normalized form
    select->setOperands(left, right, attr);
    select->calcNormalized();
    return select->closeExpr();
}
```

---

## Normalized Selectors

### The Problem

Consider `ds.field` where `ds` is an activity. The *active* select refers to the currently-in-scope row of `ds`, while the *new* select `ds.field<new>` refers to "go read `ds` independently." Both access the same field, but in different contexts.

A **normalized selector** strips away context-dependent information to yield a canonical form suitable for comparison:

```
Active:     no_select(ds, field)           → normalizedSelector = this (already canonical)
New:        no_select(ds, field, newAtom)  → normalizedSelector = no_select(ds_normalized, field)
```

### Implementation

```cpp
// For CHqlNormalizedSelectExpression — it IS its own normalized form:
IHqlExpression * CHqlNormalizedSelectExpression::queryNormalizedSelector() { return this; }

// For CHqlSelectExpression — has a separate normalized member:
IHqlExpression * CHqlSelectExpression::queryNormalizedSelector()
{
    if (normalized) return normalized;
    return this;  // fallback
}

void CHqlSelectExpression::calcNormalized()
{
    normalized.setown(calcNormalizedSelector());
    assertex(normalized);
}

// The normalization logic:
IHqlExpression * CHqlExpression::calcNormalizedSelector() const
{
    IHqlExpression * left = &operands.item(0);
    IHqlExpression * normalizedLeft = left->queryNormalizedSelector();

    // Normalized selector has exactly 2 args — no newAtom, normalized lhs
    if ((normalizedLeft != left) || (operands.ordinality() > 2))
    {
        HqlExprArray args;
        args.append(*LINK(normalizedLeft));
        args.append(OLINK(operands.item(1)));
        return doCreateSelectExpr(args);
    }
    return NULL;  // already normalized
}
```

### Why This Matters for Transforms

Transforms must track selector mappings using **normalized** selectors. When `transformSelector()` is called, it first normalizes:

```cpp
IHqlExpression * NewHqlTransformer::transformSelector(IHqlExpression * expr)
{
    IHqlExpression * normalized = expr->queryNormalizedSelector();
    IHqlExpression * transformedSelector = queryAlreadyTransformedSelector(normalized);
    if (transformedSelector) {
        if (transformedSelector->getOperator() == no_activerow)
            return LINK(transformedSelector->queryChild(0));
        return LINK(transformedSelector);
    }
    // ... fallback to full transform
}
```

---

## Active vs New Selectors

### `isSelectRootAndActive()`

This inline (in `CHqlExpression`) determines whether a `no_select` node represents an "active" row access — i.e., the dataset is currently in scope:

```cpp
inline bool isSelectRootAndActive() const
{
    dbgassertex(op == no_select);
    if (hasAttribute(newAtom))
        return false;                    // new selectors are NOT active
    IHqlExpression * ds = queryChild(0);
    if (ds->isDatarow()) {
        switch (ds->getOperator()) {
        case no_typetransfer: return true;  // hack for HPCC-21084
        }
        return false;                    // row.field — not a root select
    }
    return true;                         // dataset.field — root active select
}
```

### `normalizeSelectLhs()`

Normalizes the left-hand side of a select, unwinding wrapper nodes:

```cpp
inline IHqlExpression * normalizeSelectLhs(IHqlExpression * lhs, bool & isNew)
{
    for (;;) {
        switch (lhs->getOperator()) {
        case no_newrow:                  // unwrap, mark as new
            isNew = true;
            lhs = lhs->queryChild(0);
            break;
        case no_activerow:               // unwrap, mark as active
            isNew = false;
            lhs = lhs->queryChild(0);
            break;
        case no_left: case no_right:     // pseudo-selectors are always active
        case no_top: case no_activetable:
        case no_self: case no_selfref:
            isNew = false;
            return lhs;
        case no_select:                  // nested select — check if always active
            if (isNew && isAlwaysActiveRow(lhs))
                isNew = false;
            return lhs;
        default:
            return lhs;
        }
    }
}
```

### `createSelectExpr()`

The main select creation function normalizes the LHS and dispatches to the appropriate factory:

```cpp
extern IHqlExpression * createSelectExpr(IHqlExpression * _lhs, IHqlExpression * rhs, bool _isNew)
{
    bool isNew = _isNew;
    IHqlExpression * normalLhs = normalizeSelectLhs(lhs, isNew);
    IHqlExpression * newAttr = isNew ? newSelectAttrExpr : NULL;

    // Route based on field type:
    type_t t = rhs->queryType()->getTypeCode();
    if (t == type_table || t == type_groupedtable)
        return createDataset(no_select, { LINK(normalLhs), rhs, LINK(newAttr) });
    if (t == type_dictionary)
        return createDictionary(no_select, ...);
    if (t == type_row)
        return createRow(no_select, { LINK(normalLhs), rhs, LINK(newAttr) });

    return CHqlSelectBaseExpression::makeSelectExpression(LINK(normalLhs), rhs, LINK(newAttr));
}
```

---

## Scope Tracking in Transforms

### `ScopeInfo`

Each level of nesting maintains a `ScopeInfo` recording what datasets are in scope:

```cpp
class ScopeInfo : public CInterface
{
    IHqlExpression * context;        // the expression that introduced this scope
    HqlExprAttr dataset;             // the active dataset (e.g., input to PROJECT)
    HqlExprAttr transformedDataset;  // the transformed version of dataset
    HqlExprAttr left;                // dataset that LEFT refers to
    HqlExprAttr right;               // dataset that RIGHT refers to
    HqlExprAttr seq;                 // the selector sequence for this scope
    bool isWithin;
};
```

### `ScopedTransformer`

Maintains a stack of `ScopeInfo` objects representing the nesting of active datasets:

```cpp
class ScopedTransformer : public NewHqlTransformer
{
    CIArrayOf<ScopeInfo> scopeStack;
    ScopeInfo * innerScope;          // top of stack (fast access)
};
```

### Scope Binding During Transform

When a `ScopedTransformer` encounters an activity like `no_join`:

```
1. pushScope(expr)                    → new ScopeInfo on stack
2. transform(left_dataset)            → transform the left input
3. transform(right_dataset)           → transform the right input
4. setLeftRight(left, right, selSeq)  → binds LEFT and RIGHT in scope
5. transform(condition)               → transforms with LEFT/RIGHT accessible
6. transform(transform_expr)          → transforms with SELF/LEFT/RIGHT accessible
7. clearDataset(nested)
8. popScope()
```

The `getChildDatasetType()` function categorizes each operator's scope pattern:

| Pattern | Example | Scope Setup |
|---------|---------|-------------|
| `childdataset_dataset` | FILTER | `setDataset(ds, transformed)` |
| `childdataset_datasetleft` | PROJECT | `setDatasetLeft(ds, transformed, selSeq)` |
| `childdataset_left` | NORMALIZE | `setLeft(left, selSeq)` |
| `childdataset_leftright` | JOIN | `setLeftRight(left, right, selSeq)` |
| `childdataset_top_left_right` | DENORMALIZE | `setTopLeftRight(ds, transformed, selSeq)` |
| `childdataset_same_left_right` | SELFJOIN | `setLeftRight(left, left, selSeq)` |

### `checkInScope()`

Determines whether a selector is accessible in the current scope:

```cpp
bool ScopedTransformer::checkInScope(IHqlExpression * selector, bool allowCreate)
{
    switch (selector->getOperator()) {
    case no_left: case no_right: case no_self:
    case no_selfref: case no_activetable:
        return true;    // pseudo-selectors are always in scope
    case no_select:
        if (selector->isDataset()) break;
        return checkInScope(selector->queryChild(0), allowCreate);  // recurse up
    }

    // Walk scope stack looking for matching dataset
    IHqlExpression * normalized = selector->queryNormalizedSelector();
    ForEachItemInRev(idx, scopeStack) {
        ScopeInfo & cur = scopeStack.item(idx);
        if (cur.dataset && cur.dataset->queryNormalizedSelector() == normalized)
            return true;
    }
    return false;
}
```

---

## Transform Selector Mapping

### `transformSelector()`

The entry point for transforming a selector. Different from `transform()` because selectors may have different mappings than their underlying expressions:

```cpp
IHqlExpression * NewHqlTransformer::transformSelector(IHqlExpression * expr)
{
    IHqlExpression * normalized = expr->queryNormalizedSelector();

    // Check if this normalized selector has an explicit mapping
    IHqlExpression * transformedSelector = queryAlreadyTransformedSelector(normalized);
    if (transformedSelector) {
        if (transformedSelector->getOperator() == no_activerow)
            return LINK(transformedSelector->queryChild(0));  // unwrap activerow
        return LINK(transformedSelector);
    }

    // Try the general transform cache
    IHqlExpression * transformed = queryAlreadyTransformed(normalized);
    if (transformed)
        transformed = LINK(transformed->queryNormalizedSelector());
    else
        transformed = createTransformedSelector(normalized);

    setTransformedSelector(normalized, transformed);
    return transformed;
}
```

### `createTransformedActiveSelect()`

When a `no_select` selector needs transforming, this rebuilds it with the transformed LHS:

```cpp
IHqlExpression * NewHqlTransformer::createTransformedActiveSelect(IHqlExpression * expr)
{
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    OwnedHqlExpr newLeft = transformSelector(left);   // recursively transform LHS
    IHqlExpression * newRight = right;                 // fields don't change by default

    IHqlExpression * normLeft = left->queryNormalizedSelector();
    if ((normLeft == newLeft) && (newRight == right))
        return LINK(expr->queryNormalizedSelector());  // nothing changed

    // Check if field has been remapped (e.g., record change)
    OwnedHqlExpr mappedRight = lookupNewSelectedField(newLeft, right);
    if (mappedRight && newRight != mappedRight)
        newRight = mappedRight;

    if (newLeft->getOperator() == no_newrow)
        return createNewSelectExpr(LINK(newLeft->queryChild(0)), LINK(newRight));

    return createSelectExpr(LINK(newLeft), LINK(newRight));
}
```

### `initializeActiveSelector()`

When a transform introduces a new scope (e.g., transforms a dataset), it must record the selector mapping so that references to that dataset in child expressions are correctly remapped:

```cpp
void NewHqlTransformer::initializeActiveSelector(IHqlExpression * expr, IHqlExpression * transformed)
{
    for (;;) {
        // Map the normalized selector of expr → normalized selector of transformed
        setTransformedSelector(expr->queryNormalizedSelector(),
                               transformed->queryNormalizedSelector());

        // Walk up parent dataset chain
        if (!expr->queryDataset()) return;
        IHqlExpression * root = queryExpression(expr->queryDataset()->queryRootTable());
        if (!root || root->getOperator() != no_select) return;
        // ... continue mapping parent selectors
    }
}
```

---

## Table Gathering for Selectors

Select expressions participate in table-use tracking to determine scope dependencies:

```cpp
void CHqlSelectBaseExpression::gatherTablesUsed(CUsedTablesBuilder & used)
{
    IHqlExpression * ds = queryChild(0);
    if (isSelectRootAndActive()) {
        used.addActiveTable(ds);     // this select depends on ds being in scope
    } else {
        ds->gatherTablesUsed(used);  // propagate up for nested selects
    }
}

bool CHqlSelectBaseExpression::usesSelector(IHqlExpression * selector)
{
    IHqlExpression * ds = queryChild(0);
    if (isSelectRootAndActive()) {
        return (selector == ds);     // direct pointer comparison
    } else {
        return ds->usesSelector(selector);  // recurse
    }
}
```

---

## Common Pitfalls and Invariants

### 1. Always normalize before comparing selectors
Never compare selectors by raw pointer unless both are guaranteed normalized. Use `queryNormalizedSelector()`.

### 2. selSeq must be preserved through transforms
If a transform changes a dataset but keeps its LEFT/RIGHT structure, the selSeq must be carried to the new expression. Losing the selSeq breaks selector disambiguation.

### 3. `no_newrow` / `no_activerow` wrapping
- `no_newrow(x)` means "access x as a new (out-of-scope) dataset"
- `no_activerow(x)` means "access x as an in-scope row"
- Transforms that change a selector from active→new must wrap in `no_newrow`
- Transforms must unwrap `no_activerow` when the result is used as a selector

### 4. Annotations on selectors
Selectors should be accessed via `queryBody()` when checking identity. Annotations on selector expressions can cause false mismatches.

### 5. Scope suspension
`suspendAllScopes()` / `restoreScopes()` is used for expressions evaluated globally (e.g., `no_colon`, `no_globalscope`) where no active datasets should be visible.

### 6. Self-join selectors
For `SELFJOIN`, both LEFT and RIGHT refer to the same dataset. The selSeq differentiates them from other uses in the expression tree.

---

## Summary of Key Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `createSelector()` | hqlexpr.cpp:16327 | Create LEFT/RIGHT/SELF pseudo-selectors |
| `createSelectorSequence()` | hqlexpr.cpp:16397 | Generate unique selSeq disambiguation attribute |
| `createSelectExpr()` | hqlexpr.cpp:13271 | Create field-access expression with normalization |
| `queryNormalizedSelector()` | hqlexpr.ipp | Get canonical form of a selector |
| `calcNormalizedSelector()` | hqlexpr.cpp:4036 | Compute the normalized form |
| `isSelectRootAndActive()` | hqlexpr.ipp:226 | Is this an active root dataset.field? |
| `normalizeSelectLhs()` | hqlexpr.cpp:13194 | Unwrap newrow/activerow from LHS |
| `transformSelector()` | hqltrans.cpp:1644 | Transform a selector through a pass |
| `createTransformedActiveSelect()` | hqltrans.cpp:1689 | Rebuild a select with transformed LHS |
| `initializeActiveSelector()` | hqltrans.cpp:1753 | Register selector mapping for scope |
| `checkInScope()` | hqltrans.cpp:4519 | Check if a selector is in current scope |
| `pushScope()` / `popScope()` | hqltrans.cpp:4415 | Manage scope stack |
| `setLeftRight()` | hqltrans.ipp:1138 | Bind LEFT/RIGHT for current scope |
| `querySelSeq()` | hqlexpr.hpp:1720 | Extract selSeq from an operator expression |
