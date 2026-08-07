# Ledger-Based Processing in HPCC Systems: Auditability + Incremental-by-Default

## Overview

Ledger-based processing models data as an append-only stream of events instead of repeatedly rewriting a single mutable snapshot.  
For many data engineering workloads, this gives you:

- **Preserved audit history** (every change is retained as an event)
- **Incremental processing by default** (process only new events)
- **Lower operational risk** (replay and recovery are straightforward)

This post explains what ledger-based processing is, why it works well in ECL, and includes ECL examples that build on the same `LedgerDemo` module:

1. Define an append-only ledger
2. Derive current state from events
3. Process incrementally using a watermark
4. Reconstruct historical state at any point in time (audit trail time-travel)

## What ledger-based processing is

A ledger keeps every business change as an event, for example:

- Account opened
- Debit posted
- Credit posted
- Account closed

Instead of updating one row in place, you append a new row describing what happened.  
Current state is derived from event history, and point-in-time state is reconstructed by replaying events up to a chosen boundary.

## Why this preserves audit history

With append-only events:

- You keep **who changed what and when**
- You can explain **how** a final value was produced
- You can rebuild prior states for audits, disputes, and debugging

Because old events are not overwritten, the history remains intact and traceable.

## Why incremental processing becomes the default

In a ledger model, new business activity arrives as new events.  
That naturally supports incremental pipelines:

1. Track a watermark/high-water mark (for example, last processed sequence)
2. Read only events newer than that watermark
3. Apply only the new deltas to materialized state
4. Advance the watermark

You avoid full historical recomputation on every run.

---

## ECL Example 1: Define an append-only ledger

```ecl
EXPORT LedgerDemo := MODULE

    EXPORT LedgerEventRec := RECORD
        UNSIGNED4 accountId;
        UNSIGNED8 eventSeq;
        STRING10 eventDate;      // YYYY-MM-DD
        STRING10 eventType;      // OPEN | DEBIT | CREDIT | CLOSE
        DECIMAL10_2 amount;      // Amount for DEBIT/CREDIT
        STRING20 actor;
        STRING40 reason;
    END;

    EXPORT Ledger := DATASET([
        {1001, 1, '2026-01-01', 'OPEN',   0.00, 'ops',    'Account creation'},
        {1001, 2, '2026-01-05', 'CREDIT', 500.00, 'api',  'Initial funding'},
        {1001, 3, '2026-01-08', 'DEBIT',   75.25, 'api',  'Card purchase'},
        {1001, 4, '2026-01-10', 'CREDIT',  25.25, 'api',  'Adjustment'},
        {1002, 5, '2026-01-02', 'OPEN',    0.00, 'ops',   'Account creation'},
        {1002, 6, '2026-01-04', 'CREDIT', 100.00, 'api',  'Payroll'},
        {1002, 7, '2026-01-09', 'DEBIT',   20.00, 'api',  'ATM withdrawal'},
        {1002, 8, '2026-01-11', 'CLOSE',   0.00, 'ops',   'Customer request'}
    ], LedgerEventRec);

END;

OUTPUT(SORT(LedgerDemo.Ledger, accountId, eventSeq), NAMED('LedgerEvents'));
```

**What this shows:** every business change is appended as a new row; no prior row is mutated.

---

## ECL Example 2: Derive current state from ledger history

```ecl
AggState := TABLE(
    LedgerDemo.Ledger,
    {
        accountId,
        DECIMAL10_2 balance := SUM(
            GROUP,
            IF(eventType = 'DEBIT', -amount,
               IF(eventType = 'CREDIT', amount, 0))
        ),
        UNSIGNED1 closedFlag := MAX(GROUP, IF(eventType = 'CLOSE', 1, 0)),
        UNSIGNED8 lastEventSeq := MAX(GROUP, eventSeq)
    },
    accountId
);

CurrentState := PROJECT(
    AggState,
    TRANSFORM(
        RECORD
            RECORDOF(AggState);
            STRING10 status;
        END,
        SELF.status := IF(LEFT.closedFlag = 1, 'CLOSED', 'OPEN');
        SELF := LEFT;
    )
);

OUTPUT(SORT(CurrentState, accountId), NAMED('CurrentStateFromLedger'));
```

**What this shows:** current state is a derived view, not the storage model itself.

---

## ECL Example 3: Incremental update using a watermark

```ecl
// Last successful run processed all events up to sequence 4.
LastProcessedSeq := 4 : STORED('lastProcessedSeq');

PriorStateRec := RECORD
    UNSIGNED4 accountId;
    DECIMAL10_2 balance;
    UNSIGNED8 lastEventSeq;
END;

PriorState := DATASET([
    {1001, 450.00, 4}   // OPEN +500 -75.25 +25.25 = 450.00
], PriorStateRec);

NewEvents := LedgerDemo.Ledger(eventSeq > LastProcessedSeq);

DeltaByAccount := TABLE(
    NewEvents,
    {
        accountId,
        DECIMAL10_2 delta := SUM(
            GROUP,
            IF(eventType = 'DEBIT', -amount,
               IF(eventType = 'CREDIT', amount, 0))
        ),
        UNSIGNED8 maxEventSeq := MAX(GROUP, eventSeq)
    },
    accountId
);

UpdatedExisting := JOIN(
    PriorState,
    DeltaByAccount,
    LEFT.accountId = RIGHT.accountId,
    TRANSFORM(
        PriorStateRec,
        SELF.accountId := LEFT.accountId;
        SELF.balance := LEFT.balance + RIGHT.delta;
        SELF.lastEventSeq := IF(RIGHT.accountId = 0, LEFT.lastEventSeq, RIGHT.maxEventSeq);
    ),
    LEFT OUTER
);

NewAccounts := JOIN(
    DeltaByAccount,
    PriorState,
    LEFT.accountId = RIGHT.accountId,
    TRANSFORM(
        PriorStateRec,
        SELF.accountId := LEFT.accountId;
        SELF.balance := LEFT.delta;
        SELF.lastEventSeq := LEFT.maxEventSeq;
    ),
    LEFT ONLY
);

NextState := SORT(UpdatedExisting + NewAccounts, accountId);
NextWatermark := IF(COUNT(NewEvents) = 0, LastProcessedSeq, MAX(NewEvents, eventSeq));

OUTPUT(SORT(NewEvents, accountId, eventSeq), NAMED('IncrementalEvents'));
OUTPUT(NextState, NAMED('NextMaterializedState'));
OUTPUT(NextWatermark, NAMED('NextWatermark'));
```

**What this shows:** the run consumes only new events and advances the checkpoint.

---

## ECL Example 4: Point-in-time audit trail—reconstructing historical state

```ecl
 // Query: What did account state look like at the end of 2026-01-09?
 TargetDate := '2026-01-09' : STORED('targetDate');
HistoricalEvents := LedgerDemo.Ledger(eventDate <= TargetDate);

HistoricalState := TABLE(
    HistoricalEvents,
    {
        accountId,
        DECIMAL10_2 balance := SUM(
            GROUP,
            IF(eventType = 'DEBIT', -amount,
               IF(eventType = 'CREDIT', amount, 0))
        ),
        UNSIGNED1 openedByDate := MAX(GROUP, IF(eventType = 'OPEN', 1, 0)),
        UNSIGNED1 closedByDate := MAX(GROUP, IF(eventType = 'CLOSE', 1, 0)),
        UNSIGNED8 lastEventSeq := MAX(GROUP, eventSeq)
    },
    accountId
);

HistoricalWithStatus := PROJECT(
    HistoricalState,
    TRANSFORM(
        RECORD
            RECORDOF(HistoricalState);
            STRING20 status;
            STRING10 asOfDate;
        END,
        SELF.status := IF(LEFT.closedByDate = 1, 'CLOSED',
                         IF(LEFT.openedByDate = 1, 'OPEN', 'NOT_YET_OPENED'));
        SELF.asOfDate := TargetDate;
        SELF := LEFT;
    )
);

OUTPUT(SORT(HistoricalEvents, accountId, eventSeq), NAMED('HistoricalEventsAsOfDate'));
//Adjust NAMED value to match the target date
OUTPUT(SORT(HistoricalWithStatus, accountId), NAMED('StateAsOfEndOfJanuary092026'));  

```

**What this shows:** the ledger preserves complete history, so you can always answer "what was the state on date X?"
Any events that arrived after the target date are excluded, giving you an auditable point-in-time snapshot.
This is how audit trails work—they're immutable time-stamped records that let you prove what happened when.

---

## How ledger-based processing can reduce cost (qualitative)

Ledger-based designs often reduce cost because they shift work from repeated full recomputation to bounded incremental updates:

- **Compute cost:** Process only new events, not the entire history every run.
- **I/O cost:** Append writes are generally efficient and avoid large rewrite operations.
- **Pipeline stability cost:** Reprocessing from a known watermark is simpler than recovering from partially overwritten snapshots.
- **Operational support cost:** Audit/debug investigations are faster when full change history is available.

## Tradeoffs and when to use this pattern

Ledger-first modeling is usually a strong fit when you need:

- High traceability and reproducibility
- Frequent incremental updates
- Clear reconciliation and audit trails

Tradeoffs to plan for:

- Event stores grow continuously, so retention/compaction strategy matters.
- Consumers need clear rules for deriving state from events.
- Duplicate or out-of-order event handling should be explicit in production pipelines.

## Practical adoption pattern

A common pattern is:

1. Keep an append-only ledger as the source of truth.
2. Build one or more materialized views for fast query/read use cases.
3. Update materialized views incrementally from new ledger events.
4. Keep and monitor a processing watermark.

This keeps audit history complete while making incremental operation the default path.
