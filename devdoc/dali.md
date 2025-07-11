# Dali Physical File Storage

## Overview

Dali maintains its metadata store through a versioned file system that uses multiple file types to ensure data integrity and provide crash recovery capabilities. The storage system is designed to handle both normal shutdown/startup cycles and unexpected failures (power loss, crashes).

## File Types and Structure

Dali uses six types of physical files to manage its metadata store:

### 1. Store Pointer File - `store.<edition>`

**Purpose**: Indicates the current active version of the metadata store and provides integrity validation.

**Format**: Simple binary file containing a 4-byte CRC value that corresponds to the main store file.

**Example**: `store.1001` indicates edition 1001 is the current active store, and contains the CRC of `dalisds1001.xml`.

### 2. Main Store File - `dalisds<edition>.xml`

**Purpose**: Contains the complete Dali metadata store in XML format.

**Format**: Standard XML file containing the entire property tree structure representing all Dali metadata.

**Example**: `dalisds1001.xml` contains the complete metadata store for edition 1001.

### 3. Transaction Delta File - `daliinc<edition>.xml`

**Purpose**: Records incremental write transactions as deltas to the main store.

**Format**: XML file with special header containing:
- CRC validation header: `<CRC>0000000000</CRC>`
- Size tracking header: `<SIZE>0000000000000000</SIZE>`
- Followed by sequential transaction deltas

**Usage**:
- Every write transaction is appended to this file
- Used by Sasha during routine coalescing to create new consolidated store editions
- Used during crash recovery when in-memory state is lost
- Normally not read by Dali during regular operation (only written to)

**Example**: `daliinc1001.xml` contains all transactions since the `dalisds1001.xml` store was created.

### 4. Detached Transaction File - `dalidet<edition>.xml`

**Purpose**: Temporary file created during Sasha coalescing operations.

**Format**: Identical to `daliinc<edition>.xml` format.

**Usage**:
- Created when Sasha begins coalescing a new store revision
- Represents the "detached" transaction file that was atomically renamed from `daliinc`
- Allows Dali to continue writing to a fresh `daliinc` file during coalescing
- Read by Sasha during normal coalescing process
- Only used by Dali for crash recovery if both Dali and Sasha failed before completing the coalescing

**Example**: `dalidet1001.xml` contains transactions that were being processed when Sasha started coalescing.

### 5. External Binary Files - `dalisds_<id>.bv2`

**Purpose**: Stores large property values externally to keep the main store files manageable in size.

**Format**: Binary files containing compressed property values that exceed the size threshold.

**Size Thresholds**:
- Property values are compressed if they exceed 4KB (jptree compression threshold)
- Values exceeding 10KB (after compression, configurable) are externalized to these files

**Usage**:
- Created when write transactions contain property values exceeding the size threshold
- Each file is assigned a sequential integer identifier `<id>`
- Property tree nodes reference external files via `@sds:ext` attribute (e.g., `@sds:ext="34"`)
- Values are loaded lazily on-demand when clients access the property
- External values are not serialized until specifically probed by client code

**Example**: `dalisds_34.bv2` contains an externalized property value, referenced by `@sds:ext="34"` in the property tree.

### 6. Coven Configuration File - `dalicoven.xml`

**Purpose**: Stores global Dali state meta data

**Format**: XML file containing a limited flat set of values
- UIDBase - used by getUniqueID to fetch ranged of unique identifiers
- SDSNodes - stores an estimate of # of nodes in SDS, used to pre-size the table on startup
- ServerID - legacy, not actively used
- CovenID - legacy, not actively used
- SDSEdition - legacy, not actively used

## Normal Operation

During normal operation:

1. **Startup**: Dali loads the main store file (`dalisds<edition>.xml`) indicated by `store.<edition>`
   - On first startup in a clean environment, no store files exist and Dali creates `daliinc.xml` (without edition number)
2. **Write Operations**: All write transactions are appended to `daliinc<edition>.xml` (or `daliinc.xml` for initial startup)
3. **Shutdown**: Dali saves its complete in-memory state to a new store file and updates the store pointer
4. **Transaction files do not exist** after normal shutdown since the complete state is saved to a new store edition

## Sasha Coalescing Process

Sasha (the store coalescing service) periodically consolidates the store:

1. **Detach**: Atomically renames `daliinc<edition>.xml` to `dalidet<edition>.xml`
2. **Fresh Start**: Dali begins writing new transactions to a fresh `daliinc<edition+1>.xml`
3. **Coalesce**: Sasha loads the base store + detached transactions and saves a new consolidated store
4. **Atomic Update**: Updates `store.<edition+1>` to point to the new consolidated store
5. **Cleanup**: Removes old files after successful coalescing

## Crash Recovery Scenarios

### Single File Recovery

**Scenario**: Dali crashes but Sasha was not coalescing.

**Recovery**:
1. Load base store from `dalisds<edition>.xml`
2. Apply transactions from `daliinc<edition>.xml` (if present)

### Dual File Recovery

**Scenario**: Dali crashes while Sasha was coalescing (both `dalidet` and `daliinc` exist).

**Recovery**:
1. Load base store from `dalisds<edition>.xml`
2. Apply transactions from `dalidet<edition>.xml` first (older transactions)
3. Apply transactions from `daliinc<edition>.xml` second (newer transactions)

## File Naming Convention

Store and transaction files use the pattern `<basename><edition>.xml` where:
- `<basename>` identifies the file type (`dalisds`, `daliinc`, `dalidet`)
- `<edition>` is a numeric version identifier that increments with each new store version
- The `store.<edition>` file uses only the edition number without `.xml` extension

External binary files use the pattern `dalisds_<id>.bv2` where:
- `<id>` is a sequential integer identifier for each externalized property value

## Implementation Notes

- CRC validation ensures store integrity during loading
- Multiple store versions are retained for backup purposes (configurable retention count)
