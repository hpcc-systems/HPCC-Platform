# Roxie Package Map Format Reference

## Overview

Roxie package maps provide a mechanism to decouple data definitions from query code. They allow you to update the data files used by queries without recompiling or redeploying the queries themselves. This document provides a comprehensive reference for the package map XML format.

## Purpose

Package maps serve several key purposes:

- **Data/Code Separation**: Define which files a query uses independently from the query itself
- **Runtime Updates**: Change data files without query recompilation
- **Version Control**: Maintain multiple versions of data for different environments
- **Centralized Management**: Manage file mappings across multiple queries from a single location
- **Environment Configuration**: Define runtime parameters and settings for queries

## Basic Concepts

- **Package Map**: A complete configuration defining file mappings for one or more queries
- **Package**: A logical grouping of file definitions and environment variables
- **SuperFile**: A logical collection of multiple physical files (subfiles)
- **SubFile**: An individual physical file that is part of a superfile
- **Base**: A reference to another package, enabling inheritance and composition

## File Resolution

When a query needs to resolve a filename, Roxie follows this order:

1. **Local Package**: Search for SuperFile definitions in the query's assigned package
2. **Base Packages**: Recursively search through Base package references
3. **Dali DFU**: Query the Dali Distributed File Utility if not found in packages
4. **Local Filesystem**: Check local paths (if `resolveLocally=true`)
5. **Error Handling**: 
   - Optional files: Log warning and continue
   - Compulsory files: Throw error and fail query load

## XML Structure

### Root Elements

Package maps use one of these root elements:

```xml
<!-- Legacy/single-part format -->
<RoxiePackages>
  ...
</RoxiePackages>

<!-- Multi-part format -->
<PackageMaps multipart="1">
  ...
</PackageMaps>

<!-- Package set registry (in Dali) -->
<PackageSets>
  ...
</PackageSets>
```

### Element Hierarchy

```
RoxiePackages (or PackageMap)
├── Package
│   ├── Base (references another package)
│   ├── SuperFile
│   │   ├── SubFile
│   │   └── SubFile
│   ├── File (direct file reference)
│   └── Environment (variable definition)
└── Package
```

## Element Reference

### PackageMap Element

**Purpose**: Root container for package definitions or a named package map

**Location**: Root or child of `<PackageMaps>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Unique identifier for this package map |
| `querySet` | string | No | All | Target query set(s) - supports wildcards (e.g., `dev*`) |
| `active` | boolean | No | false | Whether this package map is currently active |
| `daliip` | string | No | - | Dali server IP address for file resolution |
| `sourceCluster` | string | No | - | Source cluster name for file resolution |
| `compulsory` | boolean | No | false | If true, all files must exist or queries fail to load |

**Example**:
```xml
<PackageMap id="production_map" querySet="roxie_prod" active="true">
  <Package id="MyQuery">
    ...
  </Package>
</PackageMap>
```

### PackageSet Element

**Purpose**: Groups multiple package maps for a process/cluster

**Location**: Child of `<PackageSets>` (in Dali registry)

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Unique identifier |
| `process` | string | No | * | Process/cluster pattern (supports wildcards) |

**Example**:
```xml
<PackageSets>
  <PackageSet id="prod_set" process="roxie*">
    <PackageMap id="pm1" querySet="qs1" active="true"/>
    <PackageMap id="pm2" querySet="dev*" active="false"/>
  </PackageSet>
</PackageSets>
```

### Part Element (Multi-part Maps)

**Purpose**: Organizes packages into manageable subsets

**Location**: Child of `<PackageMaps multipart="1">`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Part identifier (typically the filename) |

**Example**:
```xml
<PackageMaps multipart="1">
  <Part id="contacts.pkg">
    <Package id="contact_base">...</Package>
  </Part>
  <Part id="persons.pkg">
    <Package id="person_base">...</Package>
  </Part>
</PackageMaps>
```

### Package Element

**Purpose**: Groups file definitions and environment variables for a query or dataset

**Location**: Child of `<PackageMap>`, `<Part>`, or `<RoxiePackages>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Unique package identifier (often matches query name) |
| `preload` | boolean | No | false | Preload all files at Roxie startup |
| `compulsory` | boolean | No | false | All files must exist or fail to load |
| `resolveLocally` | boolean | No | false | Check local filesystem before Dali |
| `queries` | string | No | - | Wildcard pattern matching query names |
| `daliip` | string | No | - | Override Dali IP for this package |
| `sourceCluster` | string | No | - | Override source cluster for this package |

**Example**:
```xml
<Package id="PersonLookup" preload="1" compulsory="1">
  <Base id="person_data"/>
  <Environment id="timeout" value="30000"/>
</Package>
```

### Base Element

**Purpose**: References another package to inherit its definitions

**Location**: Child of `<Package>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | ID of the package to inherit from |

**Behavior**:
- Multiple `<Base>` elements are allowed and processed in order
- Inherits all SuperFile definitions from the referenced package
- Inherits environment variables (but does not override existing ones)
- Creates a package inheritance chain

**Example**:
```xml
<Package id="MyQuery">
  <Base id="thor::MyData_Key"/>
  <Base id="common_files"/>
</Package>

<Package id="thor::MyData_Key">
  <SuperFile id="~thor::MyData_Key">
    <SubFile value="~thor::Mysubfile1"/>
  </SuperFile>
</Package>
```

### SuperFile Element

**Purpose**: Defines a logical superfile and its constituent physical files

**Location**: Child of `<Package>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Logical superfile name (e.g., `~thor::MyData_Key`) |
| `daliip` | string | No | - | Override Dali IP for this superfile |
| `sourceCluster` | string | No | - | Override source cluster for this superfile |

**Notes**:
- The `~` prefix on filenames is optional
- SuperFiles can be nested (a SuperFile can contain other SuperFiles)
- Order of SubFiles matters for some operations

**Example**:
```xml
<SuperFile id="~thor::MyData_Key">
  <SubFile value="~thor::Mysubfile1"/>
  <SubFile value="~thor::Mysubfile2"/>
  <SubFile value="~thor::Mysubfile3"/>
</SuperFile>
```

### SubFile Element

**Purpose**: Specifies an individual physical file within a superfile

**Location**: Child of `<SuperFile>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `value` | string | Yes | - | Physical or logical filename |

**Notes**:
- Empty values are ignored
- Can reference logical files (which may themselves be superfiles)
- The `~` prefix is optional

**Example**:
```xml
<SubFile value="~thor::data_20240101"/>
<SubFile value="thor::data_20240102"/>
```

### File Element

**Purpose**: Direct file reference (less common, for specific use cases)

**Location**: Child of `<Package>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Logical file identifier |
| `value` | string | No | same as id | Physical filename |

**Example**:
```xml
<File id="MyIndexFile" value="physical::index::file::name"/>
```

### Environment Element

**Purpose**: Define runtime variables accessible to queries

**Location**: Child of `<Package>`

**Attributes**:

| Attribute | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Variable name |
| `value` or `val` | string | Yes | - | Variable value |

**Notes**:
- Variables are inherited from Base packages
- First defined value wins (child packages don't override parent)
- Accessible in queries via `queryEnv("varname")`
- Any attributes on the `<Package>` element become `control:*` variables

**Example**:
```xml
<Package id="myPackage" timeout="30000" maxRows="5000">
  <Environment id="dataSource" value="production"/>
  <Environment id="cacheSize" val="500MB"/>
</Package>
```

## Complete Examples

### Example 1: Simple Query Package

```xml
<?xml version="1.0" encoding="utf-8"?>
<RoxiePackages>
  <!-- Query package referencing data package -->
  <Package id="PersonLookup">
    <Base id="person_data"/>
  </Package>
  
  <!-- Data package with superfile definition -->
  <Package id="person_data">
    <SuperFile id="~thor::person_key">
      <SubFile value="~thor::person_key_20240101"/>
      <SubFile value="~thor::person_key_20240201"/>
    </SuperFile>
  </Package>
</RoxiePackages>
```

### Example 2: Multi-Query Package with Environment Variables

```xml
<?xml version="1.0" encoding="utf-8"?>
<RoxiePackages>
  <!-- Multiple queries using common data -->
  <Package id="PersonLookup" preload="1">
    <Base id="common_data"/>
    <Environment id="maxResults" value="100"/>
  </Package>
  
  <Package id="AddressLookup" preload="1">
    <Base id="common_data"/>
    <Environment id="maxResults" value="50"/>
  </Package>
  
  <!-- Shared data package -->
  <Package id="common_data" compulsory="1">
    <SuperFile id="~thor::person_master">
      <SubFile value="~thor::person_data_v1"/>
      <SubFile value="~thor::person_data_v2"/>
    </SuperFile>
    <SuperFile id="~thor::address_master">
      <SubFile value="~thor::address_data_v1"/>
    </SuperFile>
    <Environment id="timeout" value="30000"/>
  </Package>
</RoxiePackages>
```

### Example 3: Multi-Part Package Map

```xml
<?xml version="1.0" encoding="utf-8"?>
<PackageMaps multipart="1">
  <Part id="contacts.pkg">
    <Package id="contact_base" preload="1">
      <SuperFile id="~thor::contacts_key">
        <SubFile value="~thor::contacts_2024_q1"/>
      </SuperFile>
    </Package>
    <Package id="ContactQuery">
      <Base id="contact_base"/>
    </Package>
  </Part>
  
  <Part id="persons.pkg">
    <Package id="person_base" preload="1">
      <SuperFile id="~thor::person_key">
        <SubFile value="~thor::person_2024_q1"/>
      </SuperFile>
    </Package>
    <Package id="PersonQuery">
      <Base id="person_base"/>
    </Package>
  </Part>
</PackageMaps>
```

### Example 4: Dali IP Override

```xml
<?xml version="1.0" encoding="utf-8"?>
<PackageMap id="cross_cluster" daliip="10.239.8.10">
  <Package id="MyQuery" daliip="10.239.7.1" sourceCluster="thor1">
    <SuperFile id="~thor::data1">
      <SubFile value="~thor::file1"/>
    </SuperFile>
    
    <!-- This superfile uses different Dali -->
    <SuperFile id="~thor::data2" daliip="10.239.8.1" sourceCluster="thor2">
      <SubFile value="~thor::file2"/>
    </SuperFile>
  </Package>
</PackageMap>
```

## Package Inheritance

Packages can inherit from other packages using `<Base>` elements, creating a flexible composition system.

### Simple Inheritance

```xml
<Package id="MyQuery">
  <Base id="data_package"/>
</Package>

<Package id="data_package">
  <SuperFile id="~data::files">
    <SubFile value="~file1"/>
  </SuperFile>
</Package>
```

Result: `MyQuery` has access to `~data::files` through inheritance.

### Multi-Level Inheritance

```xml
<Package id="Query1">
  <Base id="Level1"/>
</Package>

<Package id="Level1">
  <Base id="Level2"/>
  <Environment id="var1" value="level1"/>
</Package>

<Package id="Level2">
  <SuperFile id="~data"/>
  <Environment id="var2" value="level2"/>
</Package>
```

Result: `Query1` inherits both `~data` and both environment variables through the chain.

### Environment Variable Inheritance

Environment variables follow a "first defined wins" strategy:

```xml
<Package id="Query">
  <Base id="BasePackage"/>
  <Environment id="timeout" value="5000"/>  <!-- This value used -->
</Package>

<Package id="BasePackage">
  <Environment id="timeout" value="10000"/> <!-- Ignored -->
  <Environment id="maxRows" value="100"/>   <!-- This value used -->
</Package>
```

## Managing Package Maps

### Adding a Package Map

```bash
# Single-part package map
ecl packagemap add <target> mypackage.xml --activate

# Multi-part package map
ecl packagemap add-part <target> <mapid> part1.xml
ecl packagemap add-part <target> <mapid> part2.xml
ecl packagemap activate <target> <mapid>
```

### Updating a Package Map

```bash
# Replace entire package map
ecl packagemap add <target> mypackage.xml --overwrite --activate

# Update a single part
ecl packagemap add-part <target> <mapid> part1.xml --delete-previous
```

### Viewing Package Maps

```bash
# List all package maps
ecl packagemap list <target>

# Get specific package map
ecl packagemap get <target> <mapid>

# Get specific part
ecl packagemap get-part <target> <mapid> <partid>
```

### Deleting Package Maps

```bash
# Delete entire package map
ecl packagemap delete <target> <mapid>

# Delete a specific part
ecl packagemap delete-part <target> <mapid> <partid>
```

## Best Practices

1. **Version Your Data**: Use dated or versioned subfile names (e.g., `~data_20240101`, `~data_v2`)
2. **Avoid File Reuse**: Don't modify files in-place; create new files instead
3. **Use Preload Wisely**: Set `preload="1"` only for frequently accessed, critical files
4. **Organize with Parts**: For large deployments, use multi-part maps to organize by function or team
5. **Document Changes**: Include comments in your XML explaining major changes
6. **Test Before Production**: Validate package maps with inactive deployments before activating
7. **Use Base Packages**: Share common file definitions across queries using Base references
8. **Set Compulsory Appropriately**: Use `compulsory="1"` for critical files that must exist
9. **Environment Variables**: Use environment variables for configuration that may differ between environments

## Validation

When Roxie loads a package map, it performs these validations:

1. **XML Well-formedness**: The XML must be valid and well-formed
2. **Required Attributes**: All required attributes must be present (`id` attributes)
3. **Package References**: Base package references must resolve to existing packages
4. **Compulsory Files**: If `compulsory="1"`, all referenced files must exist
5. **Circular References**: Base package chains cannot be circular

## Error Handling

Common errors and their causes:

| Error | Cause | Solution |
|-------|-------|----------|
| "Package not found" | Base reference to non-existent package | Check package `id` attributes |
| "File not found" | Referenced file doesn't exist with `compulsory="1"` | Verify file exists in Dali or create it |
| "Circular package reference" | Package inheritance loop | Review Base references |
| "Invalid XML" | Malformed XML structure | Validate XML syntax |
| "Query not found in package" | Query name doesn't match any package `id` | Ensure package `id` matches query name |

## Technical Details

### Package Resolution Algorithm

When Roxie needs to resolve a file for a query:

1. Determine the active PackageMap for the query's QuerySet
2. Find the Package with `id` matching the query name (or `queries` pattern)
3. Search Package.SuperFile elements for matching `id`
4. If not found, recursively search Base packages
5. If still not found and Dali is available, query Dali DFU
6. If not found and file is compulsory, fail; otherwise return null

### Hash Calculation

Package maps are hashed for change detection:
- XML is serialized with sorted tags
- Hash is calculated using `rtlHash64Data`
- Changes to any element content or attributes result in a different hash

### Caching

Roxie caches resolved file information:
- Cache is per-query-factory instance
- Cache invalidates when package map changes
- Preloaded files are kept in memory

## Related Documentation

- [ECL Language Reference - DATASET](../../docs/EN_US/ECLLanguageReference/ECLR_mods/BltInFunc-DATASET.xml)
- [Roxie Reference Guide](../../docs/EN_US/RoxieReference/)
- [ECL Command Line Reference](../../docs/EN_US/HPCCClientTools/CT_Mods/CT_ECL_CLI.xml)

## Source Code References

Key implementation files:

- `roxie/ccd/ccdstate.cpp` - Package map loading and management
- `common/pkgfiles/package.cpp` - Package file processing
- `common/pkgfiles/pkgimpl.hpp` - Package interface definitions
- `common/pkgfiles/package.h` - Public package API

## Glossary

- **DFU**: Distributed File Utility - HPCC's file management system
- **Dali**: HPCC's distributed metadata server
- **QuerySet**: A logical grouping of queries on a Roxie cluster
- **Roxie**: HPCC's rapid data delivery engine
- **Superfile**: A logical file composed of multiple physical files
- **Subfile**: An individual physical file within a superfile
