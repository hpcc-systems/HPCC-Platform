# Package Map Generation Utility

This directory contains tools and documentation for working with Roxie package maps.

## Files

- **PACKAGEMAP_FORMAT.md** - Comprehensive reference documentation for the Roxie package map XML format
- **generate_packagemap.py** - Python utility to generate package maps from deployed queries

## Package Map Generator

### Overview

The `generate_packagemap.py` utility queries an HPCC ESP server to retrieve information about a deployed Roxie query and generates a package map XML file. This is useful for:

- Creating package maps for existing queries
- Documenting current file mappings
- Migrating queries between environments
- Testing production configurations in development

### Requirements

- Python 3.6 or later
- `requests` library (install with: `pip install requests`)

### Installation

```bash
# Install required Python packages
pip install requests
```

### Usage

#### Basic Usage

```bash
./generate_packagemap.py --server http://localhost:8010 --target roxie --query MyQuery
```

This will output the generated package map XML to stdout.

#### Save to File

```bash
./generate_packagemap.py --server http://localhost:8010 --target roxie --query MyQuery --output myquery.xml
```

#### With Authentication

If your ESP server requires authentication:

```bash
./generate_packagemap.py --server https://example.com:8010 \
                         --target roxie \
                         --query MyQuery \
                         --username admin \
                         --password secret \
                         --output myquery.xml
```

#### Verbose Output

For debugging and to see what the utility is doing:

```bash
./generate_packagemap.py --server http://localhost:8010 \
                         --target roxie \
                         --query MyQuery \
                         --verbose
```

#### Skip SSL Verification

For development environments with self-signed certificates (not recommended for production):

```bash
./generate_packagemap.py --server https://localhost:8010 \
                         --target roxie \
                         --query MyQuery \
                         --no-verify-ssl
```

### Command Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--server` | Yes | ESP server URL (e.g., http://localhost:8010) |
| `--target` | Yes | Target cluster/queryset name (e.g., roxie) |
| `--query` | Yes | Query ID or alias |
| `--username` | No | Username for authentication |
| `--password` | No | Password for authentication |
| `--output`, `-o` | No | Output file path (default: stdout) |
| `--no-verify-ssl` | No | Disable SSL certificate verification |
| `--no-file-comment` | No | Don't include comment about individual files |
| `--verbose`, `-v` | No | Enable verbose output |
| `--help`, `-h` | No | Show help message |

### Examples

#### Example 1: Generate Package Map for Testing

```bash
# Query production
./generate_packagemap.py --server http://prod-esp:8010 \
                         --target roxie \
                         --query CustomerLookup \
                         --output customer_lookup.xml

# Use the generated package map in development
ecl packagemap add dev-roxie customer_lookup.xml --activate
```

#### Example 2: Document Multiple Queries

```bash
#!/bin/bash
# Generate package maps for all queries

queries=("CustomerLookup" "ProductSearch" "OrderHistory")

for query in "${queries[@]}"; do
  ./generate_packagemap.py --server http://localhost:8010 \
                           --target roxie \
                           --query "$query" \
                           --output "${query}_package.xml"
done
```

#### Example 3: Compare Environments

```bash
# Generate from production
./generate_packagemap.py --server http://prod:8010 \
                         --target roxie \
                         --query MyQuery \
                         --output prod_myquery.xml

# Generate from staging
./generate_packagemap.py --server http://staging:8010 \
                         --target roxie \
                         --query MyQuery \
                         --output staging_myquery.xml

# Compare
diff prod_myquery.xml staging_myquery.xml
```

### How It Works

1. **Query ESP Service**: The utility uses the `WUQueryFiles` ESP service method to retrieve:
   - List of files used by the query
   - List of superfiles and their subfiles

2. **Process File Information**: 
   - Organizes files into superfile structures
   - Creates package references for the query
   - Assumes filename aliases resolve to files with the same name

3. **Generate XML**:
   - Creates a `<RoxiePackages>` root element
   - Defines a package for the query with Base references
   - Defines data packages containing SuperFile definitions
   - Formats the XML for readability

### Output Format

The generated package map follows this structure:

```xml
<?xml version="1.0" encoding="utf-8"?>
<RoxiePackages>
  <!-- Package map generated for query: MyQuery -->
  
  <!-- Query package -->
  <Package id="MyQuery">
    <Base id="data_package_1"/>
    <Base id="data_package_2"/>
  </Package>
  
  <!-- Data packages -->
  <Package id="data_package_1">
    <SuperFile id="~thor::my_superfile">
      <SubFile value="~thor::subfile1"/>
      <SubFile value="~thor::subfile2"/>
    </SuperFile>
  </Package>
  
  <Package id="data_package_2">
    <SuperFile id="~thor::another_superfile">
      <SubFile value="~thor::subfile3"/>
    </SuperFile>
  </Package>
</RoxiePackages>
```

### Assumptions

The utility makes the following assumptions:

1. **Alias Resolution**: Filename aliases used in the query resolve to files with the same name
2. **No Complex Inheritance**: Generated package maps use simple one-level inheritance
3. **Standard Format**: Output uses the legacy `<RoxiePackages>` format (not multi-part)
4. **No Environment Variables**: Generated maps don't include environment variable definitions

### Customization

After generation, you may want to customize the package map:

1. **Add Environment Variables**: Add `<Environment>` elements for configuration
2. **Merge Packages**: Combine multiple data packages if they share common files
3. **Add Attributes**: Set `preload`, `compulsory`, or other attributes as needed
4. **Organize into Parts**: Convert to multi-part format for better organization

### Troubleshooting

#### Connection Refused

```
Error: Connection refused
```

**Solution**: Verify the ESP server URL and ensure the server is running.

#### Authentication Failed

```
Error: 401 Unauthorized
```

**Solution**: Provide correct username and password with `--username` and `--password`.

#### Query Not Found

```
Error: Query not found
```

**Solution**: Verify the query ID/alias and target are correct. Check that the query is deployed.

#### SSL Certificate Error

```
Error: SSL certificate verification failed
```

**Solution**: Either fix the certificate issue or use `--no-verify-ssl` (not recommended for production).

#### No Files Found

If the generated package map is empty or missing files:

1. Verify the query is properly deployed and active
2. Check that files are registered in Dali
3. Ensure the query has been executed at least once

### API Usage

You can also use the tool as a Python module:

```python
from generate_packagemap import ESPClient, PackageMapGenerator

# Create client
client = ESPClient('http://localhost:8010')

# Get query files
query_files = client.get_query_files('roxie', 'MyQuery')

# Generate package map
generator = PackageMapGenerator(
    'MyQuery',
    query_files['Files'],
    query_files['SuperFiles']
)
xml = generator.generate()

print(xml)
```

## See Also

- [PACKAGEMAP_FORMAT.md](./PACKAGEMAP_FORMAT.md) - Complete format reference
- [Roxie Reference Guide](../../docs/EN_US/RoxieReference/) - Roxie documentation
- [ECL CLI Reference](../../docs/EN_US/HPCCClientTools/CT_Mods/CT_ECL_CLI.xml) - ecl packagemap commands

## Contributing

When modifying the generator utility:

1. Maintain backwards compatibility with existing ESP services
2. Follow the package map format specification in PACKAGEMAP_FORMAT.md
3. Update this README with any new features or options
4. Add error handling for edge cases

## License

Copyright (C) 2024 HPCC Systems®.

Licensed under the Apache License, Version 2.0.
