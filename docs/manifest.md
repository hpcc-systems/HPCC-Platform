# ESDL Manifest Command

## Overview

The `esdl manifest` command is a powerful tool for building ESDL (Enterprise Service Description Language) service configurations from template-based manifest files. It simplifies the deployment and configuration of ESDL-based dynamic services on HPCC clusters by allowing you to:

- Abstract complexity from actual configuration files
- Maintain ESDL Scripts and XSLTs separately in your repository
- Generate either binding configurations (for dali-based publishing) or bundle files (for ESDL application mode)

## Purpose

The manifest command addresses the challenge of managing complex ESDL service configurations. Instead of manually editing large, monolithic XML configuration files, developers can:

1. **Use Templates**: Create manifest files that are easier to read and maintain
2. **Modularize Configuration**: Store scripts, transforms, and definitions in separate files
3. **Automate Processing**: Let the tool handle XML escaping, CDATA wrapping, and element ordering
4. **Support Multiple Deployment Modes**: Generate configurations for both dali-based and application mode deployments

## Key Features

### Output Types

The manifest command supports two output types:

- **`bundle`**: Creates an ESDL bundle file for launching an ESP in application mode (default)
- **`binding`**: Creates an ESDL binding configuration that can be published to dali

### Manifest Namespace

Manifest files use the `urn:hpcc:esdl:manifest` namespace (typically with prefix `em:`). Elements in this namespace control the tool's behavior and are replaced in the output with appropriate ESDL configuration elements.

### File Inclusion

The `<em:Include>` element imports external files into the output, enabling:
- Reusable ESDL script components
- Separate XSLT transformation files
- Modular ESDL definitions

## Key Parameters

### Command Line Options

```
esdl manifest <manifest-file> [options]
```

| Option | Description |
|--------|-------------|
| `-I, --include-path <path>` | Search path for external files included in the manifest. Use once for each path. |
| `--outfile <filename>` | Path and name of the output file |
| `--output-type <type>` | Override the manifest's output type. Allowed values: `binding` or `bundle` |
| `--help` | Display usage information |
| `-v, --verbose` | Output additional tracing information |
| `-tcat, --trace-category <flags>` | Control debug message categories (dev, admin, user, err, warn, prog, info) |

### Manifest File Elements

| Element | Purpose |
|---------|---------|
| `<em:Manifest>` | Required root element. Controls output type via `@outputType` attribute |
| `<em:ServiceBinding>` | Creates ESDL binding content with service-specific configuration |
| `<em:EsdlDefinition>` | Enables ESDL definition processing (bundle output only) |
| `<em:Include>` | Imports external file contents in place of the element |
| `<em:Scripts>` | Processes and wraps ESDL scripts with proper CDATA sections |
| `<em:Transform>` | Processes and wraps XSLT transforms with proper CDATA sections |

## Usage Examples

### Basic Bundle Creation

Create a bundle configuration from a manifest file:

```bash
esdl manifest my_service_manifest.xml --outfile my_service_bundle.xml
```

### Binding with Include Paths

Create a binding configuration with external files from a custom directory:

```bash
esdl manifest my_service_manifest.xml \
  -I ./scripts \
  -I ./transforms \
  --output-type binding \
  --outfile my_service_binding.xml
```

### Simple Manifest File Example

```xml
<em:Manifest xmlns:em="urn:hpcc:esdl:manifest">
    <em:ServiceBinding esdlservice="WsMyService" id="WsMyService_desdl_binding">
        <Methods>
            <em:Scripts>
                <em:Include file="common-scripts.xml"/>
            </em:Scripts>
            <Method name="MyMethod" url="127.0.0.1:8888">
                <em:Scripts>
                    <em:Include file="method-scripts.xml"/>
                </em:Scripts>
            </Method>
        </Methods>
    </em:ServiceBinding>
    <em:EsdlDefinition>
        <em:Include file="WsMyService.ecm"/>
    </em:EsdlDefinition>
</em:Manifest>
```

### Complete Workflow Example

A typical workflow for deploying a service using a manifest:

```bash
# 1. Generate ECL layouts from ESDL definition
esdl ecl my_service.esdl .

# 2. Publish the ESDL definition
esdl publish my_service.esdl MyService --overwrite

# 3. Create the binding configuration from manifest
esdl manifest my_service_manifest.xml -I scripts --outfile binding.xml

# 4. Bind the service to an ESP
esdl bind-service myesp 8088 myservice.1 MyService --config binding.xml --overwrite
```

## Additional Resources

For detailed documentation, including:
- Complete manifest namespace reference
- Attribute descriptions and usage
- Advanced examples with logging and transforms
- Output format specifications

See the comprehensive [ESDL Command Reference](../tools/esdlcmd/README.md#manifest).

For a working example with step-by-step instructions, see the [Manifest Example](../initfiles/examples/EsdlExample/Manifest/README.md).

## Related Commands

The manifest command is part of the `esdl` utility suite. Other related commands include:

- `esdl publish` - Publish ESDL definitions
- `esdl bind-service` - Configure ESDL-based service on ESP
- `esdl get-binding` - Retrieve existing binding configurations
- `esdl xml` - Generate XML from ESDL definition

For a complete list of ESDL commands, see the [ESDL Utility Documentation](../tools/esdlcmd/README.md).
