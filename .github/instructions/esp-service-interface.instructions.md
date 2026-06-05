---
applyTo: '**/*.ecm'
---

# Instructions for updating ESP service interface definitions (ESDL)

## Coding Standards
- Add a version attribute to any new or deprecated element.
- Versions are treated like floating point numbers with two digits of precision.
- Use the next highest version number (incremented by 0.01) in the service's `*.ecm` file for versioning changes. For example, if the current version is 1.99 the next version would be 2.00, or if the current version is 1.01 the next version would be 1.02.
- If the change is in a structure that is shared by multiple services, increment to the next highest version across all services sharing that structure, then update the version in all services sharing that structure.
- Increment the `version` and `default_client_version` attributes of the `ESPservice` element.
- Include the `exceptions_inline` attribute in any new `ESPresponse` element.
- Include the `exceptions_inline` attribute in any new `ESPmethod` element only if its parent `ESPservice` element doesn't have an `exceptions_inline` attribute.

## Naming Conventions
- Use `PascalCase` for element names.
- Use lower case for native types such as `string`, `integer`, `boolean`, etc.

## Best Practices
- Do not modify the c++ code (in the `generated` directory) generated from the `*.ecm` files.

## Build Instructions
- Build the `espscm` target to generate the c++ code from the `*.ecm` files.

## Reference Documentation

- [ESDL Reference Overview](../../docs/EN_US/DynamicESDL/ESDL_LangRef_Includer.xml)
- [ESDL Reference Sections](../../docs/EN_US/DynamicESDL/DESDL-Mods)

