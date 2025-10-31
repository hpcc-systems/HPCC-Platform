---
applyTo: '**/*.ecm'
---

# Instructions for updating ESP service interface definitions (ESDL)

## Coding Standards
- Add a version attribute to any new or deprecated element.
- Use the next highest version number in the service's `*.ecm` file for versioning changes.
- Increment the `version` and `default_client_version` attributes of the `ESPservice` element.
- Include the `exceptions_inline` attribute in any new `ESPresponse` and `ESPmethod` element.

## Naming Conventions
- Use `PascalCase` for element names.
- Use lower case for native types such as `string`, `integer`, `boolean`, etc.

## Best Practices
- Do not modify the c++ code (in the `generated` directory) generated from the `*.ecm` files.

## Build Instructions
- Build the `espscm` target to generate the c++ code from the `*.ecm` files.

## Reference Documentation

- [ESDL Reference Overview](docs/EN_US/DynamicESDL/ESDL_LangRef_Includer.xml)
- [ESDL Reference Sections](docs/EN_US/DynamicESDL/DESDL-Mods)

