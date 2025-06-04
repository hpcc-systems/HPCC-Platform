---
applyTo: "esp/services/**"
---
# ESP Services Instructions

These instructions apply specifically to the ESP services folder and related components.

## ESP Service Development Guidelines

### ECM File Definitions
- ECM files are Interface Definition Language (IDL) files that generate header files during the build process
- Generated headers are placed in the build/generated directory
- Always update the corresponding ECM file when modifying service interfaces
- ECM files define the contract between ESP services and clients

### File Structure
- Keep service implementation files organized within the ESP services folder
- Maintain separation between interface definitions (ECM) and implementation
- Follow existing naming conventions for consistency

### Build Process
- ECM files are automatically processed during build to generate C++ headers
- Generated files should not be manually edited
- Ensure ECM syntax is valid before committing changes

### Best Practices
- Document all service methods and parameters in ECM files
- Use appropriate data types for cross-platform compatibility
- Test service interfaces after ECM modifications
- Coordinate interface changes with dependent services

### Code Reviews
- Include both ECM files and generated headers in review scope
- Verify backward compatibility for interface changes
- Ensure proper error handling in service implementations