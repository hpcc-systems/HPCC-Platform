# Why You Should Avoid Using the -legacy Compiler Option

## Overview of the -legacy Flag

The **-legacy** flag was provided as a short-term solution to give you a chance to modify your code for the new semantics. It was always intended to be a temporary solution -- in fact, it was marked as deprecated when it was introduced.

The updated **IMPORT** rules are more robust and predictable. They prevent namespace collisions and align with current best practices. The updated **WHEN** behavior provides deterministic scoping, order independence, and improved optimization.

Aligning with current standards helps teams collaborate more effectively and reduces onboarding friction for new developers.

## Reasoning for Avoiding the -legacy Flag

The legacy flag creates several issues:

1. **Symbol Conflicts**: Can create symbol conflicts in your code when you upgrade the platform.
2. **Syntax Check Failures**: Can cause valid syntax to fail syntax checks.
3. **Deprecated Behaviors**: Enables deprecated behaviors that will be removed, risking future code issues.
4. **Masked Warnings**: Masks some warnings that would help you improve your code.
5. **Performance Impact**: Can disable newer optimizer paths and reduce performance.

## Understanding the Compiler Options

The compiler options were added to allow you to keep two legacy behaviors that changed: **IMPORT** and **WHEN**.

The eclcc options work as follows:

- **-legacyimport**: Forces the old IMPORT behavior
- **-legacywhen**: Forces the old WHEN behavior  
- **-legacy**: Allows both legacy behaviors

We suspect that **-legacy** is the most commonly used option.

## Recommendations

We recommend migrating away from the **-legacy** flag and updating your code to use the new semantics. This approach will:

- Ensure compatibility with future HPCC Systems platform releases
- Improve code maintainability and readability
- Take advantage of performance optimizations
- Align with current ECL best practices
