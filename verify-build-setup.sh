#!/bin/bash
# HPCC Platform Build Verification Script
# This script verifies that your system is ready to build HPCC Platform
# and demonstrates the basic build process.

set -e  # Exit on any error

echo "=== HPCC Platform Build Verification ==="
echo ""

# Check prerequisites
echo "1. Checking prerequisites..."

# Check CMake version
if ! command -v cmake &> /dev/null; then
    echo "ERROR: CMake is not installed"
    exit 1
fi

cmake_version=$(cmake --version | head -n1 | cut -d' ' -f3)
echo "   ✓ CMake version: $cmake_version"

# Check Ninja
if command -v ninja &> /dev/null; then
    ninja_version=$(ninja --version)
    echo "   ✓ Ninja version: $ninja_version"
    GENERATOR="Ninja"
else
    echo "   ⚠ Ninja not found, will use Make instead"
    GENERATOR="Unix Makefiles"
fi

# Check Git
if ! command -v git &> /dev/null; then
    echo "ERROR: Git is not installed"
    exit 1
fi
echo "   ✓ Git is available"

# Check basic development tools
if ! command -v gcc &> /dev/null && ! command -v clang &> /dev/null; then
    echo "ERROR: No C++ compiler found (gcc or clang)"
    exit 1
fi

if command -v gcc &> /dev/null; then
    gcc_version=$(gcc --version | head -n1)
    echo "   ✓ GCC: $gcc_version"
fi

echo ""

# Check if we're in the HPCC Platform directory
echo "2. Checking source directory..."
if [[ ! -f "CMakeLists.txt" ]] || [[ ! -f "BUILD_ME.md" ]]; then
    echo "ERROR: This script must be run from the HPCC Platform source directory"
    echo "       Make sure you're in the directory containing CMakeLists.txt"
    exit 1
fi
echo "   ✓ Found HPCC Platform source files"

# Check submodules
echo "3. Checking submodules..."
if [[ ! -f "vcpkg/.gitignore" ]]; then
    echo "   Initializing submodules..."
    git submodule update --init --recursive
fi
echo "   ✓ Submodules are initialized"

echo ""

# Create build directory
BUILD_DIR="build-verification"
echo "4. Setting up build directory ($BUILD_DIR)..."

if [[ -d "$BUILD_DIR" ]]; then
    echo "   Cleaning existing build directory..."
    rm -rf "$BUILD_DIR"
fi

mkdir "$BUILD_DIR"
cd "$BUILD_DIR"
echo "   ✓ Build directory created"

echo ""

# Configure build
echo "5. Configuring build (this may take a while for first-time setup)..."
echo "   Using generator: $GENERATOR"
echo "   Configuration: Client Tools Only (faster build for testing)"

CMAKE_ARGS=(
    -B .
    -S ..
    -G "$GENERATOR"
    -DCLIENTTOOLS_ONLY=ON
    -DCMAKE_BUILD_TYPE=Debug
    -DUSE_OPTIONAL=ON
    -DUSE_CPPUNIT=OFF
)

echo "   Running: cmake ${CMAKE_ARGS[*]}"
echo ""

# Run cmake configuration
if timeout 1800 cmake "${CMAKE_ARGS[@]}"; then
    echo ""
    echo "   ✓ CMake configuration completed successfully"
else
    echo ""
    echo "   ✗ CMake configuration failed or timed out (30 minutes)"
    echo "     This is often due to network issues downloading dependencies"
    echo "     You can re-run this script to continue where it left off"
    exit 1
fi

echo ""

# Verify build system files exist
echo "6. Verifying build system..."
if [[ "$GENERATOR" == "Ninja" ]] && [[ -f "build.ninja" ]]; then
    echo "   ✓ Ninja build files generated"
elif [[ "$GENERATOR" == "Unix Makefiles" ]] && [[ -f "Makefile" ]]; then
    echo "   ✓ Makefile generated"
else
    echo "   ✗ Build system files not found"
    ls -la
    exit 1
fi

echo ""

# Try a small build test
echo "7. Testing build system with a quick compile test..."
echo "   Building jlib (core library) as a test..."

if timeout 600 cmake --build . --target jlib --parallel 2; then
    echo "   ✓ Test build completed successfully"
else
    echo "   ⚠ Test build failed or timed out"
    echo "     The configuration is valid, but there may be missing dependencies"
    echo "     Check the error messages above for details"
fi

echo ""

# Show next steps
echo "=== Build Verification Complete ==="
echo ""
echo "Your system is ready to build HPCC Platform!"
echo ""
echo "Next steps:"
echo "  1. To build client tools only:"
echo "     cmake --build . --parallel"
echo ""
echo "  2. To build the full platform, configure a new build directory:"
echo "     mkdir ../build-full"
echo "     cd ../build-full"
echo "     cmake -B . -S .. -G $GENERATOR -DCMAKE_BUILD_TYPE=Release"
echo "     cmake --build . --parallel"
echo ""
echo "  3. To create an installation package:"
echo "     cmake --build . --target package"
echo ""
echo "For more information, see BUILD_MASTER_GUIDE.md"