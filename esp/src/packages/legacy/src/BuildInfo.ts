const _cmake_build_type = "@CMAKE_BUILD_TYPE@";
const _containerized = "@CONTAINERIZED@";

const cmake_true = ["1", "ON", "YES", "TRUE", "Y"];

type BUILD_TYPE = "Debug" | "Release" | "RelWithDebInfo" | "MinSizeRel";
export const cmake_build_type: BUILD_TYPE = _cmake_build_type.indexOf("CMAKE_BUILD_TYPE") > 0 ? "Debug" : _cmake_build_type as BUILD_TYPE;
export const containerized: boolean = cmake_true.indexOf(_containerized.toUpperCase()) >= 0 || _containerized.indexOf("CONTAINERIZED") > 0;
export const bare_metal: boolean = !containerized || _containerized.indexOf("CONTAINERIZED") > 0;

export const ModernMode = "ModernMode-9.0";
