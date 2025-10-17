# Current versions

| name     | version |
| -------- | ------- |
| current  |  9.12.x  |
| previous |  9.10.x  |
| critical |  9.8.x |
| security |  9.6.x |

## Supported versions

We release a new version of the platform every 3 months.  If there are major changes in functionality, or significant backward compatibility issues then it will be tagged as a new major version, otherwise a new minor version.  We normally maintain 4 versions of the system, which means that each new release will typically be supported for a year.  Once a new major or minor version has been tagged gold it should not have any changes that change the behavior of queries.

Which versions should changes be applied to?  The following gives some examples of the types of changes and which version they would be most relevant to target.

"master":
- New features.
- Bug fixes that will change the semantics of existing queries or processes.
- Refactoring.
- Performance improvements (unless simple and safe)

"\<current>":
- Bug fixes that only change behavior where it previously crashes or had undefined behavior (If well defined but wrong need to have very strong justification to change.)
- Fixes for race conditions (the behavior was previously indeterminate so less of an argument against it changing)
- Data corruption fixes - on a case by case basis if they change existing query results.
- Missing functionality that prevents features from working.
- Changes for tech-preview work that only effect those who are using it.
- Regressions.
- Improvements to logging and error messages (possibly in "previous" if simple and added to help diagnose problems).
- Occasional simple refactoring that makes up-merging simpler..
- Changes to improve backward compatibility of new features. (E.g. adding an ignored syntax to the compiler.)
- Performance improvements - if simple and safe

"\<previous>":
- Simple bug fixes that do not change behavior
- Simple changes for missing functionality
- Regressions with simple fixes (but care is needed if it caused a change in behavior)
- Serious regressions
- Complex security fixes

"\<critical>" fixes only:
- Simple security fixes
- Complex security fixes if sufficiently serious

"\<security>" fixes only:
- Serious security fixes

Occasionally earlier branches will be chosen, (e.g. security fixes to even older versions) but they should always be carefully discussed (and documented).

## Point Build Support Policy and Upgrade Recommendations

### Support Policy for Point Builds

**Point builds older than 2 months will not be supported.** This policy ensures efficient use of support resources by avoiding wasted effort investigating issues on outdated systems that may have already been resolved in newer releases.

All deployments should be kept reasonably up-to-date with the latest available point builds to ensure:
- Access to the most recent bug fixes and security patches
- Optimal support experience when issues arise
- Reduced risk of encountering known problems that have already been addressed

### Upgrade Recommendations

When upgrading from an older Major.Minor version to a newer Major.Minor version, **you are strongly encouraged to upgrade directly to the latest point build** of the target Major.Minor version rather than upgrading to an earlier point build first.

For example:
- ✅ **Recommended**: Upgrade from 9.8.96 directly to 9.14.26 (latest point build)
- ❌ **Not recommended**: Upgrade from 9.8.96 to 9.14.8, then later to 9.14.26

### Rationale

Point builds are designed to be **low-risk upgrades** that contain only:
- Safe, small, and self-contained fixes
- Security patches for dependencies and base images
- **Zero new features** that could introduce compatibility issues

This design philosophy means there should be a **low threshold for upgrading** to newer point builds. The benefits of staying current far outweigh the minimal risks associated with point build upgrades.

By following this policy, we can:
- Focus support efforts on current, well-maintained systems
- Avoid investigating issues that have already been resolved
- Ensure users benefit from the latest stability and security improvements
- Maintain efficient and effective support operations

## Patches and images

We aim to produce new point releases once a week.  The point releases will contain

a) Any changes to the code base for that branch.
b) Any security fixes for libraries that are project dependencies.  We will upgrade to the latest point release for the library that fixes the security issue.
c) For the cloud any security fixes in the base image or the packages installed in that image.

If there are no changes in any of those areas for a particular version then a new point release will not be created.

If you are deploying a system to the cloud you have one of two options

a) Use the images that are automatically built and published as part of the build pipeline.  This image is currently based on ubuntu 22.04 and contains the majority of packages users will require.

b) Use your own hardened base image, and install the containerized package that we publish into that image.

## Package versions.

We currently generate the following versions of the package and images:

- debug
- release with symbols
- release without symbols.

It is recommended that you deploy the "release with symbols" version to all bare-metal and non-production cloud deployments.  The extra symbols allow the system to generate stack backtraces which make it much easier to diagnose problems if they occur.
The "release without symbols" version is recommended for Kubernetes production deployments.  Deploying a system without symbols reduces the size of the images.  This reduces the time it takes Kubernetes to copy the image before provisioning a new node.
