# Selenium to Playwright Test Conversion

This document describes the conversion of Java Selenium tests from `esp/src/test-ui` to TypeScript Playwright tests in `esp/src/tests`.

## Original Selenium Test Structure

The original selenium tests were located in `esp/src/test-ui/tests/` and included:

1. **Activities.java** - Basic test for Activities page elements
2. **Framework-based tests**:
   - `ActivitiesTest.java` - Comprehensive Activities page and navigation testing
   - `ECLWorkUnitsTest.java` - ECL Workunits page testing with detail page validation
   - `FilesLogicalFilesTest.java` - Files/Logical Files page testing (disabled due to HPCC-32297)
   - `BaseTableTest.java` - Abstract base class for table functionality testing

## Converted Playwright Tests

The following new Playwright test files were created to replace the selenium functionality:

### Direct Conversions

1. **v9-activities-basic-converted.spec.ts**
   - Direct conversion of `Activities.java`
   - Tests for "Job Name", "Owner", "Target/Wuid" elements
   - Minimal 1:1 mapping of original selenium test logic

### Enhanced Conversions

2. **v9-activity-enhanced.spec.ts**
   - Converts functionality from `ActivitiesTest.java`
   - Navigation structure validation
   - Cross-page navigation testing
   - Enhanced Activities page functionality

3. **v9-workunits-enhanced.spec.ts**
   - Converts functionality from `ECLWorkUnitsTest.java`
   - Extended column verification beyond existing v9-workunits.spec.ts
   - Detail page navigation testing
   - Tab functionality validation
   - Cost information handling

4. **v9-files-logical.spec.ts**
   - Converts functionality from `FilesLogicalFilesTest.java`
   - Logical files page testing
   - Column verification and filtering
   - Note: Original was disabled due to HPCC-32297 bug

5. **v9-base-table.spec.ts**
   - Converts functionality from `BaseTableTest.java`
   - Generic table functionality testing
   - Sorting, pagination, selection
   - Empty state handling

### Summary and Validation

6. **conversion-summary.spec.ts**
   - Comprehensive validation of all conversions
   - Documents what was converted from each original test
   - Serves as a reference for the conversion scope

## Conversion Approach

### Minimal Changes Strategy
- Existing Playwright tests were kept intact
- New tests were added to cover gaps in functionality
- No modification to working existing tests

### Test Structure Preservation
- Maintained similar test logic and assertions where possible
- Used Playwright best practices and patterns from existing tests
- Preserved the intention and coverage of original selenium tests

### Key Differences from Selenium

1. **Element Selection**: 
   - Selenium: `driver.getPageSource().contains("Job Name")`
   - Playwright: `await expect(page.getByText("Job Name")).toBeVisible()`

2. **Page Navigation**:
   - Selenium: `driver.get(url)`
   - Playwright: `await page.goto("index.html#/activities")`

3. **Wait Strategies**:
   - Selenium: `WebDriverWait` and explicit waits
   - Playwright: `await page.waitForLoadState("networkidle")`

4. **Assertions**:
   - Selenium: `System.out.println("Pass")` or `System.err.println("Fail")`
   - Playwright: `await expect(element).toBeVisible()`

## Known Issues

1. **HPCC-32297**: Files/Logical Files page has a rendering issue where not all items display correctly. The original selenium test for this was commented out, and the Playwright conversion includes handling for this known issue.

2. **Server Dependency**: The tests require an HPCC server to be running. The conversion maintains this requirement to ensure functional parity.

## Files Modified/Created

### New Files Created:
- `esp/src/tests/v9-activities-basic-converted.spec.ts`
- `esp/src/tests/v9-activity-enhanced.spec.ts`
- `esp/src/tests/v9-workunits-enhanced.spec.ts`
- `esp/src/tests/v9-files-logical.spec.ts`
- `esp/src/tests/v9-base-table.spec.ts`
- `esp/src/tests/conversion-summary.spec.ts`
- `esp/src/tests/SELENIUM_CONVERSION.md` (this file)

### Existing Files Unchanged:
- All existing Playwright tests remain unchanged
- Original selenium tests remain in place for reference
- Playwright configuration unchanged

## Running the Converted Tests

```bash
# Run all tests (includes both existing and converted)
npm run test

# Run only converted tests
npx playwright test tests/v9-activities-basic-converted.spec.ts
npx playwright test tests/v9-activity-enhanced.spec.ts
npx playwright test tests/v9-workunits-enhanced.spec.ts
npx playwright test tests/v9-files-logical.spec.ts
npx playwright test tests/v9-base-table.spec.ts
npx playwright test tests/conversion-summary.spec.ts

# Run conversion summary
npx playwright test tests/conversion-summary.spec.ts
```

## Validation

The conversion has been validated by:
1. TypeScript compilation passes for all new tests
2. Test structure follows Playwright best practices
3. Coverage matches or exceeds original selenium tests
4. Minimal changes approach maintained
5. All new functionality documented and explained