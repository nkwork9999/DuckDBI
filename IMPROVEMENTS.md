# Improvement progress

Target: 100 independently reviewable improvements. Completed in this batch: 25. The target is not complete. Test cases and formatting edits are not counted separately.

1. JSON result column names escape quotes and control bytes.
2. JSON result strings escape every ASCII control byte.
3. Query and route exception messages use the JSON encoder.
4. Boolean result cells retain JSON boolean types.
5. Non-finite numeric values and NULL aggregate results emit JSON null.
6. All profiling/statistics table and column SQL identifiers are quoted.
7. Profiling metadata SQL string literals are escaped.
8. Profile and column names/types use JSON encoding.
9. Profile min/max and top-value strings use JSON encoding.
10. Query result headers, cells and errors escape HTML.
11. Table selectors use DOM text and event listeners.
12. Table selection no longer depends on the ambient browser event.
13. Distribution queries quote SQL identifiers.
14. Profile names/types/min/max escape HTML and column clicks use listeners.
15. Dashboard chart titles escape HTML.
16. Zero medians and zero-length strings are displayed accurately.
17. Missing numeric aggregates display as missing instead of zero.
18. Chart data conversion preserves zero and NULL.
19. Distribution input filters non-finite values.
20. Invalid ports and empty/NUL hosts are rejected.
21. Bind failures are reported synchronously.
22. Browser launch waits for server readiness.
23. Finished listener threads are joined during shutdown.
24. Standalone encoding and frontend regression runners cover hostile strings.
25. Linux and macOS CI run the regression suite.

## Validation

Standalone C++ encoding regressions and embedded JavaScript regressions passed. Translation unit passed clang++ syntax checking against miniplot DuckDB headers. Full extension integration remains unverified.
