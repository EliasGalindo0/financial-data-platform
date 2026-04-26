// Repository layer — thin wrappers over sqlx queries.
// Purpose: testable DB access, separate from HTTP handler logic.
// In handlers, we inline queries for clarity; in a larger codebase
// you'd move them here and mock this trait in unit tests.
