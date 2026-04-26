// Middleware is applied via tower layers in main.rs.
// Rate limiting via Redis token bucket is wired directly in handlers
// to keep the middleware stack thin and testable.
