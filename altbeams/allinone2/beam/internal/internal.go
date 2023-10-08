package internal

// NotForPublicUse is a marker type that an API is for internal use only.
// It does not perfectly prevent usage of that API, but helps to restrict usage.
// Anything with this marker is not covered by the Go compatibility agreement.
type NotForPublicUse struct{}

// AllowInternalUse is passed from "json" to "jsontext" to authenticate
// that the caller can have access to internal functionality.
var AllowInternalUse NotForPublicUse
