package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Problem codes: the machine-readable reason of an *Error, also the last part of its
// problem type (urn:searchlight:problem:<code>).
const (
	CodeInvalid          = "invalid_request"
	CodeUnauthorized     = "unauthorized"
	CodeForbidden        = "forbidden"
	CodeNotFound         = "not_found"
	CodeIndexNotFound    = "index_not_found"
	CodeDocumentNotFound = "document_not_found"
	CodeQueryNotFound    = "query_not_found"
	CodeMethod           = "method_not_allowed"
	CodeIndexExists      = "index_exists"
	CodeConflict         = "conflict"
	CodeTooLarge         = "too_large"
	CodeTooMany          = "too_many_requests"
	CodeTimeout          = "timeout"
	CodeRequestTimeout   = "request_timeout"
	CodeCanceled         = "client_closed_request"
	CodeUnavailable      = "unavailable"
	CodeInternal         = "internal"
)

// StatusClientClosedRequest is nginx's 499: the client went away before the answer.
// Nobody reads it; it labels the request's metrics and log.
const StatusClientClosedRequest = 499

// statusText is http.StatusText, with 499.
func statusText(code int) string {
	if code == StatusClientClosedRequest {
		return "Client Closed Request"
	}
	return http.StatusText(code)
}

// ProblemType prefixes every problem's type URI.
const ProblemType = "urn:searchlight:problem:"

// ProblemContentType is the media type of an error body (RFC 9457).
const ProblemContentType = "application/problem+json"

// DefaultRetryAfter is how long a 429 asks the client to wait.
const DefaultRetryAfter = time.Second

// Error is an API error: an HTTP status, a problem code and, for a request the API
// refuses, every problem found with its loc (a dotted path into the request, such as
// query.all.2.value). It is written as problem JSON.
type Error struct {
	Status   int
	Code     string
	Detail   string
	Problems []query.Problem
	// RetryAfter, when set, is sent as the Retry-After header (429 and 503).
	RetryAfter time.Duration
	// Extra holds further members of the problem object, such as a conflict's
	// current_seq.
	Extra map[string]any
	// Err is the cause, logged for 5xx errors and never sent.
	Err error
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Detail)
	for _, p := range e.Problems {
		msg += "; " + p.String()
	}
	return msg
}

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Err }

// Invalid is a 400 listing problems.
func Invalid(detail string, problems ...query.Problem) *Error {
	return &Error{Status: http.StatusBadRequest, Code: CodeInvalid, Detail: detail, Problems: problems}
}

// InvalidAt is a 400 with one problem at loc.
func InvalidAt(loc, format string, args ...any) *Error {
	msg := fmt.Sprintf(format, args...)
	return Invalid(msg, query.Problem{Loc: loc, Message: msg})
}

// NotFound is a 404 with code.
func NotFound(code, format string, args ...any) *Error {
	return &Error{Status: http.StatusNotFound, Code: code, Detail: fmt.Sprintf(format, args...)}
}

// Conflict is a 409.
func Conflict(code, format string, args ...any) *Error {
	return &Error{Status: http.StatusConflict, Code: code, Detail: fmt.Sprintf(format, args...)}
}

// TooLarge is a 413.
func TooLarge(format string, args ...any) *Error {
	return &Error{Status: http.StatusRequestEntityTooLarge, Code: CodeTooLarge, Detail: fmt.Sprintf(format, args...)}
}

// TooMany is a 429 asking the client to retry after retry.
func TooMany(retry time.Duration, format string, args ...any) *Error {
	return &Error{Status: http.StatusTooManyRequests, Code: CodeTooMany, Detail: fmt.Sprintf(format, args...), RetryAfter: retry}
}

// Unavailable is a 503 caused by err.
func Unavailable(err error, format string, args ...any) *Error {
	return &Error{Status: http.StatusServiceUnavailable, Code: CodeUnavailable, Detail: fmt.Sprintf(format, args...), Err: err, RetryAfter: DefaultRetryAfter}
}

// ProblemFor maps any error to the API error it is answered with:
//
//   - an *Error is itself;
//   - a *FieldError, a *search.RequestError and a *schema.ValidationError are 400s;
//   - store.ErrNotFound is a 404, store.ErrConflict (a failed if_seq) and
//     store.ErrExists are 409s;
//   - shard.ErrBackpressure is a 429 with Retry-After, and a document over the
//     segment limit a 413;
//   - a passed deadline is a 504, a closed or failed shard or store a 503;
//   - anything else is a 500, whose cause is logged and never sent.
func ProblemFor(err error) *Error {
	var e *Error
	var fe *FieldError
	var re *search.RequestError
	var ve *schema.ValidationError
	var ce *store.ConflictError
	switch {
	case errors.As(err, &e):
		return e
	case errors.As(err, &fe):
		return InvalidAt(fe.Loc, "%s", fe.Message)
	case errors.As(err, &re):
		return Invalid("the search request is invalid", re.Problems...)
	case errors.As(err, &ve):
		loc := "body"
		if ve.Field != "" {
			loc = "body." + ve.Field
		}
		return InvalidAt(loc, "%s", ve.Error())
	case errors.As(err, &ce):
		out := Conflict(CodeConflict, "the if_seq condition failed")
		if len(ce.Current) > 0 {
			out.Extra = map[string]any{"current_seq": ce.Current[0]}
		}
		return out
	case errors.Is(err, store.ErrConflict):
		return Conflict(CodeConflict, "the change conflicts with a concurrent one; retry")
	case errors.Is(err, store.ErrExists):
		return Conflict(CodeIndexExists, "it already exists")
	case errors.Is(err, store.ErrNotFound):
		return NotFound(CodeNotFound, "not found")
	case errors.Is(err, shard.ErrBackpressure):
		return &Error{Status: http.StatusTooManyRequests, Code: CodeTooMany, Detail: "the write buffer is full while refreshes catch up; retry", RetryAfter: DefaultRetryAfter, Err: err}
	case errors.Is(err, shard.ErrDocTooLarge):
		return TooLarge("the document is larger than a segment stores")
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Status: http.StatusGatewayTimeout, Code: CodeTimeout, Detail: "the request's deadline passed", Err: err}
	case errors.Is(err, context.Canceled):
		return &Error{Status: StatusClientClosedRequest, Code: CodeCanceled, Detail: "the client went away", Err: err}
	case errors.Is(err, shard.ErrClosed), errors.Is(err, shard.ErrFailed), errors.Is(err, store.ErrClosed):
		return Unavailable(err, "the node is shutting down or the copy is unavailable")
	}
	return &Error{Status: http.StatusInternalServerError, Code: CodeInternal, Detail: "internal error", Err: err}
}

// problemBody is the JSON of an *Error.
func problemBody(e *Error, requestID string) map[string]any {
	body := make(map[string]any, 8+len(e.Extra))
	for k, v := range e.Extra {
		body[k] = v
	}
	body["type"] = ProblemType + e.Code
	body["title"] = statusText(e.Status)
	body["status"] = e.Status
	body["code"] = e.Code
	if e.Detail != "" {
		body["detail"] = e.Detail
	}
	if len(e.Problems) > 0 {
		body["problems"] = e.Problems
	}
	if requestID != "" {
		body["request_id"] = requestID
	}
	return body
}

// writeProblem writes e as problem JSON.
func writeProblem(w http.ResponseWriter, e *Error, requestID string) {
	h := w.Header()
	h.Set("Content-Type", ProblemContentType)
	h.Set("Cache-Control", "no-store")
	if e.RetryAfter > 0 {
		h.Set("Retry-After", strconv.Itoa(int(max(1, (e.RetryAfter+time.Second-1)/time.Second))))
	}
	if e.Status == http.StatusUnauthorized {
		h.Set("WWW-Authenticate", `Bearer realm="searchlight"`)
	}
	b, err := json.Marshal(problemBody(e, requestID))
	if err != nil {
		b = []byte(`{"type":"` + ProblemType + CodeInternal + `","status":500}`)
	}
	w.WriteHeader(e.Status)
	_, _ = w.Write(append(b, '\n'))
}
