// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import "net/http"

type Code int

const (
	Invalid         Code = -1
	Ok                   = 0
	InvalidParams        = http.StatusBadRequest
	BadRequest           = http.StatusBadRequest
	NotFound             = http.StatusNotFound
	TooManyRequests      = http.StatusTooManyRequests
	Internal             = http.StatusInternalServerError

	// HTTPCodeUpperBound is a bound under which any Code should have the same meaning with the http status code.
	HTTPCodeUpperBound   = Code(1000)
	PrintHelpUsage       = 1001
	ClusterAlreadyExists = 1002
)

// ToHTTPCode converts the Code to http code.
// The Code below the HTTPCodeUpperBound has the same meaning as the http status code. However, for the other codes, we
// should define the conversion rules by ourselves.
func (c Code) ToHTTPCode() int {
	if c < HTTPCodeUpperBound {
		return int(c)
	}

	// TODO: use switch to convert the code to http code.
	return int(c)
}
