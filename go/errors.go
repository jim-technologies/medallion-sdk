package medallion

import "fmt"

type Error struct {
	Code      string
	RequestID string
	Message   string
}

func (e *Error) Error() string {
	return e.Message
}

type APIError struct {
	Status       int
	RequestID    string
	ResponseBody any
	Message      string
}

func (e *APIError) Error() string {
	return e.Message
}

func invalidOptions(message string) error {
	return &Error{Code: "MEDALLION_INVALID_OPTIONS", Message: message}
}

func missingOption(code, message string) error {
	return &Error{Code: code, Message: message}
}

func invalidID(path string) error {
	return &Error{
		Code:    "MEDALLION_INVALID_ID",
		Message: fmt.Sprintf("invalid ID at %s: expected string or integer", path),
	}
}
