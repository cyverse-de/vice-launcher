package internal

import (
	"net/http"
	"testing"

	"github.com/cyverse-de/app-exposer/common"
)

func verifyStatus(t *testing.T, actual, expected int) {
	if expected != actual {
		t.Errorf("status code was %d, not %d", actual, expected)
	}
}

func verifyNoError(t *testing.T, actual error) {
	if actual != nil {
		t.Errorf("an error was returned where none was expected")
	}
}

func verifyErrorCode(t *testing.T, err error, expected string) {

	// It's a problem if there is no error.
	if err == nil {
		t.Errorf("no error was returned when one was expected")
		return
	}

	// It's a problem if the wrong type of error was returned.
	var errorResponse common.ErrorResponse
	switch val := err.(type) {
	case common.ErrorResponse:
		errorResponse = val
	default:
		t.Errorf("a general error was returned when a custom error was expected")
		return
	}

	// It's a problem if the error code is not what we expect.
	actual := errorResponse.ErrorCode
	if actual != expected {
		t.Errorf("error code was %s instead of %s", actual, expected)
	}
}

func TestDefaultLimitNotReached(t *testing.T) {
	status, err := validateJobLimits("foo", 2, 1, nil)
	verifyStatus(t, status, http.StatusOK)
	verifyNoError(t, err)
}

func TestExplicitLimitNotReached(t *testing.T) {
	limit := 2
	status, err := validateJobLimits("foo", 0, 1, &limit)
	verifyStatus(t, status, http.StatusOK)
	verifyNoError(t, err)
}

func TestDefaultLimitReached(t *testing.T) {
	status, err := validateJobLimits("foo", 2, 2, nil)
	verifyStatus(t, status, http.StatusBadRequest)
	verifyErrorCode(t, err, "ERR_LIMIT_REACHED")
}

func TestExplicitLimitReached(t *testing.T) {
	limit := 2
	status, err := validateJobLimits("foo", 0, 2, &limit)
	verifyStatus(t, status, http.StatusBadRequest)
	verifyErrorCode(t, err, "ERR_LIMIT_REACHED")
}

func TestExplicitPermissionNotGranted(t *testing.T) {
	status, err := validateJobLimits("foo", 0, 0, nil)
	verifyStatus(t, status, http.StatusBadRequest)
	verifyErrorCode(t, err, "ERR_PERMISSION_NEEDED")
}

func TestBannedUser(t *testing.T) {
	limit := 0
	status, err := validateJobLimits("foo", 0, 0, &limit)
	verifyStatus(t, status, http.StatusBadRequest)
	verifyErrorCode(t, err, "ERR_FORBIDDEN")
}
