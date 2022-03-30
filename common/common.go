package common

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/cyverse-de/model/v6"
	"github.com/cyverse-de/vice-launcher/logging"
	"github.com/gosimple/slug"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "common"})
var HTTPClient = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

// AnalysisStatusPublisher is the interface for types that need to publish a job
// update.
type AnalysisStatusPublisher interface {
	Fail(ctx context.Context, jobID, msg string) error
	Success(ctx context.Context, jobID, msg string) error
	Running(ctx context.Context, jobID, msg string) error
}

type JobLabeller interface {
	LabelsFromJob(context.Context, *model.Job) (map[string]string, error)
}

// IngressName returns the name of the ingress created for the running VICE
// analysis. This should match the name created in the apps service.
func IngressName(userID, invocationID string) string {
	return fmt.Sprintf("a%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s", userID, invocationID))))[0:9]
}

var leadingLabelReplacerRegexp = regexp.MustCompile("^[^0-9A-Za-z]+")
var trailingLabelReplacerRegexp = regexp.MustCompile("[^0-9A-Za-z]+$")

// labelReplacerFn returns a function that can be used to replace invalid leading and trailing characters
// in label values. Hyphens are replaced by the letter "h". Underscores are replaced by the letter "u".
// Other characters in the match are replaced by the empty string. The prefix and suffix are placed before
// and after the replacement, respectively.
func labelReplacerFn(prefix, suffix string) func(string) string {
	replacementFor := map[rune]string{
		'-': "h",
		'_': "u",
	}

	return func(match string) string {
		runes := []rune(match)
		elems := make([]string, len(runes))
		for i, c := range runes {
			elems[i] = replacementFor[c]
		}
		return prefix + strings.Join(elems, "-") + suffix
	}
}

// LabelValueString returns a version of the given string that may be used as a value in a Kubernetes
// label. See: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/. Leading and
// trailing underscores and hyphens are replaced by sequences of `u` and `h`, separated by hyphens.
// These sequences are separated from the main part of the label value by `-xxx-`. This is kind of
// hokey, but it makes it at least fairly unlikely that we'll encounter collisions.
func LabelValueString(str string) string {
	slug.MaxLength = 63
	str = leadingLabelReplacerRegexp.ReplaceAllStringFunc(str, labelReplacerFn("", "-xxx-"))
	str = trailingLabelReplacerRegexp.ReplaceAllStringFunc(str, labelReplacerFn("-xxx-", ""))
	return slug.Make(str)
}

// ErrorResponse represents an HTTP response body containing error information. This type implements
// the error interface so that it can be returned as an error from from existing functions.
//
// swagger:response errorResponse
type ErrorResponse struct {
	Message   string                  `json:"message"`
	ErrorCode string                  `json:"error_code,omitempty"`
	Details   *map[string]interface{} `json:"details,omitempty"`
}

// ErrorBytes returns a byte-array representation of an ErrorResponse.
func (e ErrorResponse) ErrorBytes() []byte {
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Errorf("unable to marshal %+v as JSON", e)
		return make([]byte, 0)
	}
	return bytes
}

// Error returns a string representation of an ErrorResponse.
func (e ErrorResponse) Error() string {
	return string(e.ErrorBytes())
}
