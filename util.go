package realis

import (
	"net/url"
	"strings"

	"github.com/aurora-scheduler/gorealis/v2/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

var ActiveStates = make(map[aurora.ScheduleStatus]bool)
var SlaveAssignedStates = make(map[aurora.ScheduleStatus]bool)
var LiveStates = make(map[aurora.ScheduleStatus]bool)
var TerminalStates = make(map[aurora.ScheduleStatus]bool)
var ActiveJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)
var AwaitingPulseJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)

func init() {
	for _, status := range aurora.ACTIVE_STATES {
		ActiveStates[status] = true
	}

	for _, status := range aurora.SLAVE_ASSIGNED_STATES {
		SlaveAssignedStates[status] = true
	}

	for _, status := range aurora.LIVE_STATES {
		LiveStates[status] = true
	}

	for _, status := range aurora.TERMINAL_STATES {
		TerminalStates[status] = true
	}

	for _, status := range aurora.ACTIVE_JOB_UPDATE_STATES {
		ActiveJobUpdateStates[status] = true
	}
	for _, status := range aurora.AWAITNG_PULSE_JOB_UPDATE_STATES {
		AwaitingPulseJobUpdateStates[status] = true
	}
}

// TerminalJobUpdateStates returns a slice containing all the terminal states an update may end up in.
// This is a function in order to avoid having a slice that can be accidentally mutated.
func TerminalUpdateStates() []aurora.JobUpdateStatus {
	return []aurora.JobUpdateStatus{
		aurora.JobUpdateStatus_ROLLED_FORWARD,
		aurora.JobUpdateStatus_ROLLED_BACK,
		aurora.JobUpdateStatus_ABORTED,
		aurora.JobUpdateStatus_ERROR,
		aurora.JobUpdateStatus_FAILED,
	}
}

func validateAuroraAddress(address string) (string, error) {

	// If no protocol defined, assume http
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}

	u, err := url.Parse(address)

	if err != nil {
		return "", errors.Wrap(err, "error parsing url")
	}

	// If no path provided assume /api
	if u.Path == "" {
		u.Path = "/api"
	}

	// If no port provided, assume default 8081
	if u.Port() == "" {
		u.Host = u.Host + ":8081"
	}

	if !(u.Scheme == "http" || u.Scheme == "https") {
		return "", errors.Errorf("only protocols http and https are supported %v\n", u.Scheme)
	}

	if u.Path != "/api" {
		return "", errors.Errorf("expected /api path %v\n", u.Path)
	}

	return u.String(), nil
}

func calculateCurrentBatch(updatingInstances int32, batchSizes []int32) int {
	for i, size := range batchSizes {
		updatingInstances -= size
		if updatingInstances <= 0 {
			return i
		}
	}

	// Overflow batches
	batchCount := len(batchSizes) - 1
	lastBatchIndex := len(batchSizes) - 1
	batchCount += int(updatingInstances / batchSizes[lastBatchIndex])

	if updatingInstances%batchSizes[lastBatchIndex] != 0 {
		batchCount++
	}
	return batchCount
}
