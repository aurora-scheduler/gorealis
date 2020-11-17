/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package realis

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aurora-scheduler/gorealis/v2/gen-go/apache/aurora"
)

// Structure to collect all information required to create job update
type JobUpdate struct {
	task    *AuroraTask
	request *aurora.JobUpdateRequest
}

// Create a default JobUpdate object with an empty task and no fields filled in.
func NewJobUpdate() *JobUpdate {
	newTask := NewTask()

	return &JobUpdate{
		task:    newTask,
		request: &aurora.JobUpdateRequest{TaskConfig: newTask.TaskConfig(), Settings: newUpdateSettings()},
	}
}

// Creates an update with default values using an AuroraTask as the underlying task configuration.
// This function has a high level understanding of Aurora Tasks and thus will support copying a task that is configured
// to use Thermos.
func JobUpdateFromAuroraTask(task *AuroraTask) *JobUpdate {
	newTask := task.Clone()

	return &JobUpdate{
		task:    newTask,
		request: &aurora.JobUpdateRequest{TaskConfig: newTask.TaskConfig(), Settings: newUpdateSettings()},
	}
}

// JobUpdateFromConfig creates an update with default values using an aurora.TaskConfig
// primitive as the underlying task configuration.
// This function should not be used unless the implications of using a primitive value are understood.
// For example, the primitive has no concept of Thermos.
func JobUpdateFromConfig(task *aurora.TaskConfig) *JobUpdate {
	// Perform a deep copy to avoid unexpected behavior
	newTask := TaskFromThrift(task)

	return &JobUpdate{
		task:    newTask,
		request: &aurora.JobUpdateRequest{TaskConfig: newTask.TaskConfig(), Settings: newUpdateSettings()},
	}
}

// Set instance count the job will have after the update.
func (j *JobUpdate) InstanceCount(inst int32) *JobUpdate {
	j.request.InstanceCount = inst
	return j
}

// Max number of instances being updated at any given moment.
func (j *JobUpdate) BatchSize(size int32) *JobUpdate {
	j.request.Settings.UpdateGroupSize = size
	return j
}

// Minimum number of seconds a shard must remain in RUNNING state before considered a success.
func (j *JobUpdate) WatchTime(timeout time.Duration) *JobUpdate {
	j.request.Settings.MinWaitInInstanceRunningMs = int32(timeout.Seconds() * 1000)
	return j
}

// Wait for all instances in a group to be done before moving on.
func (j *JobUpdate) WaitForBatchCompletion(batchWait bool) *JobUpdate {
	j.request.Settings.WaitForBatchCompletion = batchWait
	return j
}

// Max number of instance failures to tolerate before marking instance as FAILED.
func (j *JobUpdate) MaxPerInstanceFailures(inst int32) *JobUpdate {
	j.request.Settings.MaxPerInstanceFailures = inst
	return j
}

// Max number of FAILED instances to tolerate before terminating the update.
func (j *JobUpdate) MaxFailedInstances(inst int32) *JobUpdate {
	j.request.Settings.MaxFailedInstances = inst
	return j
}

// When False, prevents auto rollback of a failed update.
func (j *JobUpdate) RollbackOnFail(rollback bool) *JobUpdate {
	j.request.Settings.RollbackOnFailure = rollback
	return j
}

// Sets the interval at which pulses should be received by the job update before timing out.
func (j *JobUpdate) PulseIntervalTimeout(timeout time.Duration) *JobUpdate {
	j.request.Settings.BlockIfNoPulsesAfterMs = thrift.Int32Ptr(int32(timeout.Seconds() * 1000))
	return j
}
func (j *JobUpdate) BatchUpdateStrategy(autoPause bool, batchSize int32) *JobUpdate {
	j.request.Settings.UpdateStrategy = &aurora.JobUpdateStrategy{
		BatchStrategy: &aurora.BatchJobUpdateStrategy{GroupSize: batchSize, AutopauseAfterBatch: autoPause},
	}
	return j
}

func (j *JobUpdate) QueueUpdateStrategy(groupSize int32) *JobUpdate {
	j.request.Settings.UpdateStrategy = &aurora.JobUpdateStrategy{
		QueueStrategy: &aurora.QueueJobUpdateStrategy{GroupSize: groupSize},
	}
	return j
}

func (j *JobUpdate) VariableBatchStrategy(autoPause bool, batchSizes ...int32) *JobUpdate {
	j.request.Settings.UpdateStrategy = &aurora.JobUpdateStrategy{
		VarBatchStrategy: &aurora.VariableBatchJobUpdateStrategy{GroupSizes: batchSizes, AutopauseAfterBatch: autoPause},
	}
	return j
}

// SlaAware makes the scheduler enforce the SLA Aware policy if the job meets the SLA awareness criteria.
// By default, the scheduler will only apply SLA Awareness to jobs in the production tier with 20 or more instances.
func (j *JobUpdate) SlaAware(slaAware bool) *JobUpdate {
	j.request.Settings.SlaAware = &slaAware
	return j
}

// AddInstanceRange allows updates to only touch a certain specific range of instances
func (j *JobUpdate) AddInstanceRange(first, last int32) *JobUpdate {
	j.request.Settings.UpdateOnlyTheseInstances = append(j.request.Settings.UpdateOnlyTheseInstances,
		&aurora.Range{First: first, Last: last})
	return j
}

func newUpdateSettings() *aurora.JobUpdateSettings {
	us := aurora.JobUpdateSettings{}
	// Mirrors defaults set by Pystachio
	us.UpdateOnlyTheseInstances = []*aurora.Range{}
	us.UpdateGroupSize = 1
	us.WaitForBatchCompletion = false
	us.MinWaitInInstanceRunningMs = 45000
	us.MaxPerInstanceFailures = 0
	us.MaxFailedInstances = 0
	us.RollbackOnFailure = true

	return &us
}

/*
   These methods are provided for user convenience in order to chain
   calls for configuration.
   API below here are wrappers around modifying an AuroraTask instance.
   See task.go for further documentation.
*/

func (j *JobUpdate) Environment(env string) *JobUpdate {
	j.task.Environment(env)
	return j
}

func (j *JobUpdate) Role(role string) *JobUpdate {
	j.task.Role(role)
	return j
}

func (j *JobUpdate) Name(name string) *JobUpdate {
	j.task.Name(name)
	return j
}

func (j *JobUpdate) ExecutorName(name string) *JobUpdate {
	j.task.ExecutorName(name)
	return j
}

func (j *JobUpdate) ExecutorData(data string) *JobUpdate {
	j.task.ExecutorData(data)
	return j
}

func (j *JobUpdate) CPU(cpus float64) *JobUpdate {
	j.task.CPU(cpus)
	return j
}

func (j *JobUpdate) RAM(ram int64) *JobUpdate {
	j.task.RAM(ram)
	return j
}

func (j *JobUpdate) Disk(disk int64) *JobUpdate {
	j.task.Disk(disk)
	return j
}

func (j *JobUpdate) Tier(tier string) *JobUpdate {
	j.task.Tier(tier)
	return j
}

func (j *JobUpdate) TaskMaxFailure(maxFail int32) *JobUpdate {
	j.task.MaxFailure(maxFail)
	return j
}

func (j *JobUpdate) IsService(isService bool) *JobUpdate {
	j.task.IsService(isService)
	return j
}

func (j *JobUpdate) TaskConfig() *aurora.TaskConfig {
	return j.task.TaskConfig()
}

func (j *JobUpdate) AddURIs(extract bool, cache bool, values ...string) *JobUpdate {
	j.task.AddURIs(extract, cache, values...)
	return j
}

func (j *JobUpdate) AddLabel(key string, value string) *JobUpdate {
	j.task.AddLabel(key, value)
	return j
}

func (j *JobUpdate) AddNamedPorts(names ...string) *JobUpdate {
	j.task.AddNamedPorts(names...)
	return j
}

func (j *JobUpdate) AddPorts(num int) *JobUpdate {
	j.task.AddPorts(num)
	return j
}
func (j *JobUpdate) AddValueConstraint(name string, negated bool, values ...string) *JobUpdate {
	j.task.AddValueConstraint(name, negated, values...)
	return j
}

func (j *JobUpdate) AddLimitConstraint(name string, limit int32) *JobUpdate {
	j.task.AddLimitConstraint(name, limit)
	return j
}

func (j *JobUpdate) AddDedicatedConstraint(role, name string) *JobUpdate {
	j.task.AddDedicatedConstraint(role, name)
	return j
}

func (j *JobUpdate) Container(container Container) *JobUpdate {
	j.task.Container(container)
	return j
}

func (j *JobUpdate) JobKey() aurora.JobKey {
	return j.task.JobKey()
}

func (j *JobUpdate) ThermosExecutor(thermos ThermosExecutor) *JobUpdate {
	j.task.ThermosExecutor(thermos)
	return j
}

func (j *JobUpdate) BuildThermosPayload() error {
	return j.task.BuildThermosPayload()
}

func (j *JobUpdate) PartitionPolicy(reschedule bool, delay int64) *JobUpdate {
	j.task.PartitionPolicy(aurora.PartitionPolicy{
		Reschedule: reschedule,
		DelaySecs:  &delay,
	})
	return j
}
