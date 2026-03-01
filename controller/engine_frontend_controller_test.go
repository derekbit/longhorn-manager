package controller

import (
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/longhorn/longhorn-manager/constant"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestShouldExpandEngineFrontend(c *C) {
	ef := &longhorn.EngineFrontend{}
	v := &longhorn.Volume{}

	c.Assert(shouldExpandEngineFrontend(nil, v), Equals, false)
	c.Assert(shouldExpandEngineFrontend(ef, nil), Equals, false)

	ef.Spec.VolumeSize = 0
	v.Status.ExpansionRequired = true
	c.Assert(shouldExpandEngineFrontend(ef, v), Equals, false)

	ef.Spec.VolumeSize = TestVolumeSize
	v.Status.ExpansionRequired = false
	c.Assert(shouldExpandEngineFrontend(ef, v), Equals, false)

	v.Status.ExpansionRequired = true
	c.Assert(shouldExpandEngineFrontend(ef, v), Equals, true)
}

type fakeEngineFrontendSwitchoverClient struct {
	callOrder        []string
	suspendErr       error
	switchErr        error
	resumeErr        error
	suspendCallCount int
	switchCallCount  int
	resumeCallCount  int

	suspendDataEngine longhorn.DataEngineType
	suspendName       string

	switchDataEngine    longhorn.DataEngineType
	switchName          string
	switchTargetAddress string
	switchEngineName    string

	resumeDataEngine longhorn.DataEngineType
	resumeName       string
}

func (f *fakeEngineFrontendSwitchoverClient) EngineFrontendSuspend(dataEngine longhorn.DataEngineType, name string) error {
	f.callOrder = append(f.callOrder, "suspend")
	f.suspendCallCount++
	f.suspendDataEngine = dataEngine
	f.suspendName = name
	return f.suspendErr
}

func (f *fakeEngineFrontendSwitchoverClient) EngineFrontendSwitchOverTarget(dataEngine longhorn.DataEngineType, name, targetAddress, engineName string) error {
	f.callOrder = append(f.callOrder, "switch")
	f.switchCallCount++
	f.switchDataEngine = dataEngine
	f.switchName = name
	f.switchTargetAddress = targetAddress
	f.switchEngineName = engineName
	return f.switchErr
}

func (f *fakeEngineFrontendSwitchoverClient) EngineFrontendResume(dataEngine longhorn.DataEngineType, name string) error {
	f.callOrder = append(f.callOrder, "resume")
	f.resumeCallCount++
	f.resumeDataEngine = dataEngine
	f.resumeName = name
	return f.resumeErr
}

func (s *TestSuite) TestSwitchEngineFrontendTarget(c *C) {
	targetAddress := "tcp://10.0.0.1:10000"

	testCases := []struct {
		name                 string
		suspendErr           error
		switchErr            error
		resumeErr            error
		expectedFailureType  switchoverFailureType
		expectedErrorPattern string
		expectedCallOrder    []string
		expectedSuspendCalls int
		expectedSwitchCalls  int
		expectedResumeCalls  int
	}{
		{
			name:                 "success",
			expectedFailureType:  switchoverFailureType(""),
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
		{
			name:                 "switch failure with recovery resume",
			switchErr:            errors.New("switch failed"),
			expectedFailureType:  switchoverFailureSwitch,
			expectedErrorPattern: ".*failed to switch over target for engine frontend ef-1.*",
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
		{
			name:                 "suspend failure",
			suspendErr:           errors.New("suspend failed"),
			expectedFailureType:  switchoverFailureSuspend,
			expectedErrorPattern: ".*failed to suspend engine frontend ef-1 before switchover.*",
			expectedCallOrder:    []string{"suspend"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  0,
			expectedResumeCalls:  0,
		},
		{
			name:                 "switch and resume failure",
			switchErr:            errors.New("switch failed"),
			resumeErr:            errors.New("resume failed"),
			expectedFailureType:  switchoverFailureSwitchAndResume,
			expectedErrorPattern: ".*failed to switch over target for engine frontend ef-1, then failed to resume:.*",
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
		{
			name:                 "resume failure after successful switch",
			resumeErr:            errors.New("resume failed"),
			expectedFailureType:  switchoverFailureResume,
			expectedErrorPattern: ".*failed to resume engine frontend ef-1 after switchover.*",
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
	}

	for _, tc := range testCases {
		ef := &longhorn.EngineFrontend{}
		ef.Name = "ef-1"
		ef.Spec.DataEngine = longhorn.DataEngineTypeV2

		client := &fakeEngineFrontendSwitchoverClient{
			suspendErr: tc.suspendErr,
			switchErr:  tc.switchErr,
			resumeErr:  tc.resumeErr,
		}

		failureType, err := switchEngineFrontendTarget(client, ef, targetAddress)
		caseInfo := Commentf("case=%s", tc.name)

		if tc.expectedErrorPattern == "" {
			c.Assert(err, IsNil, caseInfo)
		} else {
			c.Assert(err, NotNil, caseInfo)
			c.Assert(err.Error(), Matches, tc.expectedErrorPattern, caseInfo)
		}

		c.Assert(failureType, Equals, tc.expectedFailureType, caseInfo)
		c.Assert(client.callOrder, DeepEquals, tc.expectedCallOrder, caseInfo)
		c.Assert(client.suspendCallCount, Equals, tc.expectedSuspendCalls, caseInfo)
		c.Assert(client.switchCallCount, Equals, tc.expectedSwitchCalls, caseInfo)
		c.Assert(client.resumeCallCount, Equals, tc.expectedResumeCalls, caseInfo)

		if tc.expectedSwitchCalls > 0 {
			c.Assert(client.switchDataEngine, Equals, longhorn.DataEngineTypeV2, caseInfo)
			c.Assert(client.switchName, Equals, "ef-1", caseInfo)
			c.Assert(client.switchTargetAddress, Equals, targetAddress, caseInfo)
			c.Assert(client.switchEngineName, Equals, "", caseInfo)
		}
	}
}

func (s *TestSuite) TestGetEngineFrontendSwitchoverFailureEventMessage(c *C) {
	targetAddress := "10.1.2.3:9502"
	baseErr := errors.New("rpc failed")

	testCases := []struct {
		name            string
		failureType     switchoverFailureType
		expectedPattern string
	}{
		{
			name:            "suspend failure message",
			failureType:     switchoverFailureSuspend,
			expectedPattern: ".*Failed to suspend engine frontend before switchover to 10\\.1\\.2\\.3:9502: rpc failed.*",
		},
		{
			name:            "switch and resume failure message",
			failureType:     switchoverFailureSwitchAndResume,
			expectedPattern: ".*Failed to switch over target to 10\\.1\\.2\\.3:9502 and failed to resume engine frontend: rpc failed.*",
		},
		{
			name:            "resume failure message",
			failureType:     switchoverFailureResume,
			expectedPattern: ".*Switched over target to 10\\.1\\.2\\.3:9502 but failed to resume engine frontend: rpc failed.*",
		},
		{
			name:            "default switch failure message",
			failureType:     switchoverFailureSwitch,
			expectedPattern: ".*Failed to switch over target to 10\\.1\\.2\\.3:9502: rpc failed.*",
		},
	}

	for _, tc := range testCases {
		msg := getEngineFrontendSwitchoverFailureEventMessage(tc.failureType, targetAddress, baseErr)
		c.Assert(msg, Matches, tc.expectedPattern, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestRecordEngineFrontendSwitchoverFailureEvent(c *C) {
	ef := &longhorn.EngineFrontend{}
	ef.Name = "ef-1"
	targetAddress := "10.1.2.3:9502"
	baseErr := errors.New("rpc failed")

	testCases := []struct {
		name            string
		failureType     switchoverFailureType
		expectedMessage string
	}{
		{
			name:            "suspend failure event",
			failureType:     switchoverFailureSuspend,
			expectedMessage: "Failed to suspend engine frontend before switchover to 10.1.2.3:9502: rpc failed",
		},
		{
			name:            "switch and resume failure event",
			failureType:     switchoverFailureSwitchAndResume,
			expectedMessage: "Failed to switch over target to 10.1.2.3:9502 and failed to resume engine frontend: rpc failed",
		},
		{
			name:            "resume failure event",
			failureType:     switchoverFailureResume,
			expectedMessage: "Switched over target to 10.1.2.3:9502 but failed to resume engine frontend: rpc failed",
		},
		{
			name:            "default switch failure event",
			failureType:     switchoverFailureSwitch,
			expectedMessage: "Failed to switch over target to 10.1.2.3:9502: rpc failed",
		},
	}

	for _, tc := range testCases {
		fakeRecorder := record.NewFakeRecorder(5)
		recordEngineFrontendSwitchoverFailureEvent(fakeRecorder, ef, tc.failureType, targetAddress, baseErr)

		select {
		case event := <-fakeRecorder.Events:
			caseInfo := Commentf("case=%s event=%s", tc.name, event)
			c.Assert(strings.Contains(event, corev1.EventTypeWarning), Equals, true, caseInfo)
			c.Assert(strings.Contains(event, constant.EventReasonFailedSwitchover), Equals, true, caseInfo)
			c.Assert(strings.Contains(event, tc.expectedMessage), Equals, true, caseInfo)
		default:
			c.Fatalf("case=%s expected one recorded event", tc.name)
		}
	}
}
