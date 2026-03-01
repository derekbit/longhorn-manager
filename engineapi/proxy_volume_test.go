package engineapi

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestGetVolumeExpandTargetInstanceName(c *C) {
	engine := &longhorn.Engine{}
	engineName := "testvol-e-0"
	engineFrontendName := "testvol-ef-0"

	c.Assert(getVolumeExpandTargetInstanceName(engine, engineName, engineFrontendName), Equals, engineName)

	engineFrontend := &longhorn.EngineFrontend{}
	c.Assert(getVolumeExpandTargetInstanceName(engineFrontend, engineName, engineFrontendName), Equals, engineFrontendName)
}
