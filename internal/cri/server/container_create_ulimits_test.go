package server

import (
	"testing"

	"github.com/containerd/containerd/v2/internal/cri/config"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestContainerSpecUlimits(t *testing.T) {
	testID := "test-id"
	testSandboxID := "sandbox-id"
	testContainerName := "container-name"
	testPid := uint32(1234)
	containerConfig, sandboxConfig, imageConfig, specCheck := getCreateContainerTestData()
	ociRuntime := config.Runtime{}
	c := newTestCRIService()

	ulimits := []*runtime.Ulimit{
		{
			Name: "nofile",
			Hard: 1024,
			Soft: 512,
		},
		{
			Name: "RLIMIT_CPU",
			Hard: 100,
			Soft: 50,
		},
	}
	containerConfig.Linux.SecurityContext.Ulimits = ulimits

	// Force Linux platform to trigger buildLinuxSpec
	linuxPlatform := imagespec.Platform{OS: "linux", Architecture: "amd64"}

	spec, err := c.buildContainerSpec(linuxPlatform, testID, testSandboxID, testPid, "", testContainerName, testImageName, containerConfig, sandboxConfig, imageConfig, nil, ociRuntime, nil)
	require.NoError(t, err)
	specCheck(t, testID, testSandboxID, testPid, spec)

	require.Len(t, spec.Process.Rlimits, 2)

	// Check nofile (should have RLIMIT_ prefix added)
	assert.Equal(t, "RLIMIT_NOFILE", spec.Process.Rlimits[0].Type)
	assert.Equal(t, uint64(1024), spec.Process.Rlimits[0].Hard)
	assert.Equal(t, uint64(512), spec.Process.Rlimits[0].Soft)

	// Check RLIMIT_CPU (should keep RLIMIT_ prefix)
	assert.Equal(t, "RLIMIT_CPU", spec.Process.Rlimits[1].Type)
	assert.Equal(t, uint64(100), spec.Process.Rlimits[1].Hard)
	assert.Equal(t, uint64(50), spec.Process.Rlimits[1].Soft)
}
