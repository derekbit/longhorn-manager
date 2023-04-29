package engineapi

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	CurrentInstanceManagerAPIVersion = 4
	MinInstanceManagerAPIVersion     = 1
	UnknownInstanceManagerAPIVersion = 0

	UnknownInstanceManagerProxyAPIVersion = 0
	// UnsupportedInstanceManagerProxyAPIVersion means the instance manager without the proxy client (Longhorn release before v1.3.0)
	UnsupportedInstanceManagerProxyAPIVersion = 0

	DefaultEnginePortCount  = 1
	DefaultReplicaPortCount = 15

	DefaultPortArg         = "--listen,0.0.0.0:"
	DefaultTerminateSignal = "SIGHUP"

	// IncompatibleInstanceManagerAPIVersion means the instance manager version in v0.7.0
	IncompatibleInstanceManagerAPIVersion = -1
	DeprecatedInstanceManagerBinaryName   = "longhorn-instance-manager"
)

type InstanceManagerClient struct {
	ip            string
	apiMinVersion int
	apiVersion    int

	// The gRPC client supports backward compatibility.
	instanceServiceGrpcClient *imclient.InstanceServiceClient
	processManagerGrpcClient  *imclient.ProcessManagerClient
	diskServiceGrpcClient     *imclient.DiskServiceClient
}

func (c *InstanceManagerClient) GetAPIVersion() int {
	return c.apiVersion
}

func (c *InstanceManagerClient) Close() error {
	if c.processManagerGrpcClient == nil {
		return nil
	}

	return c.processManagerGrpcClient.Close()
}

func GetDeprecatedInstanceManagerBinary(image string) string {
	cname := types.GetImageCanonicalName(image)
	return filepath.Join(types.EngineBinaryDirectoryOnHost, cname, DeprecatedInstanceManagerBinaryName)
}

func CheckInstanceManagerCompatibility(imMinVersion, imVersion int) error {
	if MinInstanceManagerAPIVersion > imVersion || CurrentInstanceManagerAPIVersion < imMinVersion {
		return fmt.Errorf("current InstanceManager version %v-%v is not compatible with InstanceManagerAPIVersion %v and InstanceManagerAPIMinVersion %v",
			CurrentInstanceManagerAPIVersion, MinInstanceManagerAPIVersion, imVersion, imMinVersion)
	}
	return nil
}

func CheckInstanceManagerProxySupport(im *longhorn.InstanceManager) error {
	if UnsupportedInstanceManagerProxyAPIVersion == im.Status.ProxyAPIVersion {
		return fmt.Errorf("%v does not support proxy", im.Name)
	}
	return nil
}

// NewInstanceManagerClient creates a new instance manager client
func NewInstanceManagerClient(im *longhorn.InstanceManager) (*InstanceManagerClient, error) {
	// Do not check the major version here. Since IM cannot get the major version without using this client to call VersionGet().
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning || im.Status.IP == "" {
		return nil, fmt.Errorf("invalid Instance Manager %v, state: %v, IP: %v", im.Name, im.Status.CurrentState, im.Status.IP)
	}

	initProcessManagerTLSClient := func(endpoint string) (*imclient.ProcessManagerClient, error) {
		// check for tls cert file presence
		pmClient, err := imclient.NewProcessManagerClientWithTLS(endpoint,
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCAFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCertFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSKeyFile),
			"longhorn-backend.longhorn-system",
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load Instance Manager Client TLS files Error: %w", err)
		}

		if _, err = pmClient.VersionGet(); err != nil {
			return nil, fmt.Errorf("failed to check check version of Instance Manager Client with TLS connection Error: %w", err)
		}

		return pmClient, nil
	}

	initInstanceServiceTLSClient := func(endpoint string) (*imclient.InstanceServiceClient, error) {
		// check for tls cert file presence
		instanceClient, err := imclient.NewInstanceServiceClientWithTLS(endpoint,
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCAFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCertFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSKeyFile),
			"longhorn-backend.longhorn-system",
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load Instance Manager Instance Service Client TLS files Error: %w", err)
		}

		if _, err = instanceClient.VersionGet(); err != nil {
			return nil, fmt.Errorf("failed to check check version of Instance Manager Instance Service Client with TLS connection Error: %w", err)
		}

		return instanceClient, nil
	}

	initDiskServiceTLSClient := func(endpoint string) (*imclient.DiskServiceClient, error) {
		// check for tls cert file presence
		diskClient, err := imclient.NewDiskServiceClientWithTLS(endpoint,
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCAFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCertFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSKeyFile),
			"longhorn-backend.longhorn-system",
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load Instance Manager Disk Service Client TLS files Error: %w", err)
		}

		return diskClient, nil
	}

	// Create a new process manager client
	// HACK: TODO: fix me
	endpoint := "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerProcessManagerServiceDefaultPort)
	processManagerClient, err := initProcessManagerTLSClient(endpoint)
	if err != nil {
		// fallback to non tls client, there is no way to differentiate between im versions unless we get the version via the im client
		// TODO: remove this im client fallback mechanism in a future version maybe 2.4 / 2.5 or the next time we update the api version
		processManagerClient, err = imclient.NewProcessManagerClient(endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Instance Manager Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}

		if _, err = processManagerClient.VersionGet(); err != nil {
			return nil, fmt.Errorf("failed to get Version of Instance Manager Process Manager Service Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}
	}

	// Create a new instance service  client
	endpoint = "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerInstanceServiceDefaultPort)
	instanceServiceClient, err := initInstanceServiceTLSClient(endpoint)
	if err != nil {
		// fallback to non tls client, there is no way to differentiate between im versions unless we get the version via the im client
		// TODO: remove this im client fallback mechanism in a future version maybe 2.4 / 2.5 or the next time we update the api version
		instanceServiceClient, err = imclient.NewInstanceServiceClient(endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Instance Manager Instance Service Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}

		if _, err = instanceServiceClient.VersionGet(); err != nil {
			return nil, fmt.Errorf("failed to get Version of Instance Manager Instance Service Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}
	}

	// Create a new disk service client
	endpoint = "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerDiskServiceDefaultPort)
	diskServiceClient, err := initDiskServiceTLSClient(endpoint)
	if err != nil {
		// fallback to non tls client, there is no way to differentiate between im versions unless we get the version via the im client
		// TODO: remove this im client fallback mechanism in a future version maybe 2.4 / 2.5 or the next time we update the api version
		diskServiceClient, err = imclient.NewDiskServiceClient(endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Instance Manager Disk Service Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}
	}

	// TODO: consider evaluating im client version since we do the call anyway to validate the connection, i.e. fallback to non tls
	//  This way we don't need the per call compatibility check, ref: `CheckInstanceManagerCompatibility`

	return &InstanceManagerClient{
		ip:                        im.Status.IP,
		apiMinVersion:             im.Status.APIMinVersion,
		apiVersion:                im.Status.APIVersion,
		instanceServiceGrpcClient: instanceServiceClient,
		processManagerGrpcClient:  processManagerClient,
		diskServiceGrpcClient:     diskServiceClient,
	}, nil
}

func parseInstance(p *imapi.Instance) *longhorn.InstanceProcess {
	if p == nil {
		return nil
	}

	return &longhorn.InstanceProcess{
		Spec: longhorn.InstanceProcessSpec{
			Name:               p.Name,
			BackendStoreDriver: longhorn.BackendStoreDriverType(p.BackendStoreDriver),
		},
		Status: longhorn.InstanceProcessStatus{
			Type:      getTypeForInstance(longhorn.InstanceType(p.Type), p.PortCount),
			State:     longhorn.InstanceState(p.InstanceStatus.State),
			ErrorMsg:  p.InstanceStatus.ErrorMsg,
			PortStart: p.InstanceStatus.PortStart,
			PortEnd:   p.InstanceStatus.PortEnd,

			// These fields are not used, maybe we can deprecate them later.
			Listen:   "",
			Endpoint: "",
		},
	}
}

func (c *InstanceManagerClient) parseProcess(p *imapi.Process) *longhorn.InstanceProcess {
	if p == nil {
		return nil
	}

	return &longhorn.InstanceProcess{
		Spec: longhorn.InstanceProcessSpec{
			Name: p.Name,
		},
		Status: longhorn.InstanceProcessStatus{
			Type:      getTypeForProcess(p.PortCount),
			State:     longhorn.InstanceState(p.ProcessStatus.State),
			ErrorMsg:  p.ProcessStatus.ErrorMsg,
			PortStart: p.ProcessStatus.PortStart,
			PortEnd:   p.ProcessStatus.PortEnd,

			// These fields are not used, maybe we can deprecate them later.
			Listen:   "",
			Endpoint: "",
		},
	}
}

func getTypeForInstance(instanceType longhorn.InstanceType, portCount int32) longhorn.InstanceType {
	if instanceType != longhorn.InstanceType("") {
		return instanceType
	}

	if portCount == DefaultEnginePortCount {
		return longhorn.InstanceTypeEngine
	}
	return longhorn.InstanceTypeReplica
}

func getTypeForProcess(portCount int32) longhorn.InstanceType {
	if portCount == DefaultEnginePortCount {
		return longhorn.InstanceTypeEngine
	}
	return longhorn.InstanceTypeReplica
}

func getBinaryAndArgsForEngineProcessCreation(e *longhorn.Engine,
	volumeFrontend longhorn.VolumeFrontend, engineReplicaTimeout, replicaFileSyncHTTPClientTimeout int64,
	dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (string, []string, error) {

	frontend, err := GetEngineProcessFrontend(volumeFrontend)
	if err != nil {
		return "", nil, err
	}
	args := []string{"controller", e.Spec.VolumeName,
		"--frontend", frontend,
	}

	if e.Spec.RevisionCounterDisabled {
		args = append(args, "--disableRevCounter")
	}

	if e.Spec.SalvageRequested {
		args = append(args, "--salvageRequested")
	}

	if engineCLIAPIVersion >= 6 {
		args = append(args,
			"--size", strconv.FormatInt(e.Spec.VolumeSize, 10),
			"--current-size", strconv.FormatInt(e.Status.CurrentSize, 10))
	}

	if engineCLIAPIVersion >= 7 {
		args = append(args,
			"--engine-replica-timeout", strconv.FormatInt(engineReplicaTimeout, 10),
			"--file-sync-http-client-timeout", strconv.FormatInt(replicaFileSyncHTTPClientTimeout, 10))

		if dataLocality == longhorn.DataLocalityStrictLocal {
			args = append(args, "--data-server-protocol", "unix")
		}

		if e.Spec.UnmapMarkSnapChainRemovedEnabled {
			args = append(args, "--unmap-mark-snap-chain-removed")
		}
	}

	for _, addr := range e.Status.CurrentReplicaAddressMap {
		args = append(args, "--replica", GetBackendReplicaURL(addr))
	}
	binary := filepath.Join(types.GetEngineBinaryDirectoryForEngineManagerContainer(e.Spec.EngineImage), types.EngineBinaryName)

	return binary, args, nil
}

func getBinaryAndArgsForReplicaProcessCreation(r *longhorn.Replica,
	dataPath, backingImagePath string, dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (string, []string) {

	args := []string{
		"replica", types.GetReplicaMountedDataPath(dataPath),
		"--size", strconv.FormatInt(r.Spec.VolumeSize, 10),
	}
	if backingImagePath != "" {
		args = append(args, "--backing-file", backingImagePath)
	}
	if r.Spec.RevisionCounterDisabled {
		args = append(args, "--disableRevCounter")
	}
	if engineCLIAPIVersion >= 7 {
		args = append(args, "--volume-name", r.Spec.VolumeName)

		if dataLocality == longhorn.DataLocalityStrictLocal {
			args = append(args, "--data-server-protocol", "unix")
		}

		if r.Spec.UnmapMarkDiskChainRemovedEnabled {
			args = append(args, "--unmap-mark-disk-chain-removed")
		}
	}

	binary := filepath.Join(types.GetEngineBinaryDirectoryForReplicaManagerContainer(r.Spec.EngineImage), types.EngineBinaryName)

	return binary, args
}

// EngineInstanceCreate creates a new engine instance
func (c *InstanceManagerClient) EngineInstanceCreate(e *longhorn.Engine,
	volumeFrontend longhorn.VolumeFrontend, engineReplicaTimeout, replicaFileSyncHTTPClientTimeout int64,
	dataLocality longhorn.DataLocality, imIP string, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	binary := ""
	args := []string{}
	frontend := ""
	replicaAddresses := map[string]string{}

	var err error

	switch e.Spec.BackendStoreDriver {
	case longhorn.BackendStoreDriverTypeLonghorn:
		binary, args, err = getBinaryAndArgsForEngineProcessCreation(e, volumeFrontend, engineReplicaTimeout, replicaFileSyncHTTPClientTimeout, dataLocality, engineCLIAPIVersion)
	case longhorn.BackendStoreDriverTypeSpdkAio:
		replicaAddresses = e.Status.CurrentReplicaAddressMap
		frontend, err = GetEngineProcessFrontend(volumeFrontend)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of creating engine process */
		process, err := c.processManagerGrpcClient.ProcessCreate(e.Name, binary, DefaultEnginePortCount, args, []string{DefaultPortArg})
		if err != nil {
			return nil, err
		}
		return c.parseProcess(imapi.RPCToProcess(process)), nil
	}

	instance, err := c.instanceServiceGrpcClient.InstanceCreate(e.Name, e.Spec.VolumeName,
		string(longhorn.InstanceManagerTypeEngine), string(e.Spec.BackendStoreDriver), "", uint64(e.Spec.VolumeSize),
		binary, args, frontend, imIP, replicaAddresses, DefaultEnginePortCount, []string{DefaultPortArg}, true)
	if err != nil {
		return nil, err
	}
	return parseInstance(instance), nil
}

// ReplicaInstanceCreate creates a new replica instance
func (c *InstanceManagerClient) ReplicaInstanceCreate(r *longhorn.Replica,
	dataPath, backingImagePath string, dataLocality longhorn.DataLocality, exposeRequired bool, imIP string, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	binary := ""
	args := []string{}
	if r.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeLonghorn {
		binary, args = getBinaryAndArgsForReplicaProcessCreation(r, dataPath, backingImagePath, dataLocality, engineCLIAPIVersion)
	}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of creating replica process */
		process, err := c.processManagerGrpcClient.ProcessCreate(r.Name, binary, DefaultReplicaPortCount, args, []string{DefaultPortArg})
		if err != nil {
			return nil, err
		}
		return c.parseProcess(imapi.RPCToProcess(process)), nil
	}

	instance, err := c.instanceServiceGrpcClient.InstanceCreate(r.Name, r.Spec.VolumeName,
		string(longhorn.InstanceManagerTypeReplica), string(r.Spec.BackendStoreDriver), r.Spec.DiskID, uint64(r.Spec.VolumeSize),
		binary, args, "", imIP, nil, DefaultReplicaPortCount, []string{DefaultPortArg}, exposeRequired)
	if err != nil {
		return nil, err
	}
	return parseInstance(instance), nil
}

// InstanceDelete deletes the instance
func (c *InstanceManagerClient) InstanceDelete(name, kind string, backendStoreDriver longhorn.BackendStoreDriverType, diskUUID string) error {
	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of deleting process */
		_, err := c.processManagerGrpcClient.ProcessDelete(name)
		return err
	}

	_, err := c.instanceServiceGrpcClient.InstanceDelete(name, kind, string(backendStoreDriver), diskUUID, false)
	return err
}

// InstanceGet returns the instance process
func (c *InstanceManagerClient) InstanceGet(name, kind string, backendStoreDriver longhorn.BackendStoreDriverType, diskUUID string) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of getting process */
		process, err := c.processManagerGrpcClient.ProcessGet(name)
		if err != nil {
			return nil, err
		}
		return c.parseProcess(imapi.RPCToProcess(process)), nil
	}

	instance, err := c.instanceServiceGrpcClient.InstanceGet(name, kind, string(backendStoreDriver), diskUUID)
	if err != nil {
		return nil, err
	}
	return parseInstance(instance), nil
}

// InstanceGetBinary returns the binary name of the instance
func (c *InstanceManagerClient) InstanceGetBinary(name, kind string, backendStoreDriver longhorn.BackendStoreDriverType, diskUUID string) (string, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return "", err
	}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of getting binary name */
		process, err := c.processManagerGrpcClient.ProcessGet(name)
		if err != nil {
			return "", err
		}
		return imapi.RPCToProcess(process).Binary, nil
	}

	instance, err := c.instanceServiceGrpcClient.InstanceGet(name, kind, string(backendStoreDriver), diskUUID)
	if err != nil {
		return "", err
	}
	return instance.Binary, nil
}

// InstanceLog returns a grpc stream that will be closed when the passed context is cancelled or the underlying grpc client is closed
func (c *InstanceManagerClient) InstanceLog(ctx context.Context, name, kind string, backendStoreDriver longhorn.BackendStoreDriverType) (*imapi.LogStream, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of logging process */
		return c.processManagerGrpcClient.ProcessLog(ctx, name)
	}

	return c.instanceServiceGrpcClient.InstanceLog(ctx, name, kind, string(backendStoreDriver))
}

// InstanceWatch returns a grpc stream that will be closed when the passed context is cancelled or the underlying grpc client is closed
func (c *InstanceManagerClient) InstanceWatch(ctx context.Context) (interface{}, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of creating replica process */
		return c.processManagerGrpcClient.ProcessWatch(ctx)
	}

	return c.instanceServiceGrpcClient.InstanceWatch(ctx)
}

// InstanceList returns a map of instance name to instance process
func (c *InstanceManagerClient) InstanceList() (map[string]longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	result := map[string]longhorn.InstanceProcess{}

	if c.GetAPIVersion() < 4 {
		/* Fall back to the old way of listing processes */
		processes, err := c.processManagerGrpcClient.ProcessList()
		if err != nil {
			return nil, err
		}
		result := map[string]longhorn.InstanceProcess{}
		for name, process := range processes {
			result[name] = *c.parseProcess(imapi.RPCToProcess(process))
		}
		return result, nil
	}

	instances, err := c.instanceServiceGrpcClient.InstanceList()
	if err != nil {
		return nil, err
	}
	for name, instance := range instances {
		result[name] = *parseInstance(instance)
	}

	return result, nil
}

// EngineProcessUpgrade upgrades the engine process
func (c *InstanceManagerClient) EngineProcessUpgrade(e *longhorn.Engine, volumeFrontend longhorn.VolumeFrontend,
	engineReplicaTimeout, replicaFileSyncHTTPClientTimeout int64, dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	frontend, err := GetEngineProcessFrontend(volumeFrontend)
	if err != nil {
		return nil, err
	}
	args := []string{"controller", e.Spec.VolumeName, "--frontend", frontend, "--upgrade"}
	for _, addr := range e.Spec.UpgradedReplicaAddressMap {
		args = append(args, "--replica", GetBackendReplicaURL(addr))
	}

	if engineCLIAPIVersion >= 6 {
		args = append(args,
			"--size", strconv.FormatInt(e.Spec.VolumeSize, 10),
			"--current-size", strconv.FormatInt(e.Status.CurrentSize, 10))
	}

	if engineCLIAPIVersion >= 7 {
		args = append(args,
			"--engine-replica-timeout", strconv.FormatInt(engineReplicaTimeout, 10),
			"--file-sync-http-client-timeout", strconv.FormatInt(replicaFileSyncHTTPClientTimeout, 10))

		if dataLocality == longhorn.DataLocalityStrictLocal {
			args = append(args,
				"--data-server-protocol", "unix")
		}

		if e.Spec.UnmapMarkSnapChainRemovedEnabled {
			args = append(args, "--unmap-mark-snap-chain-removed")
		}
	}

	binary := filepath.Join(types.GetEngineBinaryDirectoryForEngineManagerContainer(e.Spec.EngineImage), types.EngineBinaryName)

	instance, err := c.instanceServiceGrpcClient.InstanceReplace(e.Name,
		string(longhorn.InstanceManagerTypeEngine), string(e.Spec.BackendStoreDriver), binary, DefaultEnginePortCount, args, []string{DefaultPortArg}, DefaultTerminateSignal)
	if err != nil {
		return nil, err
	}
	return parseInstance(instance), nil
}

// VersionGet returns the version of the instance manager
func (c *InstanceManagerClient) VersionGet() (int, int, int, int, error) {
	output, err := c.instanceServiceGrpcClient.VersionGet()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return output.InstanceManagerAPIMinVersion, output.InstanceManagerAPIVersion,
		output.InstanceManagerProxyAPIMinVersion, output.InstanceManagerProxyAPIVersion, nil
}
