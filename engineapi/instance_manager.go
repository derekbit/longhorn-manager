package engineapi

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"unsafe"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	CurrentInstanceManagerAPIVersion = 3
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
	processManagerGrpcClient *imclient.ProcessManagerClient
	diskServiceGrpcClient    *imclient.DiskServiceClient
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

func NewInstanceManagerClient(im *longhorn.InstanceManager) (*InstanceManagerClient, error) {
	// Do not check the major version here. Since IM cannot get the major version without using this client to call VersionGet().
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning || im.Status.IP == "" {
		return nil, fmt.Errorf("invalid Instance Manager %v, state: %v, IP: %v", im.Name, im.Status.CurrentState, im.Status.IP)
	}
	// HACK: TODO: fix me
	endpoint := "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerDefaultPort)

	initTLSClient := func() (*imclient.ProcessManagerClient, error) {
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

	// Create a new process manager client
	processManagerClient, err := initTLSClient()
	if err != nil {
		// fallback to non tls client, there is no way to differentiate between im versions unless we get the version via the im client
		// TODO: remove this im client fallback mechanism in a future version maybe 2.4 / 2.5 or the next time we update the api version
		processManagerClient, err = imclient.NewProcessManagerClient(endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Instance Manager Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}

		if _, err = processManagerClient.VersionGet(); err != nil {
			return nil, fmt.Errorf("failed to get Version of Instance Manager Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}
	}

	// Create a new disk service client
	ctx, cancel := context.WithCancel(context.Background())
	diskServiceClient, err := imclient.NewDiskServiceClient(ctx, cancel, im.Status.IP, InstanceManagerDiskServiceDefaultPort)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Instance Manager Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
			im.Name, im.Status.CurrentState, im.Status.IP, false, err)
	}

	// TODO: consider evaluating im client version since we do the call anyway to validate the connection, i.e. fallback to non tls
	//  This way we don't need the per call compatibility check, ref: `CheckInstanceManagerCompatibility`

	return &InstanceManagerClient{
		ip:                       im.Status.IP,
		apiMinVersion:            im.Status.APIMinVersion,
		apiVersion:               im.Status.APIVersion,
		processManagerGrpcClient: processManagerClient,
		diskServiceGrpcClient:    diskServiceClient,
	}, nil
}

func (c *InstanceManagerClient) parseProcess(p *imapi.Process) *longhorn.InstanceProcess {
	if p == nil {
		return nil
	}

	processType := longhorn.InstanceTypeReplica
	if p.PortCount == DefaultEnginePortCount {
		processType = longhorn.InstanceTypeEngine
	}

	return &longhorn.InstanceProcess{
		Spec: longhorn.InstanceProcessSpec{
			Name:               p.Name,
			BackendStoreDriver: longhorn.BackendStoreDriverTypeLonghorn,
		},
		Status: longhorn.InstanceProcessStatus{
			State:     longhorn.InstanceState(p.ProcessStatus.State),
			ErrorMsg:  p.ProcessStatus.ErrorMsg,
			PortStart: p.ProcessStatus.PortStart,
			PortEnd:   p.ProcessStatus.PortEnd,
			Type:      processType,

			// These fields are not used, maybe we can deprecate them later.
			Listen:   "",
			Endpoint: "",
		},
	}
}

func (c *InstanceManagerClient) parseReplicaInfo(info *ReplicaInfo) *longhorn.InstanceProcess {
	if info == nil {
		return nil
	}

	return &longhorn.InstanceProcess{
		Spec: longhorn.InstanceProcessSpec{
			Name:               info.Name,
			BackendStoreDriver: longhorn.BackendStoreDriverTypeSpdkAio,
		},
		Status: longhorn.InstanceProcessStatus{
			Type:  longhorn.InstanceTypeReplica,
			State: longhorn.InstanceState(info.State),

			ErrorMsg:  "",
			PortStart: 0,
			PortEnd:   0,

			// These fields are not used, maybe we can deprecate them later.
			Listen:   "",
			Endpoint: "",
		},
	}
}

func (c *InstanceManagerClient) EngineInstanceCreate(e *longhorn.Engine,
	volumeFrontend longhorn.VolumeFrontend, engineReplicaTimeout, replicaFileSyncHTTPClientTimeout int64,
	dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	switch e.Spec.BackendStoreDriver {
	case longhorn.BackendStoreDriverTypeLonghorn:
		frontend, err := GetEngineProcessFrontend(volumeFrontend)
		if err != nil {
			return nil, err
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

		engineProcess, err := c.processManagerGrpcClient.ProcessCreate(
			e.Name, binary, DefaultEnginePortCount, args, []string{DefaultPortArg})
		if err != nil {
			return nil, err
		}
		return c.parseProcess(engineProcess), nil
	case longhorn.BackendStoreDriverTypeSpdkAio:
		return nil, fmt.Errorf("SPDK AIO backend store driver is not supported for engine %v", e.Name)
	default:
		return nil, fmt.Errorf("unknown backend store driver %v", e.Spec.BackendStoreDriver)
	}
}

func (c *InstanceManagerClient) ReplicaInstanceCreate(r *longhorn.Replica,
	dataPath, backingImagePath string, dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	switch r.Spec.BackendStoreDriver {
	case longhorn.BackendStoreDriverTypeLonghorn:
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

		replicaProcess, err := c.processManagerGrpcClient.ProcessCreate(
			r.Name, binary, DefaultReplicaPortCount, args, []string{DefaultPortArg})
		if err != nil {
			return nil, err
		}
		return c.parseProcess(replicaProcess), nil
	case longhorn.BackendStoreDriverTypeSpdkAio:
		/* TODO: */
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown backend store driver %v", r.Spec.BackendStoreDriver)
	}
}

// InstanceDelete deletes the instance
func (c *InstanceManagerClient) InstanceDelete(kind string, obj interface{}) error {
	switch kind {
	case types.LonghornKindEngine:
		e, ok := obj.(*longhorn.Engine)
		if !ok {
			return fmt.Errorf("BUG: obj is not an Engine: %v", obj)
		}

		switch e.Spec.BackendStoreDriver {
		case longhorn.BackendStoreDriverTypeLonghorn:
			_, err := c.processManagerGrpcClient.ProcessDelete(e.Name)
			return err
		case longhorn.BackendStoreDriverTypeSpdkAio:
			/* TODO: */
			return nil
		default:
			return fmt.Errorf("unknown backend store driver %v for engine %v", e.Spec.BackendStoreDriver, e.Name)
		}
	case types.LonghornKindReplica:
		r, ok := obj.(*longhorn.Replica)
		if !ok {
			return fmt.Errorf("BUG: obj is not a Replica: %v", obj)
		}

		switch r.Spec.BackendStoreDriver {
		case longhorn.BackendStoreDriverTypeLonghorn:
			_, err := c.processManagerGrpcClient.ProcessDelete(r.Name)
			return err
		case longhorn.BackendStoreDriverTypeSpdkAio:
			/* TODO: */
			return nil
		default:
			return fmt.Errorf("unknown backend store driver %v for replica %v", r.Spec.BackendStoreDriver, r.Name)
		}
	default:
		return fmt.Errorf("unknown kind %v", kind)
	}
}

// InstanceGet returns the instance process
func (c *InstanceManagerClient) InstanceGet(kind string, obj interface{}) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	switch kind {
	case types.LonghornKindEngine:
		e, ok := obj.(*longhorn.Engine)
		if !ok {
			return nil, fmt.Errorf("BUG: obj is not an Engine: %v", obj)
		}
		switch e.Spec.BackendStoreDriver {
		case longhorn.BackendStoreDriverTypeLonghorn:
			process, err := c.processManagerGrpcClient.ProcessGet(e.Name)
			if err != nil {
				return nil, err
			}
			return c.parseProcess(process), nil
		case longhorn.BackendStoreDriverTypeSpdkAio:
			/* TODO: */
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown backend store driver %v for replica %v", e.Spec.BackendStoreDriver, e.Name)
		}
	case types.LonghornKindReplica:
		r, ok := obj.(*longhorn.Replica)
		if !ok {
			return nil, fmt.Errorf("BUG: obj is not a Replica: %v", obj)
		}
		switch r.Spec.BackendStoreDriver {
		case longhorn.BackendStoreDriverTypeLonghorn:
			process, err := c.processManagerGrpcClient.ProcessGet(r.Name)
			if err != nil {
				return nil, err
			}
			return c.parseProcess(process), nil
		case longhorn.BackendStoreDriverTypeSpdkAio:
			/* TODO: */
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown backend store driver %v for replica %v", r.Spec.BackendStoreDriver, r.Name)
		}
	default:
		return nil, fmt.Errorf("unknown kind %v", kind)
	}
}

// InstanceGetBinary returns the binary name of the instance
func (c *InstanceManagerClient) InstanceGetBinary(name string) (string, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return "", err
	}
	process, err := c.processManagerGrpcClient.ProcessGet(name)
	if err != nil {
		return "", err
	}
	return process.Binary, nil
}

// InstanceLog returns a grpc stream that will be closed when the passed context is cancelled or the underlying grpc client is closed
func (c *InstanceManagerClient) InstanceLog(ctx context.Context, kind string, obj interface{}) (*imapi.LogStream, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	switch kind {
	case types.LonghornKindEngine:
		e, ok := obj.(*longhorn.Engine)
		if !ok {
			return nil, fmt.Errorf("BUG: obj is not an Engine: %v", obj)
		}
		switch e.Spec.BackendStoreDriver {
		case longhorn.BackendStoreDriverTypeLonghorn:
			return c.processManagerGrpcClient.ProcessLog(ctx, e.Name)
		case longhorn.BackendStoreDriverTypeSpdkAio:
			/* TODO: */
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown backend store driver %v for engine %v", e.Spec.BackendStoreDriver, e.Name)
		}
	case types.LonghornKindReplica:
		r, ok := obj.(*longhorn.Replica)
		if !ok {
			return nil, fmt.Errorf("BUG: obj is not a Replica: %v", obj)
		}
		switch r.Spec.BackendStoreDriver {
		case longhorn.BackendStoreDriverTypeLonghorn:
			return c.processManagerGrpcClient.ProcessLog(ctx, r.Name)
		case longhorn.BackendStoreDriverTypeSpdkAio:
			/* TODO: */
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown backend store driver %v for replica %v", r.Spec.BackendStoreDriver, r.Name)
		}
	default:
		return nil, fmt.Errorf("unknown kind %v", kind)
	}
}

// InstanceWatch returns a grpc stream that will be closed when the passed context is cancelled or the underlying grpc client is closed
func (c *InstanceManagerClient) InstanceWatch(ctx context.Context) (*imapi.ProcessStream, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	return c.processManagerGrpcClient.ProcessWatch(ctx)
}

// InstanceList returns a map of instance name to instance process
func (c *InstanceManagerClient) InstanceList(imType longhorn.InstanceManagerType) (map[string]longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	result := map[string]longhorn.InstanceProcess{}

	// List all the processes
	processes, err := c.processManagerGrpcClient.ProcessList()
	if err != nil {
		return nil, err
	}
	for name, process := range processes {
		result[name] = *c.parseProcess(process)
	}

	// List all the lvols
	replicas, err := c.diskServiceGrpcClient.ReplicaList()
	if err != nil {
		return nil, err
	}
	for name, replica := range replicas {
		result[name] = *c.parseReplicaInfo((*ReplicaInfo)(unsafe.Pointer(replica)))
	}

	return result, nil
}

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

	engineProcess, err := c.processManagerGrpcClient.ProcessReplace(
		e.Name, binary, DefaultEnginePortCount, args, []string{DefaultPortArg}, DefaultTerminateSignal)
	if err != nil {
		return nil, err
	}
	return c.parseProcess(engineProcess), nil
}

func (c *InstanceManagerClient) VersionGet() (int, int, int, int, error) {
	output, err := c.processManagerGrpcClient.VersionGet()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return output.InstanceManagerAPIMinVersion, output.InstanceManagerAPIVersion,
		output.InstanceManagerProxyAPIMinVersion, output.InstanceManagerProxyAPIVersion, nil
}
