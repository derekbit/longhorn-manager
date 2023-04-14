package engineapi

import (
	"unsafe"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
)

type DiskInfo struct {
	ID          string
	UUID        string
	Path        string
	Type        string
	TotalSize   int64
	FreeSize    int64
	TotalBlocks int64
	FreeBlocks  int64
	BlockSize   int64
	ClusterSize int64
	Readonly    bool
}

type ReplicaInfo struct {
	Name        string
	UUID        string
	BdevName    string
	LvstoreName string

	TotalSize   int64
	TotalBlocks int64
	BlockSize   int64

	ThinProvision bool

	State string
}

func NewDiskServiceClient(im *longhorn.InstanceManager, logger logrus.FieldLogger, proxyConnCounter util.Counter) (c DiskServiceClient, err error) {
	defer func() {
		err = errors.Wrap(err, "failed to get disk service client")
	}()

	isInstanceManagerRunning := im.Status.CurrentState == longhorn.InstanceManagerStateRunning
	if !isInstanceManagerRunning {
		err = errors.Errorf("%v instance manager is in %v, not running state", im.Name, im.Status.CurrentState)
		return nil, err
	}

	hasIP := im.Status.IP != ""
	if !hasIP {
		err = errors.Errorf("%v instance manager status IP is missing", im.Name)
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := imclient.NewDiskServiceClient(ctx, cancel, im.Status.IP, InstanceManagerDiskServiceDefaultPort)
	if err != nil {
		return nil, err
	}

	proxyConnCounter.IncreaseCount()

	return &DiskService{
		logger:           logger,
		grpcClient:       client,
		proxyConnCounter: proxyConnCounter,
	}, nil
}

type DiskService struct {
	logger     logrus.FieldLogger
	grpcClient *imclient.DiskServiceClient

	proxyConnCounter util.Counter
}

type DiskServiceClient interface {
	DiskCreate(string, string) (*DiskInfo, error)
	DiskInfo(string) (*DiskInfo, error)
	Close()
}

func (s *DiskService) Close() {
	if s.grpcClient == nil {
		s.logger.WithError(errors.New("gRPC client not exist")).Debugf("cannot close disk service client")
		return
	}

	if err := s.grpcClient.Close(); err != nil {
		s.logger.WithError(err).Warn("failed to close disk service client")
	}

	// The only potential returning error from Close() is
	// "grpc: the client connection is closing". This means we should still
	// decrease the connection count.
	s.proxyConnCounter.DecreaseCount()
}

func (s *DiskService) DiskCreate(diskName, diskPath string) (*DiskInfo, error) {
	info, err := s.grpcClient.DiskCreate(diskName, diskPath)
	return (*DiskInfo)(unsafe.Pointer(info)), err
}

func (s *DiskService) DiskInfo(uuid string) (*DiskInfo, error) {
	info, err := s.grpcClient.DiskInfo(uuid)
	return (*DiskInfo)(unsafe.Pointer(info)), err
}

func (s *DiskService) ReplicaCreate(name, lvstoreUUID string, size int64) (*ReplicaInfo, error) {
	info, err := s.grpcClient.ReplicaCreate(name, lvstoreUUID, size)
	return (*ReplicaInfo)(unsafe.Pointer(info)), err
}

func (s *DiskService) ReplicaInfo(name, lvstoreUUID string) (*ReplicaInfo, error) {
	info, err := s.grpcClient.ReplicaInfo(name, lvstoreUUID)
	return (*ReplicaInfo)(unsafe.Pointer(info)), err
}

func (s *DiskService) ReplicaDelete(name, lvstoreUUID string) error {
	return s.grpcClient.ReplicaDelete(name, lvstoreUUID)
}
