package smrpc

import (
	fmt "fmt"
	"io/ioutil"
	"time"

	"github.com/google/fscrypt/filesystem"
	"github.com/sirupsen/logrus"
	"k8s.io/mount-utils"

	empty "github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/longhorn-share-manager/pkg/server"
	"github.com/longhorn/longhorn-share-manager/pkg/types"
	"github.com/longhorn/longhorn-share-manager/pkg/util"
	"github.com/longhorn/longhorn-share-manager/pkg/volume"
)

type ShareManagerServer struct {
	manager *server.ShareManager
}

func NewShareManagerServer(manager *server.ShareManager) *ShareManagerServer {
	return &ShareManagerServer{
		manager: manager,
	}
}

func (s *ShareManagerServer) FilesystemMount(ctx context.Context, req *empty.Empty) (resp *empty.Empty, err error) {
	volumeName := s.manager.Volume.Name
	mountPath := types.GetMountPath(volumeName)
	devicePath := types.GetVolumeDevicePath(volumeName, false)

	logrus.Infof("Mounting volume %v on %v", devicePath, mountPath)

	err = s.manager.Run()
	if err != nil {
		logrus.WithError(err).Errorf("failed to mount volume %v on %v", devicePath, mountPath)
	}

	return &empty.Empty{}, err
}

func (s *ShareManagerServer) FilesystemMountStatus(ctx context.Context, req *empty.Empty) (*FilesystemMountStatusResponse, error) {
	state, errMsg := s.manager.GetMountStatus()

	return &FilesystemMountStatusResponse{
		State: string(state),
		Error: errMsg,
	}, nil
}

func (s *ShareManagerServer) FilesystemTrim(ctx context.Context, req *FilesystemTrimRequest) (resp *empty.Empty, err error) {
	volumeName := s.manager.Volume.Name

	defer func() {
		if err != nil {
			logrus.WithError(err).Errorf("failed to trim mounted filesystem on volume %v", volumeName)
		}
	}()

	devicePath := types.GetVolumeDevicePath(volumeName, req.IsEncryptedDevice)
	if !volume.CheckDeviceValid(devicePath) {
		return &empty.Empty{}, fmt.Errorf("volume %v is not valid", volumeName)
	}

	mountPath := types.GetMountPath(volumeName)

	mnt, err := filesystem.GetMount(mountPath)
	if err != nil {
		return &empty.Empty{}, err
	}

	deviceNumber, err := util.GetDeviceNumber(devicePath)
	if err != nil {
		return &empty.Empty{}, err
	}

	if uint64(mnt.DeviceNumber) != uint64(deviceNumber) {
		return &empty.Empty{}, fmt.Errorf("the device of mount point %v is not expected", mountPath)
	}

	logrus.Infof("Trimming mounted filesystem %v for volume %v", mountPath, volumeName)

	mounter := mount.New("")
	notMounted, err := mount.IsNotMountPoint(mounter, mountPath)
	if notMounted {
		return &empty.Empty{}, fmt.Errorf("%v is not a mount point", mountPath)
	}
	if err != nil {
		return &empty.Empty{}, err
	}

	if _, err := ioutil.ReadDir(mountPath); err != nil {
		return &empty.Empty{}, err
	}

	_, err = iscsiutil.Execute("fstrim", []string{mountPath})
	if err != nil {
		return &empty.Empty{}, err
	}

	logrus.Infof("Finished trimming mounted filesystem %v on volume %v", mountPath, volumeName)

	return &empty.Empty{}, nil
}

type ShareManagerHealthCheckServer struct {
	srv *ShareManagerServer
}

func NewShareManagerHealthCheckServer(srv *ShareManagerServer) *ShareManagerHealthCheckServer {
	return &ShareManagerHealthCheckServer{
		srv: srv,
	}
}

func (s *ShareManagerHealthCheckServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if s.srv != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("share manager gRPC server is not running")
}

func (s *ShareManagerHealthCheckServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if s.srv != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for share manager gRPC server",
					healthpb.HealthCheckResponse_SERVING)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for share manager gRPC server",
					healthpb.HealthCheckResponse_NOT_SERVING)
			}

		}
		time.Sleep(time.Second)
	}
}
