package linear

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	nslib "github.com/longhorn/go-common-libs/ns"
	typeslib "github.com/longhorn/go-common-libs/types"
)

const (
	mapperFilePathPrefix = "/dev/mapper"

	executeTimeout = time.Minute
)

func VolumeMapper(volume string) string {
	return path.Join(mapperFilePathPrefix, volume)
}

func OpenVolume(volume, devicePath string) error {
	logrus.Infof("Opening device %s with linear dm device on %s", devicePath, volume)

	namespaces := []typeslib.Namespace{typeslib.NamespaceMnt, typeslib.NamespaceIpc}
	nsexec, err := nslib.NewNamespaceExecutor(typeslib.ProcessSelf, typeslib.ProcDirectory, namespaces)
	if err != nil {
		return errors.Wrap(err, "failed to create namespace executor for creating linear dm device")
	}

	sectors, err := getDeviceSectorSize(devicePath, nsexec)
	if err != nil {
		return err
	}

	table := fmt.Sprintf("0 %v linear %v 0", sectors, devicePath)

	logrus.Infof("Creating linear dm device %s with table %s", volume, table)
	_, err = nsexec.Execute("dmsetup", []string{"create", volume, "--table", table}, executeTimeout)
	if err != nil {
		return errors.Wrapf(err, "failed to create linear dm device %s with table %s", volume, table)
	}

	return nil
}

func CloseVolume(volume string) error {
	logrus.Infof("Removing linear dm device %s", volume)

	namespaces := []typeslib.Namespace{typeslib.NamespaceMnt, typeslib.NamespaceIpc}
	nsexec, err := nslib.NewNamespaceExecutor(typeslib.ProcessSelf, typeslib.ProcDirectory, namespaces)
	if err != nil {
		return errors.Wrap(err, "failed to create namespace executor for creating linear dm device")
	}

	_, err = nsexec.Execute("dmsetup", []string{"remove", volume}, executeTimeout)
	if err != nil {
		return errors.Wrapf(err, "failed to remove linear dm device %s", volume)
	}

	return nil
}

func getDeviceSectorSize(devPath string, executor *nslib.Executor) (int64, error) {
	opts := []string{
		"--getsize", devPath,
	}

	output, err := executor.Execute("blockdev", opts, executeTimeout)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(strings.TrimSpace(output), 10, 64)
}
