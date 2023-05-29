package api

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
}

type ReplicaInstance struct {
	Name       string
	UUID       string
	DiskName   string
	DiskUUID   string
	SpecSize   uint64
	ActualSize uint64
}
