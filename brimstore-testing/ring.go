package main

type ring struct {
	nodeID uint64
}

func (r *ring) ID() uint64 {
	return 1
}

func (r *ring) PartitionPower() uint16 {
	return 8
}

func (r *ring) NodeID() uint64 {
	return r.nodeID
}

func (r *ring) Responsible(partition uint32) bool {
	return true
}
