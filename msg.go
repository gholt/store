package brimstore

type MsgType uint16

const (
	MSG_TYPE_PING MsgType = iota
	MSG_TYPE_PONG
	MSG_TYPE_PULL_REPLICATION
)
