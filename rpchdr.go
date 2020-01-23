package rpc

type MessageType int
type MessageVersion uint16

const (
	MSG_TYPE_UNKNOWN MessageType = iota
	MSG_TYPE_REQUEST
	MSG_TYPE_RESPONSE
	MSG_TYPE_NOTIFY
)

const (
	DEF_MSG_VER_MAJ = 1
	DEF_MSG_VER_MIN = 0
	MSG_MAGIC_CODE  = 0x43606326
)

type RpcHdr struct {
	MsgMagicCode uint32         // MSG_MAGIC_CODE
	MsgVerMajor  MessageVersion // DEF_MSG_VER_MAJ
	MsgVerMinor  MessageVersion // DEF_MSG_VER_MIN
	MsgType      MessageType    // MSG_TYPE_*
}

func NewRpcHeader(t MessageType) *RpcHdr {
	return &RpcHdr{
		MsgMagicCode: MSG_MAGIC_CODE,
		MsgVerMajor:  DEF_MSG_VER_MAJ,
		MsgVerMinor:  DEF_MSG_VER_MIN,
		MsgType:      t,
	}
}
