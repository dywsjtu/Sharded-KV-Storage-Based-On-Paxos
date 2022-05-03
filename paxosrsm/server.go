package paxosrsm

import (
	"umich.edu/eecs491/proj5/paxos"
)

type PaxosRSM struct {
	me      int
	px      *paxos.Paxos
	applyOp func(interface{})
	equals  func(interface{}, interface{}) bool
	impl    PaxosRSMImpl
}

func (rsm *PaxosRSM) Kill() {
	rsm.px.Kill()
}

//
// applyOp(v) is a callback which the RSM invokes to let the application
// know that it can apply v (a value decided for some Paxos instance) to
// its state
// equals(v1, v2) helps the RSM compare two values and determine if they are
// identical
//
func MakeRSM(me int, px *paxos.Paxos, applyOp func(interface{}), equals func(interface{}, interface{}) bool) *PaxosRSM {
	rsm := new(PaxosRSM)

	rsm.me = me
	rsm.px = px
	rsm.applyOp = applyOp
	rsm.equals = equals

	rsm.InitRSMImpl()

	return rsm
}
