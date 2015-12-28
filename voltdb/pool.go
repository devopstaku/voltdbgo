package voltdb

import (
	"errors"
)

type Factory func() (*Conn, error)

type ConnPool struct {
	factory      Factory
	idleConns    ring
	idleCapacity int
	maxConns     int
	numConns     int

	acqchan chan acquireConn
	rchan   chan releaseConn
	cchan   chan closeConn

	activeWaits []acquireConn
}

func NewConnPool(factory Factory, idleCapacity, maxConns int) (cp *ConnPool) {
	cp = &ConnPool{
		factory:      factory,
		idleCapacity: idleCapacity,
		maxConns:     maxConns,

		acqchan: make(chan acquireConn),
		rchan:   make(chan releaseConn, 1),
		cchan:   make(chan closeConn, 1),
	}

	go cp.mux()

	return
}

type releaseConn struct {
	c *Conn
}

type acquireConn struct {
	rch chan *Conn
	ech chan error
}

type closeConn struct {
}

func (cp *ConnPool) mux() {
loop:
	for {
		select {
		case acq := <-cp.acqchan:
			cp.acquire(acq)
		case rel := <-cp.rchan:
			if len(cp.activeWaits) != 0 {
				// someone is waiting - give them the resource if we can
				if !rel.c.IsClosed() {
					cp.activeWaits[0].rch <- rel.c
				} else {
					// if we can't, discard the released resource and create a new one
					c, err := cp.factory()
					if err != nil {
						// reflect the smaller number of existant resources
						cp.numConns--
						cp.activeWaits[0].ech <- err
					} else {
						cp.activeWaits[0].rch <- c
					}
				}
				cp.activeWaits = cp.activeWaits[1:]
			} else {
				// if no one is waiting, release it for idling or closing
				cp.release(rel.c)
			}

		case _ = <-cp.cchan:
			break loop
		}
	}
	for !cp.idleConns.Empty() {
		cp.idleConns.Dequeue().Close()
	}
	for _, aw := range cp.activeWaits {
		aw.ech <- errors.New("Resource pool closed")
	}
}

func (cp *ConnPool) acquire(acq acquireConn) {
	for !cp.idleConns.Empty() {
		c := cp.idleConns.Dequeue()
		if !c.IsClosed() {
			acq.rch <- c
			return
		}
		// discard closed resources
		cp.numConns--
	}
	if cp.maxConns != -1 && cp.numConns >= cp.maxConns {
		// we need to wait until something comes back in
		cp.activeWaits = append(cp.activeWaits, acq)
		return
	}

	c, err := cp.factory()
	if err != nil {
		acq.ech <- err
	} else {
		cp.numConns++
		acq.rch <- c
	}

	return
}

func (cp *ConnPool) release(c *Conn) {
	if c == nil || c.IsClosed() {
		// don't put it back in the pool.
		cp.numConns--
		return
	}
	if cp.idleCapacity != -1 && cp.idleConns.Size() == cp.idleCapacity {
		c.Close()
		cp.numConns--
		return
	}

	cp.idleConns.Enqueue(c)
}

// Acquire() will get one of the idle resources, or create a new one.
func (cp *ConnPool) Acquire() (c *Conn, err error) {
	acq := acquireConn{
		rch: make(chan *Conn),
		ech: make(chan error),
	}
	cp.acqchan <- acq

	select {
	case c = <-acq.rch:
	case err = <-acq.ech:
	}

	return
}

// Release() will release a resource for use by others. If the idle queue is
// full, the resource will be closed.
func (cp *ConnPool) Release(c *Conn) {
	rel := releaseConn{
		c: c,
	}
	cp.rchan <- rel
}

// Close() closes all the pools resources.
func (cp *ConnPool) Close() {
	cp.cchan <- closeConn{}
}

// numConns() the number of resources known at this time
func (cp *ConnPool) NumConns() int {
	return cp.numConns
}
