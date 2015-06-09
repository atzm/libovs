# -*- coding: utf-8 -*-

import os
import sys
import contextlib

__all__ = ['vsctl']


@contextlib.contextmanager
def syspath(path):
    try:
        sys.path.insert(0, path)
        yield
    finally:
        sys.path.remove(path)


with syspath(os.environ.get('LIBOVS_PATH', '/usr/share/openvswitch/python')):
    import ovs.dirs
    import ovs.poller
    import ovs.db.idl


class vsctl(object):
    TXN_UNCOMMITTED = ovs.db.idl.Transaction.UNCOMMITTED
    TXN_UNCHANGED = ovs.db.idl.Transaction.UNCHANGED
    TXN_INCOMPLETE = ovs.db.idl.Transaction.INCOMPLETE
    TXN_ABORTED = ovs.db.idl.Transaction.ABORTED
    TXN_SUCCESS = ovs.db.idl.Transaction.SUCCESS
    TXN_TRY_AGAIN = ovs.db.idl.Transaction.TRY_AGAIN
    TXN_NOT_LOCKED = ovs.db.idl.Transaction.NOT_LOCKED
    TXN_ERROR = ovs.db.idl.Transaction.ERROR

    def __init__(self, remote='unix:%s/db.sock' % ovs.dirs.RUNDIR,
                 schema='%s/vswitch.ovsschema' % ovs.dirs.PKGDATADIR):
        schema = ovs.db.idl.SchemaHelper(location=schema)
        schema.register_all()
        self.idl = ovs.db.idl.Idl(remote, schema)
        self.seqno = self.change_seqno
        self.txn_status = self.TXN_UNCHANGED
        self.blockwait(self.seqno)

    @staticmethod
    def _addlist(row, column, value):
        setattr(row, column, getattr(row, column, []) + [value])

    @staticmethod
    def _dellist(row, column, value):
        v = [v for v in getattr(row, column, []) if v.uuid != value.uuid]
        setattr(row, column, v)

    def _find(self, table, func):
        for row in self.idl.tables[table].rows.itervalues():
            if func(row):
                return row
        return None

    def _find_by_name(self, table, name):
        return self._find(table, lambda r: r.name == name)

    def _insert(self, txn, table):
        return txn.insert(self.idl.tables[table])

    @property
    def ovs(self):
        return self.idl.tables['Open_vSwitch'].rows.itervalues().next()

    @property
    def change_seqno(self):
        return self.idl.change_seqno

    @contextlib.contextmanager
    def transaction(self):
        self.txn_status = self.TXN_UNCHANGED
        txn = ovs.db.idl.Transaction(self.idl)

        try:
            yield txn
            self.txn_status = txn.commit_block()
        except:
            txn.abort()
            self.txn_status = self.TXN_ABORTED
            raise

    def blockwait(self, seqno):
        while seqno == self.change_seqno and not self.idl.run():
            poller = ovs.poller.Poller()
            self.idl.wait(poller)
            poller.block()

    def __call__(self, func):
        while True:
            self.blockwait(self.seqno)
            self.seqno = self.change_seqno

            with self.transaction() as txn:
                func(self, txn)

            if self.txn_status != self.TXN_TRY_AGAIN:
                break

        return self.txn_status

    def add_br(self, txn, brname, **kwargs):
        if self._find_by_name('Bridge', brname):
            raise ValueError('bridge %s already exists' % brname)

        iface = self.add_iface(txn, brname, type='internal')
        port = self._insert(txn, 'Port')
        port.name = brname
        self._addlist(port, 'interfaces', iface)

        bridge = self._insert(txn, 'Bridge')
        bridge.name = brname
        self._addlist(bridge, 'ports', port)

        for k, v in kwargs.iteritems():
            setattr(bridge, k, v)

        self.ovs.verify('bridges')
        self._addlist(self.ovs, 'bridges', bridge)

        return bridge

    def add_port(self, txn, brname, ptname, **kwargs):
        bridge = self._find_by_name('Bridge', brname)
        if not bridge:
            raise ValueError('bridge %s does not exist' % brname)

        if self._find_by_name('Port', ptname):
            raise ValueError('port %s already exists' % ptname)

        iface = self.add_iface(txn, ptname)
        port = self._insert(txn, 'Port')
        port.name = ptname
        self._addlist(port, 'interfaces', iface)

        for k, v in kwargs.iteritems():
            setattr(port, k, v)

        bridge.verify('ports')
        self._addlist(bridge, 'ports', port)

        return port

    def add_iface(self, txn, ifname, **kwargs):
        if self._find_by_name('Interface', ifname):
            raise ValueError('interface %s already exists' % ifname)

        iface = self._insert(txn, 'Interface')
        iface.name = ifname

        for k, v in kwargs.iteritems():
            setattr(iface, k, v)

        return iface

    def del_br(self, txn, brname):
        bridge = self._find_by_name('Bridge', brname)
        if not bridge:
            raise ValueError('bridge %s does not exist' % brname)

        self.del_port(txn, brname, brname)

        self.ovs.verify('bridges')
        self._dellist(self.ovs, 'bridges', bridge)
        bridge.delete()

        return bridge

    def del_port(self, txn, brname, ptname):
        bridge = self._find_by_name('Bridge', brname)
        if not bridge:
            raise ValueError('bridge %s does not exist' % brname)

        port = self._find_by_name('Port', ptname)
        if not port:
            raise ValueError('port %s does not exist' % ptname)

        self.del_iface(txn, ptname)

        bridge.verify('ports')
        self._dellist(bridge, 'ports', port)
        port.delete()

        return port

    def del_iface(self, txn, ifname):
        port = self._find_by_name('Port', ifname)
        if not port:
            raise ValueError('port %s does not exist' % ifname)

        iface = self._find_by_name('Interface', ifname)
        if not iface:
            raise ValueError('interface %s does not exist' % ifname)

        port.verify('interfaces')
        self._dellist(port, 'interfaces', iface)
        iface.delete()

        return iface

    def br_exists(self, brname):
        return bool(self._find_by_name('Bridge', brname))

    def list_br(self):
        return [str(r.name)
                for r in self.idl.tables['Bridge'].rows.itervalues()]

    def list_ports(self, brname):
        bridge = self._find_by_name('Bridge', brname)
        if not bridge:
            raise ValueError('bridge %s does not exist' % brname)

        return [str(p.name) for p in bridge.ports if p.name != brname]

    def list_ifaces(self, brname):
        bridge = self._find_by_name('Bridge', brname)
        if not bridge:
            raise ValueError('bridge %s does not exist' % brname)

        ifnames = set()
        for p in bridge.ports:
            if p.name != brname:
                ifnames |= set(str(i.name) for i in p.interfaces)

        return list(ifnames)
