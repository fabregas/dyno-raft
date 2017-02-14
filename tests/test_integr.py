import random
import pytest
from conftest import DynoRaftNode


@pytest.mark.usefixtures('setup_test_class')
class TestIntegration:
    def test_basic(self):
        n0 = DynoRaftNode("sindlenode")
        n0.start()
        n0.wait_consensus()
        try:
            kv = self.set_kv([n0])
            self.get_kv([n0], kv)
        finally:
            n0.stop()

    def test_2nodes(self):
        n0 = DynoRaftNode("node00")
        n0.start()
        n1 = DynoRaftNode("node01", join_addr=n0.ip)
        n1.start()
        try:
            n1.wait_consensus(peers=1)
            kv = self.set_kv([n0, n1])
            self.get_kv([n0, n1], kv)
        finally:
            n0.stop()
            n1.stop()

    def set_kv(self, nodes, cnt=100):
        kv = {}
        for i in range(cnt):
            node = random.choice(nodes)
            k, v = "key#{}".format(i), str(random.random())
            node.set_val(k, v)
            kv[k] = v
        return kv

    def get_kv(self, nodes, kv):
        for k, v in kv.items():
            node = random.choice(nodes)
            val = node.get_val(k)
            assert v == val, "key={}, val={}; received={}".format(k, v, val)

    def test_pers(self):
        n0 = DynoRaftNode("sindlenode")
        n0.start()
        n0.wait_consensus()
        try:
            kv = self.set_kv([n0])
            n0.pause()
            n0.unpause()
            n0.wait_consensus()
            self.get_kv([n0], kv)
        finally:
            n0.stop()
