import docker as libdocker
import pytest
import time
import http
import socket
import uuid
import os
import json
import logging
from urllib.parse import urlparse


log = logging.getLogger('tests')

IMAGE = 'dyno-raft-test'


def pytest_addoption(parser):
    parser.addoption('--coverage', action='store_true', default=False)


@pytest.fixture(scope='session')
def docker(request):
    return libdocker.Client(version='auto')


def start_container(docker, container_id):
    docker.start(container=container_id)
    resp = docker.inspect_container(container=container_id)
    return resp["NetworkSettings"]["IPAddress"]


def container_logs(docker, container, srv_name):
    c_logs = docker.logs(
        container=container['Id'], stdout=True, stderr=True, follow=False)
    log.info("========== captured {} service log =========".format(srv_name))
    for msg in c_logs.decode().splitlines():
        log.info(msg)
    log.info("============================================")


@pytest.fixture(scope='session')
def unused_port():
    def factory():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return factory


@pytest.fixture(scope='session')
def session_id():
    return str(uuid.uuid4())


@pytest.yield_fixture(scope='session')
def dyno_raft_image(request, docker, session_id):
    print("* building dyno-raft image ...")
    dfpath = os.path.join(os.path.dirname(__file__), "../")
    build_resp = [line for line in docker.build(
        path=dfpath, rm=True, tag=IMAGE)]
    if b"Successfully built" not in build_resp[-1]:
        for line in build_resp:
            data = json.loads(line.decode('utf8'))
            data = data.get('stream', data)
            print(data, end='')
        raise RuntimeError("docker build error!")


class DynoRaftNode:
    def __init__(self, name, quorum=1, join_addr=None):
        self.name = name
        self.quorum = quorum
        self.join_addr = join_addr
        self._container = None
        self.ip = None

    def start(self):
        docker = libdocker.Client(version='auto')
        command = '-name {} -quorum {}'.format(self.name, self.quorum)
        if self.join_addr is not None:
            command += " -join {}:11000".format(self.join_addr)

        self._container = docker.create_container(
            image=IMAGE,
            name='dyno-raft-tests-{}'.format(session_id()),
            command=command
        )
        self.ip = start_container(docker, self._container['Id'])

    def wait_consensus(self, peers=0):
        for i in range(20):
            try:
                con = http.client.HTTPConnection('{}:11000'.format(self.ip))
                con.request("GET", "/raft-stats")
                resp = con.getresponse()
                data = resp.read()
                if resp.status == 200:
                    d = json.loads(data.decode())
                    if int(d["num_peers"]) == peers and int(d["term"]) > 0:
                        return
            except ConnectionError:
                pass

            time.sleep(0.5)

    def set_val(self, key, val):
        return self.__set_val(self.ip, key, val)

    def __set_val(self, ip, key, val):
        con = http.client.HTTPConnection('{}:11000'.format(ip))
        con.request("POST", "/key", json.dumps({key: val}))
        resp = con.getresponse()
        if resp.status == 301:
            ip = urlparse(resp.headers["Location"]).hostname
            return self.__set_val(ip, key, val)
        if resp.status != 200:
            raise RuntimeError("Set kv error: {}".format(resp.read().decode()))

    def get_val(self, key):
        return self.__get_val(self.ip, key)

    def __get_val(self, ip, key):
        con = http.client.HTTPConnection('{}:11000'.format(ip))
        con.request("GET", "/key/{}".format(key))
        resp = con.getresponse()
        if resp.status == 301:
            ip = urlparse(resp.headers["Location"]).hostname
            return self.__get_val(ip, key)
        data = resp.read().decode()
        assert resp.status == 200, data
        return json.loads(data)[key]

    def pause(self):
        docker = libdocker.Client(version='auto')
        docker.kill(container=self._container['Id'], signal='SIGTERM')
        docker.wait(container=self._container['Id'], timeout=10)

    def unpause(self):
        docker = libdocker.Client(version='auto')
        self.ip = start_container(docker, self._container['Id'])

    def stop(self):
        docker = libdocker.Client(version='auto')
        docker.kill(container=self._container['Id'], signal='SIGTERM')
        docker.wait(container=self._container['Id'], timeout=10)
        container_logs(docker, self._container, 'dyno-raft')
        docker.remove_container(self._container['Id'])


@pytest.fixture(scope='class')
def setup_test_class(request, dyno_raft_image):
    pass
