import gc
import json
import socket
import threading
import weakref

import pytest


TIMEOUT = 5


@pytest.fixture
def start_thread():
    running = []

    def start(target):
        finished = threading.Event()
        errors = []

        def run():
            try:
                target()
            except BaseException as error:
                errors.append(error)
            finally:
                finished.set()

        thread = threading.Thread(target=run, daemon=True)
        running.append((thread, finished, errors))
        thread.start()
        return finished

    yield start
    for thread, finished, errors in running:
        thread.join(TIMEOUT)
    for thread, finished, errors in running:
        assert finished.is_set(), "background thread did not finish"
        if errors:
            raise errors[0]


def shutdown(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def finish_writer(conn):
    # Also clean up when an assertion fails before close() can submit its abort.
    conn.write_thread_queue.append({'abort': True})
    conn.write_thread_semaphore.release()
    shutdown(conn.socket)
    conn.write_thread.join(TIMEOUT)
    conn.socket.close()


def test_close_waits_for_writer_exit_after_abort(rpc, start_thread):
    processed_abort = threading.Event()
    release_writer = threading.Event()
    writer_exited = threading.Event()

    class PausedWriter(rpc.connection.Connection):
        def write_thread(self):
            try:
                super().write_thread()
                processed_abort.set()
                release_writer.wait()
            finally:
                writer_exited.set()

    sender, peer = socket.socketpair()
    conn = None
    try:
        conn = PausedWriter(sender, handler_factory=rpc.handlers.NullHandler)
        closed = start_thread(conn.close)
        assert processed_abort.wait(TIMEOUT)
        # Real EOF proves close() reached socket shutdown, before its join.
        peer.settimeout(TIMEOUT)
        assert peer.recv(1) == b''
        assert not closed.wait(0.2), "close returned while the writer was still running"
        release_writer.set()
        assert closed.wait(TIMEOUT)
        assert writer_exited.is_set()
        # Keep the Connection and its proxy cycles reachable through these checks.
        assert conn.connection_status == "closed"
        assert sender.fileno() == -1
    finally:
        release_writer.set()
        if conn is not None:
            finish_writer(conn)
        sender.close()
        peer.close()


def test_close_unblocks_writer_on_saturated_socket(rpc, start_thread):
    sending = threading.Event()
    send_finished = threading.Event()
    writer_exited = threading.Event()

    class BlockingWriter(rpc.connection.Connection):
        _maxtimeout = dict(rpc.connection.Connection._maxtimeout, write=None)

        def write_line(self, data):
            sending.set()
            try:
                return super().write_line(data)
            finally:
                send_finished.set()

        def write_thread(self):
            try:
                super().write_thread()
            finally:
                writer_exited.set()

    sender, peer = socket.socketpair()
    conn = None
    try:
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        sender.setblocking(False)
        chunk = b'x' * 65536
        while True:
            try:
                sender.send(chunk)
            except BlockingIOError:
                break
        sender.setblocking(True)
        conn = BlockingWriter(sender, handler_factory=rpc.handlers.NullHandler)
        conn.write("blocked until shutdown")
        assert sending.wait(TIMEOUT)
        assert not send_finished.wait(0.2)
        # The peer never reads. Joining before shutdown would hang here.
        closed = start_thread(conn.close)
        assert closed.wait(TIMEOUT), "close did not unblock the saturated socket"
        assert send_finished.is_set()
        assert writer_exited.is_set()
        assert conn.connection_status == "closed"
    finally:
        if conn is not None:
            finish_writer(conn)
        sender.close()
        peer.close()


def test_server_releases_closed_connection_before_stopping(rpc, start_thread, monkeypatch):
    abort = threading.Event()
    accepted = [threading.Event(), threading.Event()]
    first_closed = threading.Event()
    references = []
    writers = []

    class Monitor:
        def abortRequested(self):
            return abort.is_set()

    class TrackedConnection(rpc.connection.Connection):
        def __init__(self, *args, **kwargs):
            self.test_index = len(references)
            super().__init__(*args, **kwargs)
            references.append(weakref.ref(self))
            writers.append(self.write_thread)
            accepted[self.test_index].set()

        def close(self):
            super().close()
            if self.test_index == 0:
                first_closed.set()

    class EchoHandler(rpc.handlers.BaseHandler):
        def echo(self, value):
            return value

    monkeypatch.setattr(rpc.server.xbmc, "Monitor", Monitor, raising=False)
    monkeypatch.setattr(rpc.server, "Connection", TrackedConnection)
    server = rpc.createserver(port=0, handler_factory=EchoHandler)
    clients = []
    serving_finished = None
    try:
        serving_finished = start_thread(server.serve)
        address = server.socket.getsockname()
        for connected in accepted:
            clients.append(socket.create_connection(address, timeout=TIMEOUT))
            assert connected.wait(TIMEOUT)
        # Both connections exist before EOF, so descriptor reuse cannot hide a leak.
        clients[0].close()
        assert first_closed.wait(TIMEOUT)
        clients[1].sendall(b'{"id":1,"method":"echo","params":["ready"]}\n')
        with clients[1].makefile("rb") as response:
            assert json.loads(response.readline()) == {'id': 1, 'result': 'ready', 'error': None}
        # Serving the second client also replaces serve()'s local `conn` reference.
        gc.collect()
        assert references[0]() is None, "the running server retained its closed client"
        assert references[1]() is not None
        assert not serving_finished.is_set()
    finally:
        abort.set()
        for client in clients:
            client.close()
        if serving_finished is not None:
            serving_finished.wait(TIMEOUT)
        for writer in writers:
            writer.join(TIMEOUT)
        server.socket.close()
