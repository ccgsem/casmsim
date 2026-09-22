from threading import Event
from unittest.mock import Mock

from casmsim.grpc_runner import SimulatorControlServicer
from casmsim.observation_broker import ObservationBroker
from casmsim.proto import casm_runner_pb2 as pb


def test_cancel_waits_for_worker_flush_and_is_idempotent():
    entered, release = Event(), Event()
    hook = Mock()
    def worker(run_id, config):
        entered.set()
        assert release.wait(5)
    broker = ObservationBroker()
    server = SimulatorControlServicer(broker, worker, hook)
    context = Mock()
    server.Start(pb.StartRequest(run_id="r", config_json=b"{}"), context)
    try:
        assert entered.wait(5)
        request = pb.CancelRequest(run_id="r")
        assert server.Cancel(request, context).acknowledged
        assert server.Cancel(request, context).acknowledged
        hook.assert_called_once()
        assert server.GetState(pb.GetStateRequest(run_id="r"), context).state == pb.RUN_STATE_RUNNING
        assert not broker.closed
    finally:
        release.set()
        server._worker.join(5)
    assert server.GetState(pb.GetStateRequest(run_id="r"), context).state == pb.RUN_STATE_CANCELLED
    assert broker.closed
    assert not server.Cancel(request, context).acknowledged
