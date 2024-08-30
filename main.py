import requests
import subprocess
import sys
import os
import time
from opentelemetry import trace

from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


old_image = "quickwit/quickwit:qw-matterlabs-20240709-2"
new_image = "quickwit/quickwit:edge"
container_name = "qwregression"
cwd = os.getcwd()
current_time = time.strftime("%Y-%m-%d--%H-%M-%S")
os.makedirs(current_time, exist_ok=True)

# configure otlp programatically
resource = Resource(attributes={SERVICE_NAME: "regtests"})
traceProvider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="localhost:7281", insecure=True)
processor = BatchSpanProcessor(exporter)
traceProvider.add_span_processor(processor)
trace.set_tracer_provider(traceProvider)


def wait_healthcheck():
    for _ in range(100):
        try:
            print("Checking on quickwit")
            res = requests.get("http://localhost:7280/health/readyz")
            if res.status_code == 200 and res.text.strip() == "true":
                print("Quickwit started")
                time.sleep(6)
                break
        except:
            pass
        print("Server not ready yet. Sleep and retry...")
        time.sleep(1)
    else:
        print("Quickwit never started. Exiting.")
        sys.exit(2)


def run_qw(image: str, log_file: str) -> subprocess.Popen:
    print(f"Run quickwit {image}")
    log_file = open(f"{current_time}/{log_file}", "w")
    proc = subprocess.Popen(
        [
            "docker",
            "run",
            "--name",
            container_name,
            "-e",
            "NO_COLOR=1",
            "-e",
            "QW_ENABLE_OTLP_ENDPOINT=true",
            "-p",
            "7280:7280",
            "-p",
            "7281:7281",
            "-v",
            f"{cwd}/{current_time}/qwdata:/quickwit/qwdata",
            image,
            "run",
        ],
        stdout=log_file,
    )
    wait_healthcheck()
    return proc


def ingest_trace(operation_name: str):
    print("Ingest traces")
    tracer = trace.get_tracer(__name__)
    span = tracer.start_span(operation_name)
    span.add_event("hello")
    span.end()
    processor.force_flush()
    print("Wait 20s for traces to be indexed")
    time.sleep(20)


def list_traces(index: str):
    print("List traces")
    resp = requests.get(f"http://localhost:7280/api/v1/{index}/jaeger/api/traces")
    print(resp.status_code)
    print(resp.json())


def shutdown_qw(proc: subprocess.Popen):
    print("Shutting down quickwit")
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except:
        print("Quickwit did not shutdown in time, killing the container")
    subprocess.run(["docker", "rm", "-f", container_name])


qw_proc = run_qw(old_image, "old_image_run_1.log")
try:
    ingest_trace("oldrun1")
    list_traces("otel-traces-v0_7")
finally:
    shutdown_qw(qw_proc)


qw_proc = run_qw(new_image, "new_image_run.log")
try:
    ingest_trace("newrun1")
    list_traces("otel-traces-v0_7")
finally:
    shutdown_qw(qw_proc)

qw_proc = run_qw(old_image, "old_image_run_2.log")
try:
    ingest_trace("oldrun2")
    list_traces("otel-traces-v0_7")
finally:
    shutdown_qw(qw_proc)
