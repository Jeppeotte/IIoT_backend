import docker

client = docker.from_env()

container = client.containers.run(
    image="devicedata_connector:0.0.1",
    volumes={
        "/home/jeppe/projectfolder": {"bind": "/mounted_dir", "mode": "rw"}
    },
    extra_hosts={"host.docker.internal": "host-gateway"},
    detach=True
)