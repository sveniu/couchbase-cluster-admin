from sshtunnel import SSHTunnelForwarder
import paramiko


DEFAULT_LOCAL_BIND_HOST = "127.0.0.1"
DEFAULT_REMOTE_BIND_HOST = "127.0.0.1"


class SshTunnel:
    """
    Lightweight wrapper around SSHTunnelForwarder

    Allows couchbase-cluster-admin to connect to hosts that don't directly expose
    their Couchbase REST API, but can be reached via ssh.
    """

    def __init__(self, ssh_username, remote_host, remote_port, local_host=None, local_port=None):
        self.ssh_username = ssh_username

        self.remote_bind_address = (DEFAULT_REMOTE_BIND_HOST, remote_port)

        if not local_host and not local_port:
            # A random port on localhost will be assigned
            self.local_bind_address = (DEFAULT_LOCAL_BIND_HOST,)
        else:
            self.local_bind_address = (local_host, local_port)

        self.server = SSHTunnelForwarder(
            remote_host,
            ssh_username=self.ssh_username,
            # https://stackoverflow.com/questions/54213831/paramiko-or-sshtunnel-and-ssh-agent-without-entering-passphrase
            ssh_pkey=paramiko.agent.Agent().get_keys(),
            remote_bind_address=self.remote_bind_address,
            local_bind_address=self.local_bind_address,
        )

    def start(self):
        self.server.start()

    @property
    def local_host(self):
        return self.server.local_bind_host if self.server else None

    @property
    def local_port(self):
        return self.server.local_bind_port if self.server else None

    def stop(self):
        if self.server:
            self.server.stop()

    def __del__(self):
        self.stop()
