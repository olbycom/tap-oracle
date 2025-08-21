from binascii import hexlify

import paramiko
from sshtunnel import SSH_CONFIG_FILE
from sshtunnel import SSHTunnelForwarder as BaseSSHTunnelForwarder


class SSHTunnelForwarder(BaseSSHTunnelForwarder):
    run_tunnel_auth_interactive_dumb: bool = False

    def __init__(
        self,
        ssh_address_or_host=None,
        ssh_config_file=SSH_CONFIG_FILE,
        ssh_host_key=None,
        ssh_password=None,
        ssh_pkey=None,
        ssh_private_key_password=None,
        ssh_proxy=None,
        ssh_proxy_enabled=True,
        ssh_username=None,
        local_bind_address=None,
        local_bind_addresses=None,
        logger=None,
        mute_exceptions=False,
        remote_bind_address=None,
        remote_bind_addresses=None,
        set_keepalive=5.0,
        threaded=True,  # old version False
        compression=None,
        allow_agent=True,  # look for keys from an SSH agent
        host_pkey_directories=None,  # look for keys in ~/.ssh
        run_tunnel_auth_interactive_dumb=False,
        *args,
        **kwargs  # for backwards compatibility
    ):
        self.run_tunnel_auth_interactive_dumb = run_tunnel_auth_interactive_dumb
        super().__init__(
            ssh_address_or_host,
            ssh_config_file,
            ssh_host_key,
            ssh_password,
            ssh_pkey,
            ssh_private_key_password,
            ssh_proxy,
            ssh_proxy_enabled,
            ssh_username,
            local_bind_address,
            local_bind_addresses,
            logger,
            mute_exceptions,
            remote_bind_address,
            remote_bind_addresses,
            set_keepalive,
            threaded,
            compression,
            allow_agent,
            host_pkey_directories,
            *args,
            **kwargs
        )

    def _connect_to_gateway(self):
        """
        Open connection to SSH gateway
         - First try with all keys loaded from an SSH agent (if allowed)
         - Then with those passed directly or read from ~/.ssh/config
         - As last resort, try with a provided password
        """
        for key in self.ssh_pkeys:
            self.logger.debug("Trying to log in with key: {0}".format(hexlify(key.get_fingerprint())))
            try:
                self._transport = self._get_transport()
                self._transport.connect(hostkey=self.ssh_host_key, username=self.ssh_username, pkey=key)
                if self.run_tunnel_auth_interactive_dumb:
                    self._transport.auth_interactive_dumb(self.ssh_username)
                if self._transport.is_alive:
                    return
            except paramiko.AuthenticationException:
                self.logger.debug("Authentication error")
                self._stop_transport()

        if self.ssh_password:  # avoid conflict using both pass and pkey
            self.logger.debug("Trying to log in with password: {0}".format("*" * len(self.ssh_password)))
            try:
                self._transport = self._get_transport()
                self._transport.connect(
                    hostkey=self.ssh_host_key, username=self.ssh_username, password=self.ssh_password
                )
                if self._transport.is_alive:
                    return
            except paramiko.AuthenticationException:
                self.logger.debug("Authentication error")
                self._stop_transport()

        self.logger.error("Could not open connection to gateway")
