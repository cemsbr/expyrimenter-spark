from shlex import quote
from expyrimenter.core import Executor, SSH


class Spark:
    def __init__(self, home, master, slaves=None, executor=None):
        if slaves is None:
            slaves = []
        if executor is None:
            executor = Executor()

        self._home = home
        self._master = master
        self.executor = executor
        self.logger_name = 'spark'
        self._slaves = slaves

    def set_slaves(self, hosts):
        self._slaves = hosts
        if hosts:
            self._write_slaves_file()

    def start(self):
        cmd = self._home + '/sbin/start-all.sh'
        self._ssh_master(cmd, 'start')

    def stop(self):
        cmd = self._home + '/sbin/stop-all.sh'
        self._ssh_master(cmd, 'stop')

    def start_history_server(self):
        self._history_server('start')

    def stop_history_server(self):
        self._history_server('stop')

    def submit(self, filename, file_stdout, file_stderr):
        cmd = '{}/bin/spark-submit {} 1>{} 2>{}'.format(self._home,
                                                        quote(filename),
                                                        quote(file_stdout),
                                                        quote(file_stderr))
        title = 'app ' + filename
        ssh = SSH(self._master, cmd,
                  title=title,
                  stdout=True,
                  logger_name=self.logger_name)
        self.executor.run(ssh)

    def clean_tmp(self):
        """Remove temp files."""
        self._ssh_hosts('rm -rf /tmp/spark*-*-*-*-*-*', 'clean')

    def clean_logs(self, hosts=None):
        """Remove log and temp files."""
        paths = ('/logs/*', '/work/*')
        full_paths = ' '.join((self._home + p for p in paths))
        cmd = 'rm -rf /tmp/spark*-*-*-*-*-* ' + full_paths
        title = 'clean logs -'
        self._ssh_hosts(cmd, title, hosts)

    def _history_server(self, action):
        cmd = '{}/sbin/{}-history-server.sh'.format(self._home, action)
        title = action + ' history server'
        self._ssh_master(cmd, title)

    def _write_slaves_file(self):
        filename = quote(self._home + '/conf/slaves')
        cmd = '>{}; for slave in {}; do echo $slave >>{}; done'.format(
            filename, ' '.join(self._slaves), filename)
        self._ssh_master(cmd, 'slaves file', True)

    def _ssh_master(self, cmd, title, stdout=False):
        ssh = SSH(self._master, cmd, title=title, logger_name=self.logger_name)
        self.executor.run(ssh)

    def _ssh_hosts(self, cmd, title, hosts=None):
        if hosts is None:
            hosts = [self._master] + self._slaves
        for host in hosts:
            host_title = '{} {}'.format(title, host)
            ssh = SSH(host, cmd,
                      title=host_title,
                      logger_name=self.logger_name)
            self.executor.run(ssh)
