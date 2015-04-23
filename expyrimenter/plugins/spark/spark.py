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
        self.slaves = slaves
        self.logger_name = 'spark'

    @property
    def slaves(self):
        return self._slaves

    @slaves.setter
    def set_slaves(self, value):
        if value is not None:
            self._slaves = value
            self._write_slaves_file()

    def start(self):
        cmd = self._home + '/sbin/start-all.sh'
        self._ssh_master(cmd, 'start')

    def stop(self):
        cmd = self._home + '/sbin/stop-all.sh'
        self._ssh_master(cmd, 'start')

    def submit(self, filename):
        cmd = 'spark-submit ' + quote(filename)
        title = 'run ' + filename
        self._ssh_master(cmd, title)

    def clean(self):
        # TODO anything to clean?
        pass

    def purge(self):
        """Be careful: this will erase all logs. You must stop() before."""
        cmd = 'rm -rf {}/logs/* {}/work/*'.format(self._home, self._home)
        title = 'erase logs'
        self._ssh_all(cmd, title)

    def _write_slaves_file(self, spark_home):
        filename = quote(spark_home + '/conf/slaves')
        cmd = '>{}; for slave in {}; do echo $slave >>{}; done'.format(
            filename, ' '.join(self.slaves), filename)
        self._ssh_master(cmd, 'slaves file')

    def _ssh_master(self, cmd, title):
        ssh = SSH(self._master, cmd, title=title, logger_name=self.logger_name)

    def _ssh_all(self, cmd, title):
        for host in self.slaves + [self._master]:
            ssh = SSH(host, cmd, title=title, logger_name=self.logger_name)
            self.executor.run(ssh)
