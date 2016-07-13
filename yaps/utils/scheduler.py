import os, sys, pwd, six, time, logging
import subprocess as sp

try:
    from shlex import quote #py3
except ImportError:
    from pipes import quote #py2

from clint.textui import colored

import yaps.utils.logger as logger

user_id = pwd.getpwuid( os.getuid() ).pw_name

default_lsf_params = {
    'q' : "short",
    'u' : "{}@genome.wustl.edu".format(user_id),
    'N' : None,
    'M' : "8000000",
}

class BatchJobManager(object):
    def __init__(self, logwriter):
        self.log = logwriter

    def submit_job(self, cmd, job_name, job_params=default_lsf_params):
        submit = bsub(job_name, log=self.log, **job_params)
        jobid = submit(cmd).job_id
        msg = colored.green('Generated LSF job ID: {}'.format(jobid))
        self.log.info(msg)
        return jobid

# modeled on https://github.com/brentp/bsub/blob/master/bsub/bsub.py
class BSubException(Exception):
    pass

class BSubJobNotFound(BSubException):
    pass

class bsub(object):
    TEST_ONLY = -1000
    stdlogger = logger.create('BSUB', sys.stderr, logging.INFO)

    def __init__(self, job_name, *args, **kwargs):
        self.log = kwargs.pop('log', self.__class__.stdlogger)
        self.verbose = kwargs.pop('verbose', False)
        self.kwargs = kwargs
        self.job_name = job_name
        self.args = args
        assert len(args) in (0, 1)
        self.job_id = None

    def __int__(self):
        return int(self.job_id)

    def __long__(self):
        return long(self.job_id)

    def __str__(self):
        return self.command

    def __repr__(self):
        return "bsub('%s')" % self.job_name

    @property
    def command(self):
        s = self.__class__.__name__

        return s + " " + self._kwargs_to_flag_string(self.kwargs) \
                 + ((" < %s" % self.args[0]) if len(self.args) else "")

    def __call__(self, input_string=None, job_cap=None):
        if job_cap is not None:
            self._cap(job_cap)
        if input_string is None:
            assert len(self.args) == 1
            command = str(self)
        else:
            command = "echo \"%s\" | %s" % (input_string, str(self))

        if self.verbose == self.__class__.TEST_ONLY:
            self.job_id = self.__class__.TEST_ONLY
            return self

        res = self._run(command, log=self.log)
        job = res.split("<", 1)[1].split(">", 1)[0]
        self.job_id = job
        return self

    @classmethod
    def _kwargs_to_flag_string(cls, kwargs):
        s = ""
        for k, v in kwargs.items():
            # quote if needed.
            if isinstance(v, (float, int)):
                pass
            elif v and (v[0] not in "'\"") and any(tok in v for tok in "[="):
                v = "\"%s\"" % v
            s += " -" + k + ("" if v is None else (" " + str(v)))
        return s

    def kill(self):
        if self.job_id is None: return
        return bsub.kill(int(self.job_id))

    def bkill(cls, *args, **kwargs):
        kargs = cls._kwargs_to_flag_string(kwargs)
        if all(isinstance(a, six.integer_types) for a in args):
            command = "bkill " + kargs + " " + " ".join(args)
            cls._run(command, "is being terminated")
        else:
            for a in args:
                command = "bkill " + kargs.strip() + " -J " + a
                cls._run(command, "is being terminated")

    def _get_job_name(self):
        return self._job_name

    def _set_job_name(self, job_name):
        kwargs = self.kwargs
        kwargs["J"] = quote(job_name)
        self.kwargs = kwargs
        self._job_name = kwargs["J"]

    job_name = property(_get_job_name, _set_job_name)

    @classmethod
    def _run(cls, command, check_str="is submitted", log=None):
        if log is None: log = cls.stdlogger
        log.info(colored.yellow("LSF EXEC CMD: {}".format(command)))

        p = sp.Popen(command, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        p.wait()
        res = p.stdout.read().strip().decode("utf-8", "replace")
        err = p.stderr.read().strip().decode("utf-8", "replace")
        if p.returncode == 255:
            raise BSubJobNotFound(command)
        elif p.returncode != 0:
            if(res): sys.stderr.write(res)
            if(err): sys.stderr.write(err)
            raise BSubException(command + "[" + str(p.returncode) + "]")
        if not (check_str in res and p.returncode == 0):
            raise BSubException(res)
        return res

    @classmethod
    def running_jobs(cls, names=False):
        # grab the integer id (names=False) or # the name (names=True)
        # depending on whether they requested
        return [x.split(None, 7)[-2 if names else 0]
                for x in sp.check_output(["bjobs", "-w"])\
                           .decode().rstrip().split("\n")[1:]
                           if x.strip()
               ]

    @classmethod
    def poll(cls, job_ids, timeout=43200, log=None): # 43200 secs <=> 12 hours
        if log is None: log = cls.stdlogger

        log.info('Entering LSF wait poller')

        if isinstance(job_ids, six.string_types):
            job_ids = [job_ids]

        if len(job_ids) == []:
            return

        job_ids = frozenset(job_ids)

        active_jobs = cls.running_jobs()
        log.info("There are {} active jobs running".format(len(active_jobs)))

	kill_timer = 0
        sleep_time = 1

        while job_ids.intersection(active_jobs):
            logMsg = "Sleeping for: {} secs (kill_timer: {}) [timeout={}]"
            log.debug(logMsg.format(sleep_time, kill_timer, timeout))

            time.sleep(sleep_time)
            active_jobs = cls.running_jobs()
            log.debug("There are {} active jobs running".format(len(active_jobs)))
            kill_timer += sleep_time
            cls.killpoll(kill_timer, timeout, active_jobs)

            if sleep_time < 180:
                sleep_time += 0.25

        log.info('Exiting LSF wait poller')
        return True

    @classmethod
    def killpoll(cls, killtimer, timeout, leftover_jobs):
        if killtimer >= timeout:
            msg = ('There are {} LSF jobs running past the timeout: {}.'
                   'Please investigate!')
            msg = msg.format(len(leftover_jobs), lefover_jobs)
            sys.exit(msg)

    @classmethod
    def _cap(self, max_jobs):
        sleep_time = 1
        while len(self.running_jobs()) >= max_jobs:
            time.sleep(sleep_time)
            if sleep_time < 180:
                sleep_time += 0.25
        return True
