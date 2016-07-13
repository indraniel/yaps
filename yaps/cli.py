from __future__ import print_function, division
import signal, importlib, sys, logging, os

import click

from .version import __version__

logLevels = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET']

@click.group()
@click.version_option(version=__version__)
def cli():
    # to make this script/module behave nicely with unix pipes
    # http://newbebweb.blogspot.com/2012/02/python-head-ioerror-errno-32-broken.html
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

@cli.command()
@click.option('--workspace', required=True, type=click.Path(),
              help='A directory to place outputs into')
@click.option('--job-db', default=None, type=click.Path(),
              help="Path to LSF job sqlite DB [default='<workspace>/job_queue.db']")
@click.option('--ruffus-history', default=None, type=click.Path(),
              help="Path to LSF job sqlite DB [default='<workspace>/job_queue.db']")
@click.option('--log', default=sys.stderr, type=click.File('w'),
              help="Path to write log details to [default=stdout]")
@click.option('--log-level', default='INFO', type=click.Choice(logLevels),
              help='Log Level -- [default=INFO]')
@click.option('--input-vcfs', required=True, type=click.Path(exists=True),
              help='A file of chromosomal VCFs to process')
@click.option('--project-name', default='yaps.default', type=click.STRING,
              help='A prefix used to name batch jobs')
@click.option('--email', default=None, type=click.STRING,
              help='An email used to notify about batch jobs [default=userid@genome.wustl.edu]')
@click.option('--timeout', default=43200, type=click.INT,
              help='Seconds to timeout for LSF job polling [default=43200 {12 hours}]')
@click.option('--config', default=None, type=click.Path(exists=True),
              help='An alternative configuration file to test')
def postvqsr(job_db, ruffus_history, log, log_level, input_vcfs, project_name, email, workspace, timeout, config):
    conf = importlib.import_module('yaps.configs.postvqsr')
    conf.initialize(input_vcfs, project_name, email, workspace, timeout, config)
    conf.dump_config()

    logLevel = getattr(logging, log_level.upper())

    if job_db is None:
        job_db = os.path.join(
            os.path.abspath(conf.config['workspace']),
            '.job_queue.db'
        )

    if ruffus_history is None:
        ruffus_history = os.path.join(
            os.path.abspath(conf.config['workspace']),
            '.ruffus_history.sqlite'
        )

    pipeline = importlib.import_module('yaps.pipelines.postvqsr')
    pipeline.initialize(job_db, ruffus_history, log, logLevel, input_vcfs)
    pipeline.log.info("LSF Job DB : {}".format(job_db))
    pipeline.log.info("Ruffus History DB : {}".format(ruffus_history))
    pipeline.run()
