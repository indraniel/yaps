import os, pwd
import pprint, sys

pp = pprint.PrettyPrinter(indent=4)

from ruffus import *
from clint.textui import colored

from yaps.utils.jobqueue import DrmaaJobQueue
from yaps.utils.scheduler import BatchJobManager

import yaps.utils.logger as logger
import yaps.configs.postvqsr as conf

queue = None
LSF = None
log = None
ruffus_history_path = None
orig_files = conf.input_files
config = conf.config

def initialize(job_db, ruffus_history, logfh, log_level, input_vcfs):
    global log, queue, LSF, orig_files, ruffus_history_path
    ruffus_history_path = ruffus_history
    log = logger.create('postvqsr', logfh, log_level)
    queue = DrmaaJobQueue(job_db, log)
    LSF = BatchJobManager(log)

def wait(timeout=config['lsf-timeout']):
    queue.wait(timeout, log)

def check_file_exists(file):
    flag = True if os.path.exists(file) else False
    return flag

def update_output_file_modification_time(file):
    log.info(colored.blue('Output already exists: {}'.format(file)))
    log.info('Updating output file modification time')
    os.utime(file, None)

@originate(orig_files)
def start(infile):
    pass

@posttask(wait)
@follows(start, mkdir(config['ac-0-removal']['outdir']))
@transform(
    start,                                                    # inputs
    formatter(config['ac-0-removal']['input-file-format']),   # file structure
    config['ac-0-removal']['output-file-format'],             # replacement
    "{chrom[0]}",                                             # chrom
)
def ac_0_removal(invcf, outvcf, chrom):
    print("infile: {}".format(invcf))
    print("outfile: {}".format(outvcf))
    print("chrom: {}".format(chrom))

    if check_file_exists(outvcf):
        update_output_file_modification_time(outvcf)
        return

    context = config['ac-0-removal']

    outdir = os.path.dirname(outvcf)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # properly fill up the command arguments
    cmdArgs = context['cmdArgs']
    cmdArgs['invcf'] = invcf
    cmdArgs['outvcf'] = outvcf
    cmdArgs['chrom'] = chrom

    # setup the command to give to LSF/DRMAA
    cmd = context['CMD'].format(**cmdArgs)
    print('cmd: {}'.format(cmd))

    # setup the LSF/DRMAA job params
    jobName = '-'.join([
        config['project-name'],
        '1-ac-0-removal',
        'chrom-{}'.format(chrom)
    ])
    lsfParams = context['LSF']
    lsfParams['oo'] = os.path.join(context['outdir'], chrom, 'gatk-log-%J.log')

    jobId = LSF.submit_job(cmd, jobName, job_params=lsfParams)
    queue.append(jobId)

@posttask(wait)
@follows(ac_0_removal, mkdir(config['decompose-normalize-uniq']['outdir']))
@transform(
    ac_0_removal,                                                         # inputs
    formatter(config['decompose-normalize-uniq']['input-file-format']),   # file structure
    config['decompose-normalize-uniq']['output-file-format'],             # replacement
    "{chrom[0]}",                                                         # chrom
)
def decompose_normalize_uniq(invcf, outvcf, chrom):
    print("infile: {}".format(invcf))
    print("outfile: {}".format(outvcf))
    print("chrom: {}".format(chrom))

    if check_file_exists(outvcf):
        update_output_file_modification_time(outvcf)
        return

    context = config['decompose-normalize-uniq']

    outdir = os.path.dirname(outvcf)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # properly fill up the command arguments
    cmdArgs = context['cmdArgs']
    cmdArgs['invcf'] = invcf
    cmdArgs['outvcf'] = outvcf

    # setup the command to give to LSF/DRMAA
    cmd = context['CMD'].format(**cmdArgs)
    print('cmd: {}'.format(cmd))

    # setup the LSF/DRMAA job params
    jobName = '-'.join([
        config['project-name'],
        '2-decompose-normalize-uniq',
        'chrom-{}'.format(chrom)
    ])
    lsfParams = context['LSF']
    lsfParams['oo'] = os.path.join(
        context['outdir'],
        chrom,
        'decompose-normalize-uniq-log-%J.log'
    )

    jobId = LSF.submit_job(cmd, jobName, job_params=lsfParams)
    queue.append(jobId)

@posttask(wait)
@follows(decompose_normalize_uniq, mkdir(config['filter-missingness']['outdir']))
@transform(
    decompose_normalize_uniq,                                             # inputs
    formatter(config['filter-missingness']['input-file-format']),         # file structure
    config['filter-missingness']['output-file-format'],                   # replacement
    "{chrom[0]}",                                                         # chrom
)
def filter_missingness(invcf, outvcf, chrom):
    print("infile: {}".format(invcf))
    print("outfile: {}".format(outvcf))
    print("chrom: {}".format(chrom))

    if check_file_exists(outvcf):
        update_output_file_modification_time(outvcf)
        return

    context = config['filter-missingness']

    outdir = os.path.dirname(outvcf)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # properly fill up the command arguments
    cmdArgs = context['cmdArgs']
    cmdArgs['invcf'] = invcf
    cmdArgs['outvcf'] = outvcf
    cmdArgs['stats'] = os.path.join(
        context['outdir'],
        chrom,
        "{}.stats.missingness.out".format(chrom)
    )

    # setup the command to give to LSF/DRMAA
    cmd = context['CMD'].format(**cmdArgs)
    print('cmd: {}'.format(cmd))

    # setup the LSF/DRMAA job params
    jobName = '-'.join([
        config['project-name'],
        '3-filter-missingness',
        'chrom-{}'.format(chrom)
    ])
    lsfParams = context['LSF']
    lsfParams['eo'] = os.path.join(context['outdir'], chrom, 'missingness-%J.err')
    lsfParams['oo'] = os.path.join(context['outdir'], chrom, 'missingness-%J.out')

    jobId = LSF.submit_job(cmd, jobName, job_params=lsfParams)
    queue.append(jobId)

@posttask(wait)
@follows(filter_missingness, mkdir(config['annotate-with-1000G']['outdir']))
@transform(
    filter_missingness,                                                   # inputs
    formatter(config['annotate-with-1000G']['input-file-format']),        # file structure
    config['annotate-with-1000G']['output-file-format'],                  # replacement
    "{chrom[0]}",                                                         # chrom
)
def annotate_with_1000G(invcf, outvcf, chrom):
    print("infile: {}".format(invcf))
    print("outfile: {}".format(outvcf))
    print("chrom: {}".format(chrom))

    if check_file_exists(outvcf):
        update_output_file_modification_time(outvcf)
        return

    context = config['annotate-with-1000G']

    outdir = os.path.dirname(outvcf)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # properly fill up the command arguments
    cmdArgs = context['cmdArgs']
    cmdArgs['invcf'] = invcf
    cmdArgs['outvcf'] = outvcf

    # setup the command to give to LSF/DRMAA
    cmd = context['CMD'].format(**cmdArgs)
    print('cmd: {}'.format(cmd))

    # setup the LSF/DRMAA job params
    jobName = '-'.join([
        config['project-name'],
        '4-annotate-w-1000G',
        'chrom-{}'.format(chrom)
    ])
    lsfParams = context['LSF']
    lsfParams['oo'] = os.path.join(context['outdir'], chrom, '1000G-annotate-%J.out')

    jobId = LSF.submit_job(cmd, jobName, job_params=lsfParams)
    queue.append(jobId)

def end():
    return annotate_with_1000G

def run():
    pipeline_run(
        target_tasks = [end()],
        exceptions_terminate_immediately=True,
        history_file = ruffus_history_path,
    )
