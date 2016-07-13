import os, pwd, json
import pkg_resources

email = None
workspace = None
timeout = 43200 # 12 hours in seconds (12 * 60 * 60)
input_files = []
config = {}
project_name = None

def parse_input_vcf_file(file):
    global input_files
    with open(file, 'r') as f:
        vcfs = [ line.rstrip().split("\t")[1] for line in f ]
    return vcfs

def setup_email(email_address):
    if email_address is None:
        user_id = pwd.getpwuid( os.getuid() ).pw_name
        return '{}@genome.wustl.edu'.format(user_id)
    return email_address

def setup_workspace(workspace):
    if not os.path.exists(workspace):
        os.makedirs(workspace)
    return workspace

def setup_config(alt_config):
    config = custom_config(alt_config) if alt_config else standard_config()
    return config

def custom_config(alt_config):
    config = json.load(alt_config)
    return config

def standard_config():
    config = {
        'inputs' : input_files,
        'workspace' : workspace,
        'project-name': project_name,
        'lsf-timeout' : timeout,
        'ac-0-removal' : {
            'outdir' : os.path.join(workspace, '1-select-variants-ac-0-removal'),
            'CMD' : (
                "{java} -jar {jar} -T SelectVariants -R {reference} "
                "--removeUnusedAlternates -V {invcf} -L {chrom} -o {outvcf}"
            ),
            'LSF' : {
                'u' : email,
                'N' : None,
                'q' : "long",
                'M' : 8000000,
                'R' : 'select[mem>8000] rusage[mem=8000]',
                'J' : '{job_name}',
                'oo': '{log_path}',
            },
            'cmdArgs' : {
                'java' : '/gapp/x64linux/opt/java/jre/jre1.7.0_45/bin/java',
                'jar'  : '/usr/share/java/GenomeAnalysisTK-3.4.jar',
                'java_opts' : "-Xmx4096m",
                'reference' : '/gscmnt/gc2719/halllab/genomes/human/GRCh37/1kg_phase1/human_g1k_v37.fasta',
                'invcf' : None,
                'outvcf' : None,
                'chrom' : None,
            },
            'input-file-format'  : r'\S*/(?P<chrom>\S+)/combined.\S+\.FINMETSEQ\.recal\.het\.genotype\.annotated\.vcf\.gz$',
            'output-file-format' : os.path.join(
                workspace,
                '1-select-variants-ac-0-removal',
                r'{chrom[0]}/combined.c{chrom[0]}.vcf.gz',
            ),
        },
        'decompose-normalize-uniq' : {
            'outdir' : os.path.join(workspace, '2-decompose-normalize-uniq'),
            'CMD' : (
                "bash {script} {invcf} {outvcf}"
            ),
            'LSF' : {
                'u' : email,
                'N' : None,
                'q' : "long",
                'M' : 8000000,
                'R' : 'select[mem>8000] rusage[mem=8000]',
                'J' : '{job_name}',
                'oo': '{log_path}',
            },
            'cmdArgs' : {
                'script' : pkg_resources.resource_filename('yaps', 'data/postvqsr/run-decompose.sh'),
                'invcf' : None,
                'outvcf' : None,
            },
            'input-file-format'  : r'\S*/(?P<chrom>\S+)/combined.c\S+\.vcf\.gz$',
            'output-file-format' : os.path.join(
                workspace,
                '2-decompose-normalize-uniq',
                r'{chrom[0]}/combined.c{chrom[0]}.vcf.gz',
            ),
        },
        'filter-missingness' : {
            'outdir' : os.path.join(workspace, '3-filter-missingness'),
            'CMD' : (
                "bash {script} {invcf} {outvcf} {stats}"
            ),
            'LSF' : {
                'u' : email,
                'N' : None,
                'q' : "long",
                'M' : 8000000,
                'R' : 'select[mem>8000] rusage[mem=8000]',
                'J' : '{job_name}',
                'oo': '{log_path}',
            },
            'cmdArgs' : {
                'script' : pkg_resources.resource_filename('yaps', 'data/postvqsr/filter-missingness.sh'),
                'invcf' : None,
                'outvcf' : None,
                'stats' : None,
            },
            'input-file-format'  : r'\S*/(?P<chrom>\S+)/combined.c\S+\.vcf\.gz$',
            'output-file-format' : os.path.join(
                workspace,
                '3-filter-missingness',
                r'{chrom[0]}/filtered.c{chrom[0]}.vcf.gz',
            ),
        },
        'annotate-with-1000G' : {
            'outdir' : os.path.join(workspace, '4-annotate-w-1000G'),
            'CMD' : (
                "bash {script} {invcf} {outvcf}"
            ),
            'LSF' : {
                'u' : email,
                'N' : None,
                'q' : "long",
                'M' : 8000000,
                'R' : 'select[mem>8000] rusage[mem=8000]',
                'J' : '{job_name}',
                'oo': '{log_path}',
            },
            'cmdArgs' : {
                'script' : pkg_resources.resource_filename('yaps', 'data/postvqsr/annotate-w-1000G.sh'),
                'invcf' : None,
                'outvcf' : None,
            },
            'input-file-format'  : r'\S*/(?P<chrom>\S+)/filtered.c\S+\.vcf\.gz$',
            'output-file-format' : os.path.join(
                workspace,
                '4-annotate-w-1000G',
                r'{chrom[0]}/1kg.annotated.c{chrom[0]}.vcf.gz',
            ),
        },
    }

    return config

def initialize(input_vcfs, prj_name, email_address, wkspace, time_out, alt_config):
    global email, workspace, input_files, config, timeout, project_name
    project_name = prj_name
    timeout = time_out
    email = setup_email(email_address)
    workspace = setup_workspace(wkspace)
    input_files = parse_input_vcf_file(input_vcfs)
    config = setup_config(alt_config)

def dump_config():
    data = json.dumps(config, sort_keys=True, indent=4)
    outfile = os.path.join(workspace, 'config.json')
    with open(outfile, 'w') as f:
        f.write(data)
