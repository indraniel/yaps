# Yet Another Pipeline System (YAPS)

_A work in progress.  Not really ready for prime time.  This is a pure idea experiment and code may be abandoned in the future._

## Example execution

    yaps postvqsr --workspace=/path/to/workspace/name/test-pipeline --input-vcfs=inputs.txt --project-name="test-pipeline" --timeout=300 --log-level=INFO

## Deployment

    pip install git+https://github.com/indraniel/yaps.git

## Development

    git clone https://github.com/indraniel/yaps.git
    cd yaps
    virtualenv venv
    source venv/bin/activate
    pip install -e .
    # < do development work >
     
    # test
    yaps postvqsr --workspace=/path/to/workspace/name/test-pipeline --input-vcfs=inputs.txt --project-name="test-pipeline" --timeout=300 --log-level=INFO

    # clean up dev workspace
    make clean

## Misc. Notes

* Currently only the `postvqsr` pipeline exists.  See `yaps --help` and/or `yaps postvqsr --help` for more information on the available commands/pipelines and options.
* `--input-vcfs` is a tab-separated list of `*.vcf.gz` files in `<CHROM>\t<VCF.GZ FILE>` format

