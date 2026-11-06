"""Assemble the Lambda code folders that template.yaml deploys, under build/ (git-ignored):

    build/extract_ng/   entry.py + extract_prod_ng.py
    build/extract_ky/   entry.py + extract_prod_ky.py
    build/load/         entry.py + load_redshift.py
    build/layer/python/ pyarrow + requests for Linux x86_64, Python 3.14 (lambda/pipeline/requirements.txt)

The scripts stay where they are in the repo, so they still run by hand as before. Run from the repo root:
    python deploy/build_lambdas.py            # everything
    python deploy/build_lambdas.py --no-deps  # code only (CI checks; skips downloading the layer packages)
"""
import argparse, shutil, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
ENTRY = ROOT / "lambda" / "pipeline" / "entry.py"
FUNCTIONS = {"extract_ng": "extract_prod_ng.py", "extract_ky": "extract_prod_ky.py", "load": "load_redshift.py"}
RUNTIME = "3.14"  # matches Runtime in template.yaml


def build_code():
    for folder, script in FUNCTIONS.items():
        target = BUILD / folder
        shutil.rmtree(target, ignore_errors=True)
        target.mkdir(parents=True)
        shutil.copy2(ENTRY, target / "entry.py")
        shutil.copy2(ROOT / script, target / script)
        print(f"build/{folder}: entry.py, {script}")


def build_layer():
    target = BUILD / "layer" / "python"
    shutil.rmtree(target.parent, ignore_errors=True)
    # Wheels for Lambda's Linux, whatever machine this runs on
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--no-compile",
                    "--requirement", str(ROOT / "lambda" / "pipeline" / "requirements.txt"),
                    "--target", str(target), "--implementation", "cp", "--python-version", RUNTIME,
                    "--platform", "manylinux_2_28_x86_64", "--platform", "manylinux2014_x86_64",
                    "--only-binary=:all:"], check=True)
    # Not needed at run time; keeps the layer well under Lambda's 250 MB unzipped limit
    for junk in ["pyarrow/include", "pyarrow/tests", "pyarrow/src"]:
        shutil.rmtree(target / junk, ignore_errors=True)
    for cache in target.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
    size = sum(f.stat().st_size for f in target.rglob("*") if f.is_file()) / 2**20
    print(f"build/layer: {size:.0f} MB unzipped")
    if size > 240:
        raise SystemExit("the layer is too big for Lambda (250 MB unzipped with the function code)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-deps", action="store_true", help="skip the layer's packages")
    build_code()
    if not parser.parse_args().no_deps:
        build_layer()
