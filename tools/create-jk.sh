#!/bin/bash
#
# Jupyter kernel setup
#
#
# dvp, Apr 2022
#

envs=( $(pyenv local) )
env=${envs[0]}
pkgs=( $(ls src) )
pkg=${pkgs[0]}

echo "Creating jupyter kernel for python environment $env"
# python -m pip install jupyterlab
python -m ipykernel install --user --name "$env"
if [[ $? ]]; then
    echo "To use $env environment in jupyter:"
    echo "  - Run 'jupyter lab'"
    echo "  - Open or create notebook"
    echo "  - Select kernel $env"
    echo "  - check if 'import $pkg' in the notebook works"
    echo
    echo "To remove a kernel use jupyter commands:"
    echo "  jupyter kernelspec list"
    echo "  jupyter kernelspec remove <kernels...>"
fi
