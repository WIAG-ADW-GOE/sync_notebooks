# Installation

## 1. Downloading the repository

Start by, if you haven't already, downloading the repository. You can do this either with git (e.g. `git clone https://github.com/WIAG-ADW-GOE/sync_notebooks.git C:\Users\Public\sync_notebooks`) or by downloading the content as a .zip file ([direct link](https://github.com/WIAG-ADW-GOE/sync_notebooks/archive/refs/heads/main.zip)), extracting the contents and moving the directory to where you want (the recommended location is `C:\Users\Public\sync_notebooks`, because then you won't have to adjust the paths in the notebooks).

## 2. App for using Jupyter Notebooks
There are several options for using Jupyter notebooks:
- use [VSCodium](https://vscodium.com/) (or [VSCode](https://code.visualstudio.com/)) to run the notebooks. This is probably the most user-friendly option.
- use an online service like [the one by the GWDG](https://academiccloud.de/services/jupyter/) to run the notebooks. This way you don't need to install anything on your own device, but installing packages might be more of a hassle.
- use the official [Jupyter Lab](https://jupyter.org/)
- etc.

With VSCodium you need to use the "Open Folder" option and select `C:\Users\Public\sync_notebooks`. Then you can open and run a notebook by selecting it in the sidebar (first follow the rest of the guide though to make sure Python and all the packages are installed).

## 3. Installation of Python and required packages

If you use an online service, Python will already be installed, and uv probably won't (and you probably won't be able to install it), so you should skip to [Installing packages with pip](#installing-packages-with-pip)

### Using uv (recommended)

If you don't have uv installed yet, check out [the guide here](https://docs.astral.sh/uv/getting-started/installation/), or if you just want to install uv for yourself without any special requirements:
1. open a Powershell
2. run `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
3. close the Powershell and open a new one (so you can then use uv)

If you already have uv installed, all you need to do is run `uv sync` in the project directory, i.e.:

1. open a Powershell
2. run `cd C:\Users\Public\sync_notebooks`
3. run `uv sync`

This will install both Python, if it's not installed yet, and the necessary packages.

### Without uv
 
If you don't want to install uv, you can also install packages with, e.g., pip.

#### Python

Before installing packages you need to make sure that you have Python (>3.13) installed. You can download it [here](https://www.python.org/downloads/).

#### Installing packages with pip

Its recommended (but not necessary) to create a virtual environment for the repository. If you don't want to do that, just skip steps 3 and 4.

1. open a Powershell
2. to move into the project directory run: `cd C:\Users\Public\sync_notebooks`
3. to create the virtual environment, run: `python -m venv .venv`
4. to activate it, run: `.venv\Scripts\activate`
5. now to install the packages, run: `pip install requests pandas polars dotenv datetime openai aiohttp ipykernel`
