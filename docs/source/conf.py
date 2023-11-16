# Configuration file for the Sphinx documentation builder.

# -- Path setup --------------------------------------------------------------

import os
import sys

package_dir = os.path.join(
    os.path.abspath(__file__), 
    '..', '..', '..', 'indexia'
)

sys.path.insert(0, package_dir)

# -- Project information -----------------------------------------------------

import indexia

project = indexia.__name__
copyright = f'2023, {indexia.__author__}'
author = indexia.__author__
version = indexia.__version__

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon'
]

napoleon_google_docstring = False
templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

html_theme = 'alabaster'
html_static_path = ['_static']

html_theme_options = {
    'description': 'Collections & connections. A virtual Zettelkasten.',
    'github_button': 'true',
    'github_repo': 'https://github.com/Perceptua/indexia',
    'github_user': 'Perceptua',
}