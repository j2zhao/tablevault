# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
# and the package folder is in 'tablevault'
sys.path.insert(0, os.path.abspath('../../tablevault'))

project = 'TableVault'
copyright = '2025, Jinjin Zhao'
author = 'Jinjin Zhao'
release = '0.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinxcontrib.autodoc_pydantic'
]

templates_path = ['_templates']
exclude_patterns = []


autodoc_pydantic_model_show_json = False
autodoc_pydantic_model_show_validator_members = False



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
