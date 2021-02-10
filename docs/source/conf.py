# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sass
from web_common import WEB_COMMON_ROOT
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'web_common'
copyright = '2020, Hail Team'
author = 'Hail Team'

nitpicky = True

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    # 'sphinx.ext.autosummary',
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'IPython.sphinxext.ipython_console_highlighting',
    'sphinx.ext.viewcode',
    'autodocsumm',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = [f'{WEB_COMMON_ROOT}/templates', '_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

intersphinx_mapping = {
    'python': ('https://docs.python.org/3.7', None),
    'PySpark': ('https://spark.apache.org/docs/latest/api/python/', None),
    'Bokeh': ('https://docs.bokeh.org/en/1.2.0/', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'scipy': ('https://docs.scipy.org/doc/scipy-1.3.3/reference', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),
    'aiohttp': ('https://docs.aiohttp.org/en/stable/', None)}


def setup(app):
    scss_path = '_static/common_static/styles'
    css_path = '_static/common_static/css'
    os.makedirs(scss_path, exist_ok=True)
    os.makedirs(css_path, exist_ok=True)

    sass.compile(
        dirname=(scss_path, css_path), output_style='compressed',
        include_paths=[f'{WEB_COMMON_ROOT}/styles'])

    def apidoc(path):
        from sphinx.ext import apidoc
        apidoc.main(['-o', './source', '-d2', '-feMT', path])

    def apidoc_all_the_things(garbage):
        apidoc('../web_common')
        apidoc('../gear/gear')
        # apidoc('../hail/python/hail')
        apidoc('../hail/python/hailtop')
    app.connect('builder-inited', apidoc_all_the_things)

autodoc_default_options = {
    'autosummary': True,
}

autodata_content = 'both'
