# -- Project information ------------------------------------------------

project = 'pclean'
author = 'MiCASA'
copyright = '2026, MiCASA'  # noqa: A001

# Dynamically read version from the package
try:
    from pclean._version import version as release
except ImportError:
    release = '0.0.0'
version = '.'.join(release.split('.')[:2])

# -- General configuration ---------------------------------------------

extensions = [
    'myst_parser',
    'sphinxcontrib.mermaid',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx_copybutton',
]

# MyST-Parser settings
myst_enable_extensions = [
    'colon_fence',
    'deflist',
    'fieldlist',
    'tasklist',
]
myst_fence_as_directive = ['mermaid']
myst_heading_anchors = 3

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

master_doc = 'index'
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------

html_theme = 'furo'
html_title = 'pclean'
html_logo = '_static/logo.png'
html_static_path = ['_static']
html_css_files = ['custom.css']
html_js_files = [
    'vendor/medium-zoom.min.js',
    'vendor/panzoom.min.js',
    'zoom.js',
]
html_theme_options = {
    'footer_icons': [],
}

# -- Intersphinx -------------------------------------------------------

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'dask': ('https://docs.dask.org/en/stable/', None),
}

# -- Autodoc -----------------------------------------------------------

# -- Mermaid ----------------------------------------------------------

mermaid_init_js = "mermaid.initialize({startOnLoad:true, theme:'base', flowchart:{wrappingWidth:120}});"

# -- Autodoc -----------------------------------------------------------

autodoc_member_order = 'bysource'
autodoc_typehints = 'description'

# -- Napoleon ----------------------------------------------------------

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_param = True
napoleon_use_rtype = True
