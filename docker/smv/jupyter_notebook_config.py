import os

## Default value for IP address the notebook server will listen on. Will be overriden by command line
c.NotebookApp.ip = os.environ.get('JUPYTER_IP', '*')

## Deafult port for the notebook server will listen on. Will be overriden by command line
c.NotebookApp.port = int(os.environ.get('JUPYTER_PORT', '8888'))

## Disable authentication
c.NotebookApp.token = ''

## The work directory
c.FileContentsManager.root_dir = u'notebooks'

## Whether to open in a browser after starting. The specific browser used is
#  platform dependent and determined by the python standard library `webbrowser`
#  module, unless it is overridden using the --browser (NotebookApp.browser)
#  configuration option.
c.NotebookApp.open_browser = False

## Supply overrides for the tornado.web.Application that the Jupyter notebook
#  uses.
#  This is to allow third party app to embed notebook page in iframe
c.NotebookApp.tornado_settings = {
    'headers': {
        'Content-Security-Policy': "frame-ancestors *"
    }
}
