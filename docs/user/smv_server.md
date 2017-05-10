# SMV Server
SMV provides a RESTFull API server for running/viewing/updating modules.  Much of the interface is in Alpha right now and will change dramatically in the short term.

## Starting the server
The server must be started from the top level directory of an SMV project.
```bash
$ cd myproject
$ smv-server
```
By default the server will start to listen on port 5000.

## Extending the server
**Note:** this is WIP

Users can enhance the base `smv-server` by adding their own entry points as follows:

### Create entry points script
```python
# myserver.py
import smvserver

@smvserver.app.route("/api/new_entry_point", methods = ['POST'])
def new_entry_point():
    '''implementation of new entry point goes here'''
    pass

if __name__ == "__main__":
    smvserver.Main()
```

### Launch server with new entry point
```bash
$ cd myproject
$ smv-server -e myserver.py
```
## API
TBD: wait until it is a bit more stable.
