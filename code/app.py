from flask import Flask, request, make_response, abort, send_from_directory
import grp
import logging
import os
import pwd
from rdflib import Graph, Literal, Namespace, RDF, URIRef
import requests
import shutil
import simplejson as json
import sparql
import traceback
from werkzeug.exceptions import HTTPException
import yaml

app = Flask(__name__, static_url_path='', static_folder='/web/static')

### LOCAL ###
def write_ready_file(path):
    file = path + os.path.sep + 'ready'
    open(file, 'wb').write("".encode())
    app.logger.info('Wrote the ready file.')

def remove_ready_file(path):
    file = path + os.path.sep + 'ready'
    if os.path.exists(file):
        os.remove(file)
        app.logger.info('Removed the ready file.')

def local_check_dump_path(path):
    # does a locked file exist?
    lock_file = path + os.path.sep + 'locked'
    return os.path.exists(lock_file)

def local_download_void_data_dump(url, path, user=None, group=None):
    data_dump_filename = url.rsplit('/', 1)[1]
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            raise Exception(path + ' does not exist and could not be created')
    file = path + os.path.sep + data_dump_filename

    # https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
    with requests.get(url, stream=True) as r:
        #with open(file, 'wb') as f:
        #    shutil.copyfileobj(r.raw, f)
        r.raise_for_status()
        with open(file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)


    if user is not None:
        uid = pwd.getpwnam(user).pw_uid
        gid = grp.getgrnam(group).gr_gid if group is not None else -1
        app.logger.info('Setting permissions to: ' + user + ':' + group)
        os.chown(file, uid, gid)
    app.logger.info('Downloaded: ' + url + ' to ' + file)



### UTILS ###
def read_config(graph):
    # Read the configuration so that we can support on the fly
    with open('config.yaml', 'r') as yamlfile:
        cfg = yaml.safe_load(yamlfile)
    return cfg

def response_context():
    return {
        'schema': 'http://schema.org/',
        'error': 'schema:error',
        'name': 'schema:name',
        'description': 'schema:description'
    }

def response_json(response):
    response.content_type = "application/ld+json"
    return response

def response_error(msg, code, name=None):
    err = {
        'code': code,
        'description': msg,
        'stacktrace': traceback.format_exc()
    }

    log_msg = ''
    if name is not None:
        err['name'] = name
        log_msg += (name + ' - ')

    log_msg += msg
    app.logger.info('Error[' + str(code) + ']: ' + log_msg)

    return json.dumps({
        '@context': response_context(),
        'error': err
    })

@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        response = e.get_response()
        # replace the body with JSON
        response.data = response_error(e.description, e.code, name=e.name)
        return response_json(response)
    else:
        code = 500
        error = response_error(str(e), code)
        response = make_response(error, code)
        return response_json(response)


### ROUTING ###
@app.route('/logs/<path:path>', methods=['GET'])
def view_log_file(path):
    app.logger.debug('PATH: ' + path)
    return send_from_directory('/logs', path, mimetype='text/plain')


@app.route('/void-graph-loader', methods=['GET'])
def virtuoso_graph_load():
    graph = request.args.get('graph')
    if graph is None:
        abort(404, description="Graph not provided")

    triplestore = request.args.get('triplestore')
    if graph is None:
        abort(404, description="Triplestore not provided")

    # get the latest configuration
    cfg = read_config(graph)
    if 'void' not in cfg:
        abort(404, description="No VoID configuration")
    graph_cfg = cfg['void'].get(graph, None)
    if graph_cfg is None:
        abort(404, description="VoID graph configuration not found")

    # Get the configuration
    graph_uri = URIRef(graph_cfg['uri'])
    void_url = graph_cfg['void']
    triplestore_cfg = graph_cfg['triplestore'][triplestore]
    dump_type = triplestore_cfg['dump']
    dump_path = triplestore_cfg['path']

    # Read the VoID
    VOID = Namespace("http://rdfs.org/ns/void#")
    void_rdf = Graph()
    app.logger.info('Parsing VoID: ' + void_url)
    void_rdf.parse(void_url)

    # Prepare the response
    res = {
        '@context': response_context(),
        '@type': 'schema:SearchAction',
        'request':{
            'parameters':{
                'graph': graph,
                'triplestore': triplestore
            }
        },
        'response':{
            'void:dataDump':[]
        }
    }

    # Handle local data load
    if (dump_type == 'local'):
        # Check if dump path is being used
        if local_check_dump_path(dump_path):
            raise Exception(dump_path + ' is currently in use')

        # check for existing ready file
        remove_ready_file(dump_path)

        local_user = triplestore_cfg.get('user', None)
        local_group = triplestore_cfg.get('group', None)
        # Download the void.dataDumps
        void_dump_path = dump_path + os.path.sep + 'void'
        for data_dump in void_rdf.objects(graph_uri, VOID.dataDump):
            data_dump_url = str(data_dump)
            local_download_void_data_dump(data_dump_url, void_dump_path, user=local_user, group=local_group)
            res['response']['void:dataDump'].append(data_dump_url)

        # Set the ready file
        write_ready_file(dump_path)

    response = make_response(res, 200)
    return response_json(response)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

