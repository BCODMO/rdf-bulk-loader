from flask import Flask, request, make_response, abort, send_from_directory
import grp
import logging
import os
import pwd
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.util import guess_format
import re
import requests
import shutil
import simplejson as json
from SPARQLWrapper import SPARQLWrapper, POST, BASIC, DIGEST
import traceback
import unicodedata
from urllib.parse import urlparse, unquote
from werkzeug.exceptions import HTTPException
import yaml

app = Flask(__name__, static_url_path='')

RDF_DOWNLOAD_PATH = '/web/rdf'

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

def download_url(url, file, **kwargs):
    app.logger.info(kwargs)
    app.logger.info(url)
    # https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
    with requests.get(url, stream=True, **kwargs) as r:
        r.raise_for_status()
        with open(file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def local_download_void_data_dump(url, path, user=None, group=None):
    data_dump_filename = url.rsplit('/', 1)[1]
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            raise Exception(path + ' does not exist and could not be created')
    file = path + os.path.sep + data_dump_filename

    # https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
    download_url(url, file)

    if user is not None:
        uid = pwd.getpwnam(user).pw_uid
        gid = grp.getgrnam(group).gr_gid if group is not None else -1
        app.logger.info('Setting permissions to: ' + user + ':' + group)
        os.chown(file, uid, gid)
    app.logger.info('Downloaded: ' + url + ' to ' + file)



### UTILS ###
def slugify(filename):
    """
    Convert to ASCII. Convert spaces to hyphens.
    Remove characters that aren't alphanumerics, underscores, or hyphens.
    Convert to lowercase. Also strip leading and trailing whitespace.
    """
    value = str(filename)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    return re.sub(r'[-\s]+', '-', value)

def read_config():
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
    return send_from_directory('/logs', path, mimetype='text/plain', cache_timeout=0)

@app.route('/rdf/turtle/<path>', methods=['GET'])
def view_rdf_turtle_file(path):
    app.logger.debug('RDF: ' + path)
    return send_from_directory('/web/rdf', path, mimetype='text/turtle', cache_timeout=0)

@app.route('/file-graph-loader', methods=['GET'])
def file_graph_load():
    if 'BASE_URL' not in os.environ:
        abort(404, description="No base URL defined")
    base_url = os.environ['BASE_URL']

    graph = request.args.get('graph')
    if graph is None:
        abort(404, description="Graph not provided")

    triplestore = request.args.get('triplestore')
    if triplestore is None:
        abort(404, description="Triplestore not provided")

    file = request.args.get('file')
    if file is None:
        abort(404, description="File not provided")
    else:
        file = unquote(file)

    rdf_format = request.args.get('format')
    if rdf_format is None:
        abort(404, description="RDF Format not provided")

    # get the latest configuration
    cfg = read_config()

    if 'triplestore' not in cfg:
        abort(404, description="No triplestore configuration")
    ts_cfg = cfg['triplestore'].get(triplestore, None)
    if ts_cfg is None:
        abort(404, description="Triplestore configuration not found")

    if 'endpoint' not in ts_cfg:
        abort(404, description="No triplestore endpoint")

    file_kwargs = None
    file_server = request.args.get('server')
    if file_server is not None and 'fileserver' in cfg and file_server in cfg['fileserver']:
        file_kwargs = {}
        custom_request_cfg = cfg['fileserver'][file_server]
        if 'headers' in custom_request_cfg:
            file_kwargs['headers'] = {}
            for header in custom_request_cfg['headers']:
                if 'env' in header:
                    file_kwargs['headers'][header['name']] = os.environ[header['env']]

    app.logger.info(file_kwargs)
    #if 'path' not in ts_cfg:
    ##    abort(404, description="No triplstore dump path in configuration")
    #dump_path = ts_cfg['path']
    dump_path = RDF_DOWNLOAD_PATH

    # Download file
    filename = slugify('__'.join(file.split('/')[2:]))
    destination = os.path.join(dump_path, filename)
    app.logger.debug('Download RDF file: ' + destination)
    if file_kwargs is not None:
        download_url(file, destination, **file_kwargs)
    else:
        download_url(file, destination)

    # Convert to turtle and remove original file
    if 'turtle' != rdf_format:
        # Parse original file
        rdf = Graph()
        app.logger.info('Parsing RDF at: ' + destination)
        rdf.parse(destination, format=rdf_format)
        # Prepare a turtle file
        filename += '.ttl'
        ttl_destination = os.path.join(dump_path, filename)
        app.logger.debug('Turtle file: ' + ttl_destination)
        # Convert RDF to turtle
        rdf.serialize(destination=ttl_destination, format='turtle')
        # Delete original file
        os.remove(destination)
        destination = ttl_destination

    # Load Turtle into Triplestore
    load_url = base_url + '/rdf/turtle/' + filename
    #load_url = 'https://www.w3.org/ns/dcat2.ttl'
    app.logger.debug('LOAD: ' + load_url)
    app.logger.debug('Endpoint: ' + ts_cfg['endpoint'])
    sparql = SPARQLWrapper(ts_cfg['endpoint'])
    auth = ts_cfg.get('auth', 'none')
    if 'none' != auth:
        user_var = triplestore+'_user'
        if user_var not in os.environ:
            abort(404, description="No triplestore username")
        pswd_var = triplestore+'_pswd'
        if pswd_var not in os.environ:
            abort(404, description="No triplestore password")
        if 'basic' == auth:
            app.logger.debug('BASIC Auth: ' + os.environ[user_var] + ':' + os.environ[pswd_var])
            sparql.setHTTPAuth(BASIC)
        elif 'digest' == auth:
            app.logger.debug('DIGEST Auth: ' + os.environ[user_var] + ':' + os.environ[pswd_var])
            sparql.setHTTPAuth(DIGEST)
        sparql.setCredentials(os.environ[user_var], os.environ[pswd_var])
    sparql.setMethod(POST)
    stmt = 'CLEAR GRAPH <' + graph + '> LOAD <' + load_url + '> INTO GRAPH <' + graph + '>'
    app.logger.info(stmt)
    sparql.setQuery(stmt)

    results = sparql.query()
    res = results.response.read()

    # Delete destination after loading
    os.remove(destination)

    # Send response
    json = {
        'response': res
    }
    response = make_response(json, 200)
    return response_json(response)


@app.route('/void-graph-loader', methods=['GET'])
def virtuoso_graph_load():
    graph = request.args.get('graph')
    if graph is None:
        abort(404, description="Graph not provided")

    triplestore = request.args.get('triplestore')
    if triplestore is None:
        abort(404, description="Triplestore not provided")

    # get the latest configuration
    cfg = read_config()
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
        app.logger.info('Dump path: ' + void_dump_path)
        for data_dump in void_rdf.objects(graph_uri, VOID.dataDump):
            data_dump_url = str(data_dump)
            app.logger.info(data_dump_url)
            local_download_void_data_dump(data_dump_url, void_dump_path, user=local_user, group=local_group)
            res['response']['void:dataDump'].append(data_dump_url)

        # Set the ready file
        write_ready_file(dump_path)

    response = make_response(res, 200)
    return response_json(response)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

