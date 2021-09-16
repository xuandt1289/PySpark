import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf
import psutil

def init_spark_context():
    import findspark
    findspark.init()

    import pyspark
    from pyspark.sql import SparkSession

    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass additional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'myapp.py'])

    return sc


def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

# Init spark context and load libraries
sc = init_spark_context()
dataset_path = os.path.join('datasets', 'ml-latest')
app = create_app(sc, dataset_path)

# start web server
run_server(app)


if __name__ == '__main__':
    app.run()
