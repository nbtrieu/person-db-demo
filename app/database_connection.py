# put in services
# Connect to localhost
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal


def get_gremlin_client():
    connection = DriverRemoteConnection("ws://localhost:8183/gremlin", "g")
    g = traversal().withRemote(connection)
    return g
