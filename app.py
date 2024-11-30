from flask import Flask, render_template, request
import pymysql

app = Flask(__name__)

PORT = 3000

# Database Nodes
HOST = "ccscloud.dlsu.edu.ph"
PASSWORD = "password"
DATABASE = "mco2"
USER = "user1"

nodes = [
    {"id": 21262, "user": USER, "online": True, "engine": None},
    {"id": 21272, "user": USER, "online": True, "engine": None},
    {"id": 21282, "user": USER, "online": True, "engine": None},
]

# try to connect to a specific node
def try_connection(node):
    try:
        connection = pymysql.connect(
            host=HOST,
            port=node["id"],
            user=node["user"],
            password=PASSWORD,
            database=DATABASE
        )
        print(f"Connected to node {node['id']}")
        node["engine"] = connection
    
    except Exception as e:
        node["online"] = False
        print(f"Failed to connect to node {node['id']}: {e}")


# create connection to all nodes
def init_connections():
    for node in nodes:
        if node["online"] and not node["engine"]:
            try_connection(node)


# close all connections
def close_connections():
    for node in nodes:
        if node["engine"]:
            node["engine"].close()
            node["engine"] = None
            print(f"Connection to node {node['id']} closed.")


def fetch_data_from_node(node, query):
    if node["engine"]:
        try:
            with node["engine"].cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
        except Exception as e:
            print(f"Error querying node {node['id']}: {e}")
    else:
        print(f"Node {node['id']} is not connected.")
    return None


# -- ROUTES --
@app.route('/')
def home():
    return render_template("index.html")

@app.route('/all_games')
def all_games():
    # close all connections before accessing this node
    close_connections()
    
    node = nodes[0] # all games node
    try_connection(node)

    if not node["engine"]:
        return render_template("error.html", message="Application is not currently connected to the All Games node.")
    
    query = "SELECT * FROM games LIMIT 10;"
    data = fetch_data_from_node(node, query)

    if data:
        return render_template("table.html", rows=data)
    
    return render_template("error.html", message="No data found or an error occured.")


# -- MAIN EXECUTION --
if __name__ == '__main__':
    try:
        app.run(debug=True, port=PORT)
    finally:
        close_connections()