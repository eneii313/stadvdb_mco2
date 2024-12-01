from flask import Flask, render_template, request, session
from datetime import datetime, date
import pymysql
import atexit

app = Flask(__name__)
app.secret_key = '12345'

PORT = 3000

# Database Nodes
HOST = "ccscloud.dlsu.edu.ph"
PASSWORD = "password"
DATABASE = "mco2"
USER = "user1"

nodes = [
    {"host": "ccscloud.dlsu.edu.ph", "id": 21262, "user": USER, "online": True, "engine": None},
    {"host": "ccscloud.dlsu.edu.ph", "id": 21272, "user": USER, "online": True, "engine": None},
    {"host": "ccscloud.dlsu.edu.ph", "id": 21282, "user": USER, "online": True, "engine": None},
]

#nodes = [
#    {"host": "10.2.0.126",  "id": 3306, "user": USER, "online": True, "engine": None},
#    {"host": "10.2.0.127", "id": 3306, "user": USER, "online": True, "engine": None},
#    {"host": "10.2.0.128", "id": 3306, "user": USER, "online": True, "engine": None},
#]

# try to connect to a specific node
def try_connection(node):
    try:
        connection = pymysql.connect(
            host=node["host"],
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


def fetch_data_from_node(node, query, params=None):
    if node["engine"]:
        try:
            with node["engine"].cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                return result
        except Exception as e:
            print(f"Error querying node {node['id']}: {e}")
    else:
        print(f"Error in fetching data: Node {node['id']} is not connected.")
    return None


# -- SQL ROUTES FOR UPDATING DB --



# -- ROUTES --
@app.route('/')
def home():
    #TODO: do proper node selection
    node = nodes[0] # all games node
    try_connection(node)

    if not node["engine"]:
        return render_template("error.html", message="Application is not currently connected to the database.")
    
    query = "SELECT COUNT(*) FROM games;"
    data = fetch_data_from_node(node, query)

    session['total'] = data[0][0]
    session['engine'] = node['id']

    if data:
        return render_template("all_games.html", total_count=session.get('total', 0))
    
    return render_template("error.html", message="No data found or an error occured.")

@app.route('/new_game')
def new_game():
    node_id = session.get('engine')
    node = next((node for node in nodes if node["id"] == node_id), None)

    #TODO: auto-increment appid after adding game
    query = "SELECT AppID FROM games ORDER BY AppID DESC LIMIT 1;"
    new_id = fetch_data_from_node(node, query)
    session['new_id'] = new_id[0][0] + 10

    return render_template("new_game.html", AppID=session.get('new_id'))

@app.route('/view_game/<int:appid>')
def view_game(appid):
    node_id = session.get('engine')
    node = next((node for node in nodes if node["id"] == node_id), None)

    query = "SELECT * FROM games WHERE AppID = %s"
    params = (appid)
    data = fetch_data_from_node(node, query, params)
    data = data[0]
    game = {
            "app_id": data[0],
            "name": data[1],
            "release_date": data[2],
            "price": float(data[3]), 
            "required_age": data[4],
            "dlc_count": data[5],
            "achievements": data[6],
            "about_the_game": data[7],
            "windows": data[8],
            "mac": data[9],
            "linux": data[10],
            "peak_ccu": data[11],
            "average_playtime_forever": data[12],
            "average_playtime_2weeks": data[13],
            "median_playtime_forever": data[14],
            "median_playtime_2weeks": data[15]
            }
    return render_template("view_game.html", game=game)

@app.route('/edit_game/<int:appid>')
def edit_game(appid):
    node_id = session.get('engine')
    node = next((node for node in nodes if node["id"] == node_id), None)

    query = "SELECT * FROM games WHERE AppID = %s"
    params = (appid)
    data = fetch_data_from_node(node, query, params)
    data = data[0]
    game = {
            "app_id": data[0],
            "name": data[1],
            "release_date": data[2],
            "price": float(data[3]), 
            "required_age": data[4],
            "dlc_count": data[5],
            "achievements": data[6],
            "about_the_game": data[7],
            "windows": data[8],
            "mac": data[9],
            "linux": data[10],
            "peak_ccu": data[11],
            "average_playtime_forever": data[12],
            "average_playtime_2weeks": data[13],
            "median_playtime_forever": data[14],
            "median_playtime_2weeks": data[15]
            }
    return render_template("edit_game.html", game=game)

@app.route('/all_games')
def all_games():
    # close all connections before accessing this node
    close_connections()

    node = nodes[0] # all games node
    try_connection(node)

    if not node["engine"]:
        return render_template("error.html", message="Application is not currently connected to the database.")
    
    query = "SELECT COUNT(*) FROM games;"
    data = fetch_data_from_node(node, query)

    session['total'] = data[0][0]
    session['engine'] = node['id']

    if data:
        return render_template("all_games.html", total_count=session.get('total', 0))
    
    return render_template("error.html", message="No data found or an error occured.")

@app.route('/search_all')
def search_all():
    node_id = session.get('engine')
    node = next((node for node in nodes if node["id"] == node_id), None)

    search = request.args.get('search')

    if not node["engine"]:
        return render_template("error.html", message="Application is not currently connected to the database.")
    
    if search:
        query = "SELECT AppID, name FROM games WHERE AppID = %s OR `name` LIKE %s"
        params = (search, f"%{search}%")

        data = fetch_data_from_node(node, query, params)
        return render_template("table.html", rows=data, query=search, total_count=session.get('total', 0), result_count=len(data))
    

@app.template_filter('format_date')
def format_date(value):
    if isinstance(value, date):
        return value.strftime("%B %d, %Y")
    try:
        return datetime.strptime(value, "%Y/%d/%m").strftime("%B %d, %Y")
    except ValueError:
        return value

# -- MAIN EXECUTION --
if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0', port=80)
    app.run(debug=True, port=PORT)

# clear sessions when app is shutting down
# @app.teardown_appcontext
# def shutdown_session(exception=None):
#     close_connections()
#     session.clear()
#     print("Session cleared and connections closed.")