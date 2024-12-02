from flask import Flask, render_template, request, session
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
import pymysql

app = Flask(__name__)
app.secret_key = '12345'



# Database Nodes
HOST = "ccscloud.dlsu.edu.ph"
PASSWORD = "password"
DATABASE = "mco2"
USER = "user1"
SCHEMA = "mco2"
PORT = 3000


nodes = [
    {"host": HOST, "id": 21262, "user": USER, "online": True, "engine": None, "session": None}, # master node
    {"host": HOST, "id": 21272, "user": USER, "online": True, "engine": None, "session": None}, # windows exclusive node
    {"host": HOST, "id": 21282, "user": USER, "online": True, "engine": None, "session": None}, # multiplatform node
]

#nodes = [
#    {"host": "10.2.0.126",  "id": 3306, "user": USER, "online": True, "engine": None, "session": None},
#    {"host": "10.2.0.127", "id": 3306, "user": USER, "online": True, "engine": None, "session": None},
#    {"host": "10.2.0.128", "id": 3306, "user": USER, "online": True, "engine": None, "session": None},
#]


# try to connect to a specific node
def try_connection(node):
    try:
        engine_url =  f"mysql+pymysql://{USER}:{PASSWORD}@{node['host']}:{node['id']}/{SCHEMA}"
        engine = create_engine(engine_url, echo=True)

        with engine.connect() as connection:
            print(f"Connected to node {node['id']}")

        node['engine'] = engine
        node['session'] = sessionmaker(bind=node['engine'])
        node['online'] = True

    except Exception as e:
        node["online"] = False
        return render_template("error.html", f"Failed to connect to node {node['id']}: {e}")

# initialize connections to each node
def init_connections(): 
    for node in nodes:
        try_connection(node)
        

# create connection to all nodes
# def init_connections():
#     for node in nodes:
#         if node["online"] and not node["engine"]:
#             try_connection(node)

def get_master_node():
    return nodes[0]

def get_slave_node(game_type):
    if game_type == 'windows':
        return nodes[1]
    elif game_type == 'multiplatform':
        return nodes[2]
    else:
        raise ValueError("Invalid game type. Use 'windows' or 'multiplatform'.")


# try to connect to a specific node
# def try_connection(node):
#     try:
#         connection = pymysql.connect(
#             host=node["host"],
#             port=node["id"],
#             user=node["user"],
#             password=PASSWORD,
#             database=DATABASE
#         )
#         print(f"Connected to node {node['id']}")
#         node["engine"] = connection
    
#     except Exception as e:
#         node["online"] = False
#         print(f"Failed to connect to node {node['id']}: {e}")


# close all connections
# def close_connections():
#     for node in nodes:
#         if node["engine"]:
#             node["engine"].close()
#             node["engine"] = None
#             print(f"Connection to node {node['id']} closed.")

def close_connections():
    for node in nodes:
        if node["session"]:
            node["session"].close()
            node["session"] = None
            print(f"Session for node {node['id']} closed.")
    
        if node["engine"]:
            node["engine"].dispose()
            node["engine"] = None
            print(f"Connection to node {node['id']} closed.")


# ========== SQL CRUD ROUTES ==========
# WRITE TRANSACTION
def create_game(game):
    #TODO: write to slave nodes after master node
    master_node = get_master_node()
    Session = master_node['session']()


    # game = session.query(Game).filter_by(id=data_id).first()
    # game.name = new_name
    # session.commit()
    # print(f"Written to master: {new_name}")
    Session.close()


#  READ TRANSACTION
def fetch_data_from_node(node, query, params=None):
    Session = node['session']()
    
    with Session.begin():
        
        Session.connection(execution_options={"isolation_level": "READ COMMITTED"})
        if Session:
            try:
                data = Session.execute(query, params).fetchall()
                Session.close()
                return data
            except Exception as e:
                print(f"Error querying node {node['id']}: {e}")
        else:
            print(f"Error in fetching data: Node {node['id']} is not connected.")
    return None

# ========== WEB ROUTES ==========
@app.route('/')
def home():
    return render_template("all_games.html", filter="all")

@app.route('/new_game')
def new_game():
    node_id = session.get('node')
    node = next((node for node in nodes if node["id"] == node_id), None)

    #TODO: auto-increment appid after adding game
    # query = text("SELECT AppID FROM games ORDER BY AppID DESC LIMIT 1;")
    # new_id = fetch_data_from_node(node, query)
    # session['new_id'] = new_id[0][0] + 10

    return render_template("new_game.html")

@app.route('/view_game/<int:appid>')
def view_game(appid):
    node_id = session.get('node')
    node = next((node for node in nodes if node["id"] == node_id), None)

    query = text("SELECT * FROM games WHERE AppID = :appid")
    params = {"appid":appid}
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
    node_id = session.get('node')
    node = next((node for node in nodes if node["id"] == node_id), None)

    query = text("SELECT * FROM games WHERE AppID = :appid")
    params = {"appid":appid}
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

@app.route('/search', methods=['GET'])
def search_all():
    search = request.args.get('search')
    filter = request.args.get('filter')

    #TODO: proper master-slave node setup?
    # switch active node depending on selected filter
    if (filter == "all"):
        node = get_master_node()
    else:
        node = get_slave_node(filter)
        
    try_connection(node)

    if not node["engine"]:
        return render_template("error.html", message="Application is not currently connected to the database.")
    else:
        session['node'] = node['id']

    if search:
        query = text("SELECT AppID, name FROM games WHERE AppID = :search OR `name` LIKE :search_like")
        params = {"search": search, "search_like": f"%{search}%"}

        data = fetch_data_from_node(node, query, params)                   
        return render_template("table.html", rows=data, query=search, result_count=len(data), filter=filter)


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