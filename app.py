from flask import Flask, render_template, redirect, url_for, request, session
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError 
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime, date
import tkinter as tk
from tkinter import messagebox

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
        node['session'] = scoped_session(sessionmaker(bind=node['engine']))
        node['online'] = True

    except Exception as e:
        node["online"] = False
        return render_template("error.html", f"Failed to connect to node {node['id']}: {e}")

# initialize connections to each node
def init_connections(): 
    for node in nodes:
        try_connection(node)


def get_master_node():
    return nodes[0]

def get_slave_node(game_type):
    if game_type == 'windows':
        return nodes[1]
    elif game_type == 'multiplatform':
        return nodes[2]
    else:
        raise ValueError("Invalid game type. Use 'windows' or 'multiplatform'.")

# close all connections
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
@app.route('/write_game', methods=['POST'])
def write_game():

    windows = 1 if request.form.get('windows') else 0
    mac =  1 if request.form.get('mac') else 0
    linux =  1 if request.form.get('linux') else 0

    id_query = text("SELECT AppID FROM games ORDER BY AppID DESC LIMIT 1;")
    new_id = fetch_data_from_node(get_master_node(), id_query)[0][0] + 10

    game = {
        "AppID": new_id,
        "name": request.form.get('name'),
        "release_date": request.form.get('release_date'),
        "price": float(request.form.get('price')), 
        "required_age": int(request.form.get('required_age')),
        "dlc_count": 0,
        "achievements": 0,
        "about_the_game": request.form.get('about_the_game'),
        "windows": windows,
        "mac": mac,
        "linux": linux,
        "peak_ccu": 0,
        "average_playtime_forever": 0,
        "average_playtime_2weeks": 0,
        "median_playtime_forever": 0,
        "median_playtime_2weeks": 0
        }
    

    query = text("""
    INSERT INTO games
    (AppID, name, release_date, price, required_age, dlc_count, achievements, about_the_game, windows, mac, linux, peak_ccu, average_playtime_forever, average_playtime_2weeks, median_playtime_forever, median_playtime_2weeks)
    VALUES (:AppID, :name, :release_date, :price, :required_age, :dlc_count, :achievements, :about_the_game, :windows, :mac, :linux, :peak_ccu, :average_playtime_forever, :average_playtime_2weeks, :median_playtime_forever, :median_playtime_2weeks)
    """)

    master_session = get_master_node()['session']()
    slave_sessions = []

    # Determine slave node to write into
    if windows == 1 and mac == 0 and linux == 0:
        slave_node = get_slave_node("windows")
        slave_sessions.append(slave_node['session']())

    if ((windows == 1) + (mac == 1) + (linux == 1)) >= 2:
        slave_node = get_slave_node("multiplatform")
        slave_sessions.append(slave_node['session']())

    try:
        master_session.begin()

        for slave_session in slave_sessions:
            slave_session.begin()
            slave_session.connection(execution_options={'isolation_level': 'SERIALIZABLE'})

            # start a subtransaction on the slave
            savepoint_slave = slave_session.begin_nested()

            try:
                slave_session.execute(query, game)
                slave_session.commit()  # Commit to the slave first
                print(f"Game added to slave: {slave_session.bind.url}")
            except SQLAlchemyError as e:
                savepoint_slave.rollback()  # Rollback to savepoint if error occurs
                print(f"Error adding game to slave: {e}")
                slave_session.rollback()
                return render_template("error.html", message=f"Error adding game to slave: {e}")

        # Commit to the master only after successful commit to the slave
        master_session.execute(query, game)
        master_session.commit()
        print("Game added to master:", master_session.bind.url)
        messagebox.showinfo("Success", "Game successfully added.")

        # set node for viewing newly added game
        session['node'] = get_master_node()['id']

        return redirect(url_for('view_game', appid=new_id))

    except Exception as e:
        # Rollback if there is an error
        for slave_session in slave_sessions:
            slave_session.rollback()
        master_session.rollback()
        return render_template("error.html", message=f"Error adding game: {e}")

    finally:
        for slave_session in slave_sessions:
            slave_session.close()
        master_session.close()



#  READ TRANSACTION
def fetch_data_from_node(node, query, params=None):
    # this is based on the selected filter on the home page
    Session = node['session']()
    
    if Session:
        with Session.begin():
            
            Session.connection(execution_options={"isolation_level": "READ COMMITTED"})
            try:
                data = Session.execute(query, params).fetchall()
                Session.close()
                return data
            except Exception as e:
                print(f"Error querying node {node['id']}: {e}")
    else:
        print(f"Error in fetching data: Node {node['id']} is not connected.")
        
    return None

# UPDATE TRANSACTION
@app.route('/update_game/<int:appid>', methods=['POST'])
def update_game(appid):

    windows = 1 if request.form.get('windows') else 0
    mac =  1 if request.form.get('mac') else 0
    linux =  1 if request.form.get('linux') else 0

    game = {
        "AppID": appid,
        "name": request.form.get('name'),
        "release_date": request.form.get('release_date'),
        "price": float(request.form.get('price')), 
        "required_age": int(request.form.get('required_age')),
        "dlc_count": 0,
        "achievements": 0,
        "about_the_game": request.form.get('about_the_game'),
        "windows": windows,
        "mac": mac,
        "linux": linux,
        "peak_ccu": 0,
        "average_playtime_forever": 0,
        "average_playtime_2weeks": 0,
        "median_playtime_forever": 0,
        "median_playtime_2weeks": 0
        }
    
    a_query = text("""
    INSERT INTO games
    (AppID, name, release_date, price, required_age, dlc_count, achievements, about_the_game, windows, mac, linux, peak_ccu, average_playtime_forever, average_playtime_2weeks, median_playtime_forever, median_playtime_2weeks)
    VALUES (:AppID, :name, :release_date, :price, :required_age, :dlc_count, :achievements, :about_the_game, :windows, :mac, :linux, :peak_ccu, :average_playtime_forever, :average_playtime_2weeks, :median_playtime_forever, :median_playtime_2weeks)
    """)

    u_query = text("""
        UPDATE games SET
        name = :name,
        release_date = :release_date,
        price = :price,
        required_age = :required_age,
        dlc_count = :dlc_count,
        achievements = :achievements,
        about_the_game = :about_the_game,
        windows = :windows,
        mac = :mac,
        linux = :linux
        WHERE AppID = :AppID
    """)

    d_query = text("DELETE FROM games WHERE AppID = :appid")
    s_query = text("SELECT * FROM games WHERE AppID = :appid")

    master_session = get_master_node()['session']()

    
    try:
        # determine the old and new slave nodes
        old_data = fetch_data_from_node(get_master_node(), s_query, {"appid": appid})[0]
    
        # Determine slave node to write into
        old_nodes = determine_slave_nodes(old_data[8], old_data[9], old_data[10])
        new_nodes = determine_slave_nodes(windows, mac, linux)

        master_session.begin()
        master_session.execute(u_query, game)

        # delete item from old nodes where it shouldn't be in
        for node in old_nodes:
            if node['id'] not in [new_node['id'] for new_node in new_nodes]:
                print("^^^^^^^^^^^^^^^^^^^^^^^^DELETING in node: ", node['id'])
                Session = node['session']()
                Session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
                Session.execute(d_query, {"appid": appid})
                Session.commit()
                Session.close()
        
        # update or create in new nodes
        for node in new_nodes:
            Session = node['session']()
            Session.connection(execution_options={"isolation_level": "SERIALIZABLE"})

            try:
                if  node['id'] not in [old_node['id'] for old_node in old_nodes]:
                    print("^^^^^^^^^^^^^^^^^^^^^^^^ADDING in node: ", node['id'])
                    Session.execute(a_query, game)
                else:
                    print("^^^^^^^^^^^^^^^^^^^^^^^^UPDATING in node: ", node['id'])
                    Session.execute(u_query, game)
                Session.commit()
            except Exception as e:
                # If something fails in new_nodes, we revert changes in old_nodes
                for old_node in old_nodes:
                    old_session = old_node['session']()
                    try:
                        old_session.execute(a_query, game)
                        old_session.commit()
                    except Exception as old_e:
                        old_session.rollback()
                        print(f"Error rolling back old node {old_node['id']}: {old_e}")
                    finally:
                        old_session.close()

                Session.rollback()
                Session.close()
                return render_template("error.html", f"Error updating game in slave node {node['id']}: {str(e)}")
            
            finally:
                Session.close()


        # Commit to the master only after successful commit to the slave
        master_session.commit()
        print("Game updated to master:", master_session.bind.url)
        messagebox.showinfo("Success", "Game successfully updated.")

        # set node for viewing newly updated game
        session['node'] = get_master_node()['id']

        return redirect(url_for('view_game', appid=appid))

    except Exception as e:
        master_session.rollback()
        for node in old_nodes + new_nodes:
            Session = node['session']()
            Session.rollback()
            Session.close()
        return render_template("error.html", message=f"Error updating game: {e}")

    finally:
        master_session.close()

def determine_slave_nodes(windows, mac, linux):
    nodes = []
    if windows == 1 and mac == 0 and linux == 0:
        nodes.append(get_slave_node("windows"))
    if ((windows == 1) + (mac == 1) + (linux == 1)) >= 2:
        nodes.append(get_slave_node("multiplatform"))
    return nodes


# DELETE TRANSACTION
@app.route('/delete_game/<int:appid>')
def delete_game(appid):

    query = text("DELETE FROM games WHERE AppID = :appid")
    params = {"appid":appid}

    master_session = get_master_node()['session']()
    slave_sessions = [get_slave_node("windows")['session'](), get_slave_node("multiplatform")['session']()]

    try:
        master_session.begin()

        for slave_session in slave_sessions:
            slave_session.begin()
            slave_session.connection(execution_options={'isolation_level': 'SERIALIZABLE'})

            # start a subtransaction on the slave
            savepoint_slave = slave_session.begin_nested()

            try:
                slave_session.execute(query, params)
                slave_session.commit()  # Commit to the slave first
                print(f"Game deleted from slave: {slave_session.bind.url}")
            except SQLAlchemyError as e:
                savepoint_slave.rollback()  # Rollback to savepoint if error occurs
                print(f"Error deleting game from slave: {e}")
                slave_session.rollback()
                return render_template("error.html", message=f"Error deleting game from slave: {e}")

        # Commit to the master only after successful commit to the slave
        master_session.execute(query, params)
        master_session.commit()
        print("Game deleted from master:", master_session.bind.url)
        messagebox.showinfo("Success", "Game successfully deleted.")

        # set default node to master node
        session['node'] = get_master_node()['id']

        return redirect(url_for('home'))

    except Exception as e:
        # Rollback if there is an error
        for slave_session in slave_sessions:
            slave_session.rollback()
        master_session.rollback()
        return render_template("error.html", message=f"Error adding game: {e}")

    finally:
        for slave_session in slave_sessions:
            slave_session.close()
        master_session.close()

# ========== WEB ROUTES ==========
@app.route('/')
def home():
    return render_template("all_games.html", filter="all")

@app.route('/new_game')
def new_game():
    return render_template("new_game.html")

@app.route('/view_game/<int:appid>')
def view_game(appid):
    node_id = session.get('node')
    node = next((node for node in nodes if node["id"] == node_id), None)

    query = text("SELECT * FROM games WHERE AppID = :appid")
    params = {"appid":appid}
    data = fetch_data_from_node(node, query, params)
    
    if data:
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
    else:
        return render_template("error.html", message=f"Game with id {appid} not found.")


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
    init_connections()
    app.run(debug=True, port=PORT)

root = tk.Tk()
root.withdraw() 

# clear sessions when app is shutting down
# @app.teardown_appcontext
# def shutdown_session(exception=None):
#     close_connections()
#     session.clear()
#     print("Session cleared and connections closed.")