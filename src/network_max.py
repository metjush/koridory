import sqlite3
import os
import csv
import unicodedata as UD
import re


def __connect(dbname):
    """

    :param dbname:
    :return: cursor
    """
    conn = sqlite3.connect(dbname)
    return conn, conn.cursor()


def create_tables(dbname):
    """

    :param dbname: name of the sqlite database
    :return:
    """

    # connect to database
    conn, c = __connect(dbname)

    # create NODE table
    c.execute('''
        CREATE TABLE nodes
        (rowid integer PRIMARY KEY, name text, built integer)
        WITHOUT ROWID
        ''')
    # create LINK table
    c.execute('''
        CREATE TABLE links
        (rowid integer PRIMARY KEY, name text, startNode integer, endNode integer, cost real, distance real, built integer)
        WITHOUT ROWID
        ''')
    # create INTERCHANGE table
    # link = 1, node = 2
    c.execute('''
        CREATE TABLE interchanges
        (rowid integer PRIMARY KEY, name text, linkOrNode integer, parent integer)
        WITHOUT ROWID''')
    # create TOWN table
    c.execute('''
        CREATE TABLE towns
        (rowid integer PRIMARY KEY, name text, population integer)
        WITHOUT ROWID''')
    # create OD PAIR table
    c.execute('''
        CREATE TABLE odpairs
        (rowid integer PRIMARY KEY, interchange integer, town integer, time real)
        WITHOUT ROWID''')
    # create HISTORY table
    c.execute('''
        CREATE TABLE history
        (rowid integer PRIMARY KEY, fromNode integer, toNode integer, link integer, newPeople integer)
        WITHOUT ROWID''')

    # commit
    conn.commit()
    conn.close()
    return dbname


def __clean_string(text, decoder='utf-8-sig'):
    """
    helper function to clean slovak-encoded text

    :param text: weird slovak unicode text
    :return: clean english text
    """
    return re.sub("[^a-zA-Z0-9 ]+", "", UD.normalize('NFKC', text.decode(decoder))).encode('ascii')


def __format_csv(row, table):
    """

    :param row:
    :param table:
    :return:
    """
    if table == 'odpairs':
        row[1] = int(row[1])
        row[2] = int(row[2])
        if row[3] == 'NULL':
            row[3] = None
        else:
            row[3] = float(row[3])
        return row
    if table == 'towns':
        row[2] = int(row[2])
        return row
    if table == 'interchanges':
        row[2] = int(row[2])
        row[3] = int(row[3])
        return row
    if table == 'nodes':
        row[2] = int(row[2])
        return row
    if table == 'links':
        row[1] = __clean_string(row[1], 'utf-8')
        row[2] = int(row[2])
        row[3] = int(row[3])
        row[4] = float(row[4])
        row[5] = float(row[5])
        row[6] = int(row[6])
        return row
    return row


def __load_csv(filename, tablename):
    """

    :param filename:
    :param tablename:
    :return:
    """
    # check if exists
    path = '../data/database/' + filename
    if not os.path.isfile(path):
        print("Invalid file")
        return False

    # read into list
    data = []
    with open(path, 'rU') as loaded:
        r = csv.reader(loaded, delimiter=';')
        for row in r:
            # format row
            row[0] = int(row[0]) # rowid
            row = __format_csv(row, tablename)
            data.append(row)

    return data


def __load_table(tablename, data, dbname):
    """

    :param tablename:
    :param data:
    :param dbname:
    :return:
    """
    # connect to database
    conn, c = __connect(dbname)

    # query
    q = "insert into " + tablename + " values(" + ','.join(['?'] * len(data[0])) + ")"
    c.executemany(q, data)
    conn.commit()
    conn.close()
    return tablename


def load_tables(interchanges, odpairs, towns, nodes, links, dbname):
    """

    :param interchanges:
    :param odpairs:
    :param towns:
    :param nodes:
    :param links:
    :param dbname:
    :return:
    """
    # save passed arguments
    parameters = locals()

    # connect to database
    conn, c = __connect(dbname)

    # drop old tables if they exist
    d = ["drop table if exists interchanges",
        "drop table if exists towns",
        "drop table if exists odpairs",
        "drop table if exists nodes",
        "drop table if exists links",
         "drop table if exists history"]
    for q in d:
        c.execute(q)
    conn.commit()
    conn.close()

    # create blank tables
    create_tables(dbname)

    # insert values
    for parameter in parameters.keys():
        if parameter == 'dbname':
            continue
        print __load_table(parameter, parameters[parameter], dbname)

    return dbname


def __built_nodes(dbname):
    """
    helper function to retrieve all built nodes
    :param dbname: database name as string
    :return: list of built node IDs
    """
    # connect to database
    conn, c = __connect(dbname)

    # results
    c.execute("select rowid from nodes where built = 1")
    nodes = c.fetchall()
    nodes = [n[0] for n in nodes]
    conn.close()
    return nodes


def __built_links(dbname):
    """

    :param dbname:
    :return:
    """

    # connect to database
    conn, c = __connect(dbname)

    # results
    c.execute("select rowid from links where built = 1")
    links = c.fetchall()
    links = [i[0] for i in links]
    conn.close()
    return links


def __built_interchanges(built_nodes, built_links, dbname):
    """

    :param built_nodes:
    :param built_links:
    :param dbname:
    :return:
    """

    # connect to database
    conn, c = __connect(dbname)

    # interchanges on built nodes
    # build query
    q = "select rowid from interchanges where linkOrNode = 2 and parent in (" + ','.join(['?'] * len(built_nodes)) + ")"
    c.execute(q, tuple(built_nodes))
    interchanges = c.fetchall()
    interchanges = [i[0] for i in interchanges]

    # interchanges on built links
    # build query
    q = "select rowid from interchanges where linkOrNode = 1 and parent in (" + ','.join(['?'] * len(built_links)) + ")"
    c.execute(q, tuple(built_links))
    i2 = c.fetchall()
    for i in i2:
        interchanges.append(i[0])

    conn.close()
    return interchanges


def __check_node(node_id, dbname):
    """

    :param node_id:
    :param dbname:
    :return:
    """
    # connect to database
    conn, c = __connect(dbname)

    #query
    c.execute("select built from nodes where rowid = ?", node_id)
    built = c.fetchone()[0]
    conn.close()
    return built


def __check_link(link_id, dbname):
    """

    :param link_id:
    :param dbname:
    :return:
    """
    # connect to database
    conn, c = __connect(dbname)

    #query
    c.execute("select built from links where rowid = ?", link_id)
    built = c.fetchone()[0]
    conn.close()
    return built


def __check_interchange(interchange_ids, dbname):
    """
    get all towns that are within distance of interchanges
    :param interchange_ids:
    :param dbname:
    :return:
    """
    # connect to database
    conn, c = __connect(dbname)

    #query
    q = "select distinct town from odpairs where time < 30 and interchange in (" + ','.join(['?'] * len(interchange_ids)) + ")"
    c.execute(q, tuple(interchange_ids))
    towns = c.fetchall()
    towns = [t[0] for t in towns]
    conn.close()
    return towns


def __towns_population(towns, dbname):
    """

    :param towns:
    :param dbname:
    :return:
    """
    # connect to database
    conn, c = __connect(dbname)

    # query
    q = "select sum(population) as pop from towns where rowid in (" + ','.join(['?'] * len(towns)) + ")"
    c.execute(q, tuple(towns))
    pop = c.fetchone()[0]
    conn.close()
    return pop


def towns_on_network(dbname):
    """

    :param dbname:
    :return:
    """

    # get built nodes and links
    built_nodes = __built_nodes(dbname)
    built_links = __built_links(dbname)

    # get built interchanges
    interchanges = __built_interchanges(built_nodes, built_links, dbname)

    # find towns within 30 min
    # connect to database
    conn, c = __connect(dbname)
    # build query
    q = "select distinct town from odpairs where time < 30 and interchange in (" + ','.join(['?'] * len(interchanges)) + ")"
    c.execute(q, tuple(interchanges))
    towns = c.fetchall()
    towns = [t[0] for t in towns]
    conn.close()
    return towns


def new_population(towns, new_link, new_node, dbname):
    """
    compute new population after building new link

    :param towns: set of towns on network
    :param new_link:
    :param new_node:
    :param dbname:
    :return:
    """

    # find all interchanges on new link/node
    q = '''
        select rowid from interchanges where
        (linkOrNode = 1 and parent = ?)
        or
        (linkOrNode = 2 and parent = ?)
        '''
    conn, c = __connect(dbname)
    c.execute(q, (new_link, new_node))
    interchanges = c.fetchall()
    interchanges = [i[0] for i in interchanges]
    conn.close()

    # find towns close to the new interchanges
    new_towns = set(__check_interchange(interchanges, dbname))
    new_towns = new_towns.difference(towns)

    # compute population
    new_pop = __towns_population(new_towns, dbname)
    return new_pop


def __test_one_node(towns, node, dbname):
    """

    :param towns: current towns on network
    :param node: built node id
    :param dbname:
    :return:
    """

    # find all unbuilt links that start or end at the given node
    q = '''
        select rowid, cost, startNode, endNode, name from links
        where
        (startNode = ? or endNode = ?) and
        built = 0
    '''
    conn, c = __connect(dbname)
    c.execute(q, (node, node))
    links = c.fetchall()
    links = [list(l) for l in links]
    conn.close()

    # for each link, test new population
    min_cost = 999
    min_id = -1
    id = 0
    for link in links:
        # new node?
        new_n = link[3]
        if node == link[3]:
            new_n = link[2]
        # new population
        new_pop = new_population(towns, link[0], new_n, dbname)
        if new_pop is None:
            continue
        cost_per_pop = link[1] / new_pop
        link.append(new_pop)
        link.append(cost_per_pop)
        # find lowest cost per population
        if min_cost == 999:
            min_cost = cost_per_pop
            min_id = id
        else:
            min_cost = min(min_cost, cost_per_pop)
            if min_cost == cost_per_pop:
                min_id = id
        id += 1

    # return link that minimizes cost_per_pop
    if min_id == -1:
        return False
    return links[min_id]


def __algorithm_step(towns, nodes, dbname, epsilon):
    """

    :param towns: current towns on network
    :param nodes: built nodes id
    :param dbname:
    :param epsilon:
    :return:
    """

    # iterate over all nodes
    best_links = [0]*len(nodes)
    min_cost = 999
    min_id = -1
    for node_id, node in enumerate(nodes):
        best_links[node_id] = __test_one_node(towns, node, dbname)
        if best_links[node_id] is False:
            continue
        if best_links[node_id][5] < epsilon:
            continue
        if min_cost == 999:
            min_cost = best_links[node_id][6]
            min_id = node_id
        else:
            if min_cost > best_links[node_id][6]:
                min_id = node_id
                min_cost = best_links[node_id][6]

    # return best link to build
    # if all below epsilon
    if min_id == -1:
        return False
    return best_links[min_id], nodes[min_id]


def __write_history(step, link_id, from_node, to_node, new_pop, dbname):
    """

    :param link_id:
    :param from_node:
    :param to_node:
    :param new_pop:
    :param dbname:
    :return:
    """

    conn, c = __connect(dbname)
    # mark link as built
    q1 = '''
        update links
        set built = 1
        where rowid = ?
    '''
    c.execute(q1, (link_id,))
    # write into history
    q2 = '''
        insert into history
        values
        (?, ?, ?, ?, ?)
    '''
    c.execute(q2, (step, from_node, to_node, link_id, new_pop))
    # mark end node as built
    q3 = '''
        update nodes
        set built = 1
        where rowid = ?
    '''
    c.execute(q3, (to_node,))
    # commit
    conn.commit()
    conn.close()
    return link_id


def simple_algorithm(dbname, step, epsilon=1000):
    """

    :param dbname:
    :param epsilon: threshold for new population
    :return:
    """

    ### entry scenario:
    # towns on current network
    towns = towns_on_network(dbname)
    # built nodes
    bnodes = __built_nodes(dbname)

    ### algorithm iteration
    # find best link to build in this step
    result = __algorithm_step(towns, bnodes, dbname, epsilon)
    # check if we had enough
    if not result:
        return False
    best_link, which_node = result[0], result[1]
    # write into history
    print "Building " + best_link[4] + " to get " + str(best_link[5]) + " new people on the network"
    end_node = list(set([best_link[2],best_link[3]]).difference(set([which_node])))[0]
    id = __write_history(step, best_link[0], which_node, end_node, best_link[5], dbname)
    # recurse back
    return simple_algorithm(dbname, step + 1, epsilon)


def start(dbname):
    """

    :param dbname:
    :return:
    """

    # load csv data
    data = {}
    files = ['odpairs', 'interchanges', 'towns', 'links', 'nodes']
    for table in files:
        data[table] = __load_csv(table + '.csv', table)

    # start database
    dbname = load_tables(data['interchanges'], data['odpairs'], data['towns'], data['nodes'], data['links'], dbname)

    result = simple_algorithm(dbname, 1, 500)
    return result


if __name__ == '__main__':
    start('dialnice')







