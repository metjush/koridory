import requests
import os
import csv
import unicodedata as UD
import re

# personal API_KEY
API_KEY = # insert api key


def build_query(destination, MUK_lat, MUK_lon):
    """
    Create query that connects to Google Maps Directions API
    Return results from the API endpoint in a JSON format

    :param destination: name of target town (string)
    :param MUK_lat: latitude of destination interchange (float)
    :param MUK_lon: longitute od destination interchange (float)
    :return: query results as JSON
    """
    global API_KEY

    # build query
    latlon = str(MUK_lat) + "," + str(MUK_lon)
    query = "https://maps.googleapis.com/maps/api/directions/json?origin=" + destination + "&destination=" + latlon + "&key=" + API_KEY

    # get results
    r = requests.get(query)
    return r.json()


def query_results(result):
    """
    Process JSON query results
    If the query returns an error message (NOT FOUND, ZERO RESULTS), return an NA as string
    :param result: JSON result from API query
    :return: duration for the given O-D pair, in minutes (float)
    """

    # return duration of the found route in minutes
    if result["status"] in ["ZERO_RESULTS", "NOT_FOUND"]:
        return 'NA'
    try:
        return result["routes"][0]["legs"][0]["duration"]["value"]/60.
    except IndexError:
        return 'NA'


def __clean_string(text, decoder='utf-8-sig'):
    """
    helper function to clean slovak-encoded text

    :param text: weird slovak unicode text
    :return: clean english text
    """
    return re.sub("[^a-zA-Z ]+", "", UD.normalize('NFKD', text.decode(decoder))).encode('ascii')


def load_obce(obce):
    """
    helper function to load a list of all slovak towns and villages

    :param obce: csv filename with towns
    :return: machine readable list of all towns
    """

    # checking if csv file exists
    obce_path = "../data/" + obce + ".csv"
    if not os.path.isfile(obce_path):
        print("Invalid obce file")
        return False

    # read towns into list
    OBCE = []
    with open(obce_path, 'rU') as obce_csv:
        obce_reader = csv.reader(obce_csv, delimiter=';')
        for row in obce_reader:
            OBCE.append(row)
            #print row

    # clean obce
    for row in OBCE:
        row[1] = __clean_string(row[1], 'windows-1250')
        row[2] = __clean_string(row[2], 'windows-1250')
        row[3] = int(row[3])
        #print row

    return OBCE


def test_MUK(MUK_lat, MUK_lon, OBCE, kraje=[]):
    """
    Primary function that computes travel time to a given interchange from all towns
    The set of towns can be limited by region
    Returns a list of travel time results for each town

    :param MUK_lat: interchange latitude (float)
    :param MUK_lon: interchange longitude (float)
    :param OBCE: list of towns/villages (list)
    :param kraje: list of regions to limit search (list)
    :return: list of results
    """
    result = []

    for obec in OBCE:
        if len(kraje) > 0:
            if obec[0] not in kraje:
                continue
        q = build_query(obec[2], MUK_lat, MUK_lon)
        t = query_results(q)
        result.append([obec[2], t])

    return result


def test_koridor(koridor, OBCE, kraje=[]):
    """
    Wrapper function to compute travel time for the full O-D matrix (towns x interchanges)
    Calls test_MUK for each interchange
    Returns a dict of results, where interchange name is the dict key

    :param koridor: name of target motorway to test (string)
    :param OBCE: list of all towns/villages
    :param kraje: list of regions to include in the test (list of strings)
    :return: dict of results
    """

    # checking if csv file exists
    koridor_path = "../data/koridory/" + koridor + ".csv"
    if not os.path.isfile(koridor_path):
        print("Invalid koridory file")
        return False

    # read koridory CSV into a list
    MUKs = []
    with open(koridor_path, 'rU') as koridor_csv:
        muk_reader = csv.reader(koridor_csv, delimiter=';')
        for row in muk_reader:
            MUKs.append(row)
            #print(row)

    # clean MUKs
    for row in MUKs:
        row[0] = __clean_string(row[0])
        row[1] = float(row[1])
        row[2] = float(row[2])
        #print row

    # test each MUK
    koridor_results = {}
    for row in MUKs:
        koridor_results[row[0]] = test_MUK(row[1], row[2], OBCE, kraje)
        print row[0]

    return koridor_results


def save_results(result, filename):
    """
    Helper function to save results of the test_koridor function to a csv file

    :param result: dict of results from test_koridor
    :param filename: desired filename, including the .csv extension (string)
    :return: filename (string)
    """
    # flatten dict to a list
    flattened = []
    for muk in result.keys():
        for obec in result[muk]:
            flattened.append([muk, obec[0], obec[1]])

    with open(filename, 'wb') as FILE:
        lw = csv.writer(FILE, delimiter=";")
        for row in flattened:
            lw.writerow(row)

    return filename


if __name__ == '__main__':
    # loads towns
    OBCE = load_obce("obce")

    R7 = test_koridor('R3juh', OBCE, ['NR','BB'])
    r7save = save_results(R7, 'R3_juh_vysledky.csv')
    print r7save

    R2 = test_koridor('R2_stred', OBCE, ['TN', 'BB', 'NR'])
    r2save = save_results(R2, 'R2_stred_vysledky.csv')
    print r2save



    #R7: Lucenec ako krizovatka zobrat z R2, Detva a Krivan zobrat ako counter z R2
    #R3Turiec: BB, ZA, TN
    #D1Vychod: KE, PO
    #R3 Juh: BB, NR, pouzit z R7: Sahy, Tesmak. Vyiksovat z R1 a R2.
    #R2 stred (TN, BB, NR) Brezolupy, Ruskovce, Chocholna pouzit z R8, Lovcicu a Raztocno pouzit z R3








