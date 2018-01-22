from pyspark import SparkContext
from pyspark.sql import SQLContext
from numpy.random import choice
import os, errno

# initialisation
sc = SparkContext(appName="Select 200 Random Tweets Per Party Over Time Period")
sc.setLogLevel('ERROR')

sqlc = SQLContext(sc)

# real dataframe, two weeks before election
dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/03/{0[0-9],1[0-4]}/*/*")

# real dataframe, poll 4 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/02/{1[5-9],2[0-8]}/*/*")

# real dataframe, poll 3 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/02/{0[0-9],1[0-4]}/*/*")

# real dataframe, poll 2 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/{1[5-9],2[0-8]}/*/*")

# real dataframe, poll 1 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/{0[0-9],1[0-4]}/*/*")

# test dataframe
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/01/*/*")

#
# Dictionary mapping the keywords to the party they belong to.
#
KEYWORDS = {
    "Volkspartij voor Vrijheid en Democratie": 'vvd',
    "VVD": 'vvd',
    "Mark Rutte": 'vvd',
    "Rutte": 'vvd',
    "Klaas Dijkhoff": 'vvd',
    "markrutte": 'vvd',
    "dijkhoff": 'vvd',
    "Partij voor de Vrijheid": 'pvv',
    "PVV": 'pvv',
    "Geert Wilders": 'pvv',
    "Wilders": 'pvv',
    "geertwilderspvv": 'pvv',
    "Christen-Democratish Appel": 'cda',
    "CDA": 'cda',
    "Sybrand van Haersma Buma": 'cda',
    "Sybrand Buma": 'cda',
    "Buma": 'cda',
    "cdavandaag": 'cda',
    "sybrandbuma": 'cda',
    "Democraten 66": 'd66',
    "D66": 'd66',
    "Alexander Pechtold": 'd66',
    "Pechtold": 'd66',
    "d66": 'd66',
    "APechtold": 'd66',
    "GroenLinks": 'gl',
    "GL": 'gl',
    "Jesse Klaver": 'gl',
    "Klaver": 'gl',
    "groenlinks": 'gl',
    "jesseklaver": 'gl',
    "Socialistische Partij": 'sp',
    "SP": 'sp',
    "Emile Roemer": 'sp',
    "Roemer": 'sp',
    "Lilian Marijnissen": 'sp',
    "SPnl": 'sp',
    "emileroemer": 'sp',
    "MarijnissenL": 'sp',
    "Partij van de Arbeid": 'pvda',
    "PvdA": 'pvda',
    "Lodewijk Asscher": 'pvda',
    "Asscher": 'pvda',
    "LodewijkA": 'pvda',
    "ChristenUnie": 'cu',
    "CU": 'cu',
    "Gert-Jan Segers": 'cu',
    "Segers": 'cu',
    "christenunie": 'cu',
    "gertjansegers": 'cu',
    "Partij voor de Dieren": 'pvdd',
    "PvdD": 'pvdd',
    "Marianne Thieme": 'pvdd',
    "Thieme": 'pvdd',
    "PartijvdDieren": 'pvdd',
    "mariannethieme": 'pvdd',
    "50Plus": '50p',
    "Henk Krol": '50p',
    "Krol": '50p',
    "50pluspartij": '50p',
    "HenkKrol": '50p',
    "Staatkundig Gereformeerde Partij": 'sgp',
    "SGP": 'sgp',
    "Kees van der Staaij": 'sgp',
    "Staaij": 'sgp',
    "SGPnieuws": 'sgp',
    "keesvdstaaij": 'sgp',
    "Denk": 'denk',
    "Tunahan Kuzu": 'denk',
    "Kuzu": 'denk',
    "DenkNL": 'denk',
    "tunahankuzu": 'denk',
    "Forum voor Democratie": 'fvd',
    "FvD": 'fvd',
    "Thierry Baudet": 'fvd',
    "Baudet": 'fvd',
    "fvdemocratie": 'fvd',
    "thierrybaudet": 'fvd',
}

#
# List of Twitter handles (without the @) that we exclude. Includes parties own handles and major news outlets.
#
EXCLUDE_HANDLES = ['VVD', 'markrutte', 'dijkhoff', 'geertwilderspvv', 'cdavandaag', 'sybrandbuma', 'd66',
                   'APechtold', 'groenlinks', 'jesseklaver', 'SPnl', 'emileroemer', 'MarijnissenL', 'PvdA',
                   'LodewijkA', 'christenunie', 'gertjansegers', 'PartijvdDieren', 'mariannethieme',
                   '50pluspartij', 'HenkKrol', 'SGPnieuws', 'keesvdstaaij', 'DenkNL', 'tunahankuzu',
                   'fvdemocratie', 'thierrybaudet', 'NUnl', 'RTLnieuws', 'NPOpolitiek', 'MarioGibbels',
                   'nieuwsuittwente', 'telegraaf', 'NOS', 'ADnl', 'FD_Nieuws', 'financieelblad', 'Metro', 'ndnl', 'nrc',
                   'NRC_commentaar', 'refdag', 'trouw', 'volkskrant', 'BarneveldseKrnt', 'BNDeStem', 'brabantsdagblad',
                   'delimburger', 'dvhn_nl', 'ED_Eindhoven', 'ED_Regio', 'frieschdagblad', 'DeGelderlander',
                   'gooieneemlander', 'hdhaarlem', 'lc_nl', 'leidschdagblad', 'nhdagblad', 'parool', 'pzcredactie',
                   'De_Stentor', 'tubantia', 'foknieuws', 'quotevanvandaag', 'CoronelKart', 'Gemeente', 'Politiek',
                   '2eKamertweets']


#
# method mapping a tweet to a party based on keywords and excluded handles
#
def mapTweetToParty(tweet):
    # parse a tweet as a dictionary
    tweet = tweet.asDict()
    # encode the tweet text as utf8 (Tweets are utf8)
    text = tweet['text'].encode('utf8')

    # check if we should exclude handle
    screen_name = tweet['screen_name']
    if screen_name in EXCLUDE_HANDLES:
        return (None, text)

    # remove hashtags and twitter handle symbols from text, convert to lower case and split text in words
    words = text.replace('@', '').replace('#', '').lower().split()
    # establish an empty set of all parties matched, a set so duplicate parties are already filtered
    parties = set()

    # iterate over every keyword
    for keyword, party in KEYWORDS.items():
        # check if keyword occurs in the sentence
        if keyword.lower() in words:
            # if it occurs, the tweet matches that keyword's party
            parties.add(party)

    # if a tweet does not match exactly one party, we don't count it as a vote
    if len(parties) != 1:
        return (None, text)
    # else we return the party the tweet matches
    else:
        return (parties.pop(), text)


#
# below are the helper functions we need for combineByKey
#

# this is the createCombiner function
# it creates a 'single value' combiner (in our case a set with one item) from a single value
def create_combiner(value):
    return [value]


# this is the mergeValue function
# it merges a single value into an existing combiner
# in our case, we add the value to an existing set
def merge_value(combiner, value):
    combiner.append(value)
    return combiner


# this is the mergeCombiners function
# it merges two combiners into one combiner
# in our case, we just need to combine two sets into one
def merge_combiners(combiner1, combiner2):
    return combiner1 + combiner2


# where => filter so that we only have Dutch tweets remaining
# select => select, from a tweet, only the text and the screen name of the tweeter
# map => for every tweet that is now left, determine what party it matches
# filter => filter out all tweets that have 'None' as a party (ie. no match, or multiple matches)
# combineByKey => combine all seperate tweets into one key-value pair: (party, [list of tweet texts])
tweets = dataFrame.where(dataFrame.lang == 'nl').select('text', 'user.screen_name').rdd \
    .map(mapTweetToParty) \
    .filter(lambda tuple: tuple[0] is not None) \
    .combineByKey(create_combiner, merge_value, merge_combiners)

# number of tweets per party we wish to extract
N = 200

# for every party, save the tweets
for party, tweets in tweets.collect():
    # to this filename
    filename = "/home/s1220535/gone-with-polls/200tweets/{}/{}.txt".format('pre-election', party)

    # check if folder exists
    # from: https://stackoverflow.com/a/12517490
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    # write file for party
    f = open(filename, 'w+')
    # write every tweet on one line (remove any newlines from the tweets)
    # should result in a file of max. 200 lines, every line containing exactly one tweet about that party
    f.writelines(map(lambda t: t.replace('\n', '').replace('\r', '') + '\n',
                     choice(tweets, size=min(N, len(tweets)), replace=False).tolist()))
    f.close()
