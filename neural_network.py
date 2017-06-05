
import sys
import urllib
import sqlite3

import numpy as np
import tensorflow as tf

tf.logging.set_verbosity(tf.logging.INFO)

TRAIN_BATCH = 500000
EVALUATE_BATCH = 10000

def get_train_inputs(games, offset = 0):
    return get_inputs('train', games, offset)

def get_test_inputs(games, offset = 0):
    return get_inputs('test', games, offset)

def get_predictions_inputs(games, offset = 0):
    x, _ = get_inputs('test', games, offset)
    return x


def get_inputs(region, games, offset = 0):

    conn = sqlite3.connect('lol-' + region + '.db')
    c = conn.cursor()
    query = "SELECT * FROM matchs ORDER BY gameId LIMIT " + str(games)
    if offset > 0:
        query += " OFFSET " + str(offset)

    matchs = c.execute(query)

    players = []
    champs = []
    labels = []
    n = 0

    for i in range(0, 10):
        players.append([])
        champs.append([])

    for m in matchs:
        n += 1
        for i in range(0, 10):
            champs[i].append(str(m[i + 1]))
            players[i].append(m[i + 11])
        labels.append(m[21]/200)

    print(str(n) + " matchs for this batch")



    x = {}

    for i in range(0, 10):
        x['champ' + str(i)] = tf.SparseTensor(indices= [[j, 0] for j in range(0, n)], values= champs[i], dense_shape= [n, 500])
        x['player' + str(i)] = tf.constant(players[i], shape=[n, 1])


    y = tf.constant(labels)

    print("feature columns and labels ready")
    # print(x)
    # print(y)

    return x, y

def classifier_construction():
    # Specify that all features
    feature_columns = []
    for i in range(0, 10):
        champ = tf.contrib.layers.sparse_column_with_hash_bucket("champ" + str(i), hash_bucket_size=500)
        feature_columns.append(tf.contrib.layers.embedding_column(champ, dimension=8))
        feature_columns.append(tf.contrib.layers.real_valued_column("player" + str(i)))

    # Build 3 layer DNN with 64, 32, 16 units respectively.
    classifier = tf.contrib.learn.DNNClassifier(feature_columns=feature_columns,
                                                hidden_units=[64, 32, 16],
                                                n_classes=2,
                                                model_dir="/tmp/lol_model5")

    return classifier

def main():

    classifier = classifier_construction()

    print ("Classifier ready")

    conn = sqlite3.connect('lol-train.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM matchs")
    total_train = c.fetchone()[0]

    # Fit model.

    iterations = total_train/TRAIN_BATCH
    if total_train % TRAIN_BATCH != 0:
        iterations += 1

    probas = []

    for i in range(0, iterations):
        classifier.fit(input_fn=lambda: get_train_inputs(TRAIN_BATCH,i*TRAIN_BATCH ), steps = 5000)

        # Define the test inputs


        # Evaluate accuracy.
        accuracy_score = classifier.evaluate(input_fn=lambda: get_test_inputs(EVALUATE_BATCH, i* EVALUATE_BATCH),
                                             steps=1)["accuracy"]

        print("\nTest Accuracy: {0:f}\n".format(accuracy_score))

        probas = proba_pedictions(probas, classifier)
        # predictions = list(classifier.predict_proba(input_fn=lambda : get_predictions_inputs(100)))
        #
        # #print (predictions)
        #
        # probasAtThisStep = map(max, predictions)
        # print (probasAtThisStep)
        # if not probas:
        #     probas = map(list,probasAtThisStep)
        # else:
        #     for j in range(0, probas.__len__()):
        #         probas[j].append(probasAtThisStep[j])

    for p in probas:
        print(p)

    weighted_accuracy(classifier, iterations*EVALUATE_BATCH)


def weighted_accuracy(classifier, lowerBound):
    predictions = list(classifier.predict_proba(input_fn=lambda: get_predictions_inputs(100)))


def proba_pedictions(probas, classifier):


    predictions = list(classifier.predict_proba(input_fn=lambda: get_predictions_inputs(100)))

    probasAtThisStep = map(max, predictions)
    print (probasAtThisStep)

    if not probas:
        for x in probasAtThisStep:
            probas.append([x])
    else:
        for j in range(0, probas.__len__()):
            probas[j].append(probasAtThisStep[j])

    return probas

def test():

    print ("Test")
    probas = []
    probas = proba_pedictions(probas, classifier_construction())
    probas = proba_pedictions(probas, classifier_construction())
    print (probas)


if __name__ == "__main__":
    if sys.argv.__len__() == 2 and (sys.argv[1] == "test"):
        test()
    else:
        main()
