
import sys
import urllib
import sqlite3

import numpy as np
import random

import time
import print_functions as pf
import tensorflow as tf

tf.logging.set_verbosity(tf.logging.INFO)

TRAIN_BATCH = 500000
EVALUATE_BATCH = 10000
TEST_BATCH = 200000
FULL_TEST_BATCH = 1000000

shuffle_index = 0

def get_train_inputs(games, offset = 0):
    return get_inputs('train', games, offset)

def get_test_inputs(games, offset = 0):
    return get_inputs('test', games, offset)

def get_predictions_inputs(games, offset = 0):
    return get_inputs('test', games, offset, rawLabels=True)


def get_inputs(region, games, offset = 0, rawLabels = False):

    global shuffle_index

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

    conn.close()

    if shuffle_index != 0:
        newChamps, newPlayers = team_shuffle(champs, players, shuffle_index)
        champs = newChamps
        players = newPlayers



    x = {}

    for i in range(0, 10):
        x['champ' + str(i)] = tf.SparseTensor(indices= [[j, 0] for j in range(0, n)], values= champs[i], dense_shape= [n, 500])
        x['player' + str(i)] = tf.constant(players[i], shape=[n, 1])


    y = tf.constant(labels)

    print("feature columns and labels ready")
    # print(x)
    # print(y)
    if rawLabels:
        return x, labels

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
                                                model_dir="./dnn/lol_model")

    return classifier

def train(shuffleOrder = 0, shuffling = 1):

    global shuffle_index

    shuffle_index = shuffleOrder
    TRAINING_STEPS = 3000

    startTime = time.time()

    classifier = classifier_construction()

    print ("Classifier ready")

    conn = sqlite3.connect('lol-train.db')
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM matchs")
    total_train = c.fetchone()[0]

    conn.close()
    # Fit model.

    iterations = total_train/TRAIN_BATCH
    if total_train % TRAIN_BATCH != 0:
        iterations += 1

    probas = []

    for j in range(0, shuffling):

        if shuffle_index > 0:
            TRAINING_STEPS = 2000

        for i in range(0, iterations):
            classifier.fit(input_fn=lambda: get_train_inputs(TRAIN_BATCH,i*TRAIN_BATCH ), steps = TRAINING_STEPS)

            # Define the test inputs


            # Evaluate accuracy.
            accuracy_score = classifier.evaluate(input_fn=lambda: get_test_inputs(EVALUATE_BATCH, i* EVALUATE_BATCH),
                                                 steps=1)["accuracy"]

            print("\nTest Accuracy: {0:f}\n".format(accuracy_score))

            probas = proba_pedictions(probas, classifier)

        weighted_accuracy(classifier, iterations*EVALUATE_BATCH)

        shuffle_index += 1

    for p in probas:
        print(p)
    print (pf.big_statement("Training on {} examples done in {}".format((shuffling*total_train), pf.time_format(time.time() - startTime))))




def weighted_accuracy(classifier, lowerBound= 0, intermediateResult= 0, probas_correct_label= None):

    if not probas_correct_label:
        probas_correct_label = raw_evaluation(classifier, lowerBound)


    total = 0
    n = 0
    score = 0
    scoreTotal = 0

    for p in probas_correct_label:
        n += 1
        #proba of the correct response
        if p > 0.5:
            #Correct guess
            score += p
        else:
            #Wrong guess
            score -= p

        if intermediateResult != 0 and n % intermediateResult == 0:
            print "Current score : " + str(score/n)
            scoreTotal += score
            total += n
            n = 0
            score = 0

    scoreTotal += score
    total += n
    print "Total score : " + str(scoreTotal/total)

    return "Weighted accuracy : {0:f}".format(scoreTotal/total)

def certainty_accuracy(classifier, probas_bounds= [0.6], lowerBound = 0, probas_correct_label= None):

    if not probas_correct_label:
        probas_correct_label = raw_evaluation(classifier, lowerBound)

    total = 0
    matchingPredictions = []
    score = []
    output = []


    for i in range(probas_bounds.__len__()):
        matchingPredictions.append(0)
        score.append(0)
        output.append('')

    for p in probas_correct_label:
        total += 1

        for i in range(0, probas_bounds.__len__()):
            if p > probas_bounds[i]:
                matchingPredictions[i] += 1
                score[i] += 1
            if p < (1 - probas_bounds[i]):
                matchingPredictions[i] +=1


    for i in range(0, probas_bounds.__len__()):
        if matchingPredictions[i] != 0:
            accuracy = float(score[i]) / matchingPredictions[i]
        else:
            accuracy = 'undefined'
        percentageExamples = float(matchingPredictions[i])*100/total
        output[i] = "Accuracy for proba > {} : {}, {} examples out of {} ({}%)".format(
            probas_bounds[i], accuracy, matchingPredictions[i], total, percentageExamples)

    return output

def raw_evaluation(classifier, lowerBound= 0):

    conn = sqlite3.connect('lol-test.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM matchs")
    total_test = c.fetchone()[0]
    iterations = (total_test - lowerBound)/TEST_BATCH
    if (total_test - lowerBound)% TEST_BATCH != 0:
        iterations += 1


    probas_correct_label = []

    for i in range(0, iterations):

        # TODO: WHY ? Why need to it twice ?????
        _,labels = get_predictions_inputs(TEST_BATCH, lowerBound + i *TEST_BATCH)

        predictions = list(classifier.predict_proba(input_fn=lambda : get_predictions_inputs(TEST_BATCH, lowerBound + i *TEST_BATCH)[0]))

        for p, l in zip(predictions, labels):
            #proba of the correct response
            probas_correct_label.append(p[l])

    return probas_correct_label


def evaluate_model():

    startTime = time.time()

    classifier = classifier_construction()

    probas_correct_label = raw_evaluation(classifier, 0)

    weighted_accuracy_score = weighted_accuracy(classifier, probas_correct_label= probas_correct_label)
    certainty_accuracy_scores = certainty_accuracy(classifier, probas_bounds=[0.55,0.60,0.65, 0.7, 0.75, 0.8, 0.85], probas_correct_label= probas_correct_label)


    #TODO : improve

    evaluation = classifier.evaluate(input_fn=lambda: get_test_inputs(FULL_TEST_BATCH, 0), steps=1)

    accuracy_score = "Accuracy : {0:f}".format(evaluation['accuracy'])

    print("\nTest Accuracy: {0:f}\n".format(evaluation['accuracy']))

    timeDescription = "Evaluation done in {}".format(pf.time_format(time.time() - startTime))

    fullEvaluation = [accuracy_score, weighted_accuracy_score]
    fullEvaluation.extend(certainty_accuracy_scores)
    fullEvaluation.append(timeDescription)

    print (pf.big_statement('\n'.join(fullEvaluation)))


def proba_pedictions(probas, classifier):


    predictions = list(classifier.predict_proba(input_fn=lambda: get_predictions_inputs(100)))

    probasAtThisStep = [item[0] for item in predictions]
    print (probasAtThisStep)

    if not probas:
        for x in probasAtThisStep:
            probas.append([x])
    else:
        for j in range(0, probas.__len__()):
            probas[j].append(probasAtThisStep[j])

    return probas

def team_shuffle_indexes(n):

    indexes = [i for i in range(0, 5)]
    random.seed(5)
    for i in range (0, n):
        random.shuffle(indexes)

    return indexes

def team_shuffle(champs, players, n):

    indexes = team_shuffle_indexes(n)

    newChamps = [[] for i in range(10)]
    newPlayers = [[] for i in range(10)]

    for i in range(0, 5):
        newChamps[indexes[i]] = champs[i]
        newChamps[indexes[i] + 5] = champs[i + 5]
        newPlayers[indexes[i]] = players[i]
        newPlayers[indexes[i] + 5] = players[i + 5]

    return (newChamps, newPlayers)

def test():

    champs = [[i] for i in range(1, 11)]
    players = [[i] for i in range(10,20)]

    print team_shuffle_indexes(2)

    newChamps, newPlayers = team_shuffle(champs, players, 2)

    print champs
    print newChamps
    print '\n'
    print players
    print newPlayers




if __name__ == "__main__":
    if sys.argv.__len__() >= 2:
        if (sys.argv[1] == "test"):
            test()
        elif (sys.argv[1] == "evaluate"):
            evaluate_model()
        elif (sys.argv[1] == "train"):
            if sys.argv.__len__() == 4:
                train(shuffleOrder=int(sys.argv[2]), shuffling=int(sys.argv[3]))
            elif sys.argv.__len__() == 3:
                train(shuffleOrder=int(sys.argv[2]))
            else:
                train()
        else:
            print("neural_network <test|evaluate|train> [shuffleIndex] [shuffling]")
    else:
        print("Missing arguments !\nneural_network <test|evaluate|train> [shuffleIndex] [shuffling]")
