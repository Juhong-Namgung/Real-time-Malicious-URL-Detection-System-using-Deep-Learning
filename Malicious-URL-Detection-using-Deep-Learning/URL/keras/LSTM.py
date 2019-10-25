# Load Libraries
import pandas as pd
import numpy as np
import os
from string import printable
from sklearn import model_selection

# import gensim
import tensorflow as tf
from keras.models import Model, model_from_json
from keras import regularizers
from keras.layers.core import Dense, Dropout
from keras.layers import Input, LSTM, Embedding
from keras.preprocessing import sequence
from keras.optimizers import Adam
from keras import backend as K
from sklearn.model_selection import KFold

from pathlib import Path
import json
from keras.utils.vis_utils import model_to_dot
from IPython.display import SVG
from keras.utils import plot_model


import warnings
warnings.filterwarnings("ignore")

## Load data URL
DATA_HOME ='../../../data/'
df = pd.read_csv(DATA_HOME + 'urls.csv',encoding='ISO-8859-1', sep=';')


# Initial Data Preparation URL

# Step 1: Convert raw URL string in list of lists where characters that are contained in "printable" are sotred encoded as integer
url_int_tokens = [[printable.index(x) + 1 for x in url if x in printable] for url in df.url]

# Step 2: Cut URL string at max_len or pad with zeros if shorter
max_len = 75
X = sequence.pad_sequences(url_int_tokens, maxlen=max_len)

# Step 3: Extract labels form df to nupy array
target = np.array(df.label)

# Simple Cross-Validation: Split the data set into training and test data
X_train, X_test, target_train, target_test = model_selection.train_test_split(X, target, test_size=0.25, random_state=33)

# General get layer dimensions for any model
def print_layers_dims(model):
    l_layers = model.layers
    # Note None is ALWAYS batch_size
    for i in range(len(l_layers)):
        print(l_layers[i])
        print('Input Shape: ' , l_layers[i].input_shape, 'Output Shpae: ', l_layers[i].output_shape)

# General save model to disk function
def save_model(fileModelJSON, fileWeights):
    if Path(fileModelJSON).is_file():
        os.remove(fileModelJSON)
    json_string = model.to_json()
    with open(fileModelJSON, 'w') as f:
        json.dump(json_string, f)
    if Path(fileWeights).is_file():
        os.remove(fileWeights)
    model.save_weights(fileWeights)

# General load model from disk function
def load_model(fileModelJSON, fileWeights):
    with open(fileModelJSON, 'r') as f:
        model_json = json.load(f)
        model = model_from_json(model_json)

    model.load_weights(fileWeights)
    return model

# 1D Convolution and Fully Connected Layers
def simple_lstm(max_len=75, emb_dim=32, max_vocab_len=100, lstm_output_size=32, W_reg=regularizers.l2(1e-4)):
    # Input
    main_input = Input(shape=(max_len,), dtype='int32', name='main_input')
    # Embedding layer
    emb = Embedding(input_dim=max_vocab_len, output_dim=emb_dim, input_length=max_len,
                    dropout=0.2, W_regularizer=W_reg)(main_input)

    # LSTM layer
    lstm = LSTM(lstm_output_size)(emb)
    lstm = Dropout(0.5)(lstm)

    # Output layer (last fully connected layer)
    output = Dense(1, activation='sigmoid', name='output')(lstm)

    # Compile model and define optimizer
    model = Model(input=[main_input], output=[output])
    adam = Adam(lr=1e-4, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
    model.compile(optimizer=adam, loss='binary_crossentropy', metrics=['accuracy'])
    return model


def freeze_session(session, keep_var_names=None, output_names="main_output", clear_devices=True):
    """
    Freezes the state of a session into a pruned computation graph.

    Creates a new computation graph where variable nodes are replaced by
    constants taking their current value in the session. The new graph will be
    pruned so subgraphs that are not necessary to compute the requested
    outputs are removed.
    @param session The TensorFlow session to be frozen.
    @param keep_var_names A list of variable names that should not be frozen,
                          or None to freeze all the variables in the graph.
    @param output_names Names of the relevant graph outputs.
    @param clear_devices Remove the device directives from the graph for better portability.
    @return The frozen graph definition.
    """
    graph = session.graph
    with graph.as_default():
        freeze_var_names = list(set(v.op.name for v in tf.global_variables()).difference(keep_var_names or []))
        output_names = output_names or []
        output_names += [v.op.name for v in tf.global_variables()]
        input_graph_def = graph.as_graph_def()
        if clear_devices:
            for node in input_graph_def.node:
                node.device = ""
        frozen_graph = tf.graph_util.convert_variables_to_constants(
            session, input_graph_def, output_names, freeze_var_names)
        return frozen_graph

def save_model(session, input_tensor, output_tensor):
    signature = tf.saved_model.signature_def_utils.build_signature_def(
        inputs = {'input': tf.saved_model.utils.build_tensor_info(input_tensor)},
        outputs = {'output': tf.saved_model.utils.build_tensor_info(output_tensor)},
    )
    b = tf.saved_model.builder.SavedModelBuilder('./pptmodel')
    b.add_meta_graph_and_variables(session,
                                   [tf.saved_model.tag_constants.SERVING],
                                   signature_def_map={tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: signature})
    b.save()


# Fit model and 5-fold Cross-Validation, 3 CONV + Fully Connected
epochs = 5
batch_size = 32

kfold = KFold(n_splits=5, shuffle=True, random_state=33)
accuracy = []

for train, validation in kfold.split(X, target):
    model = simple_lstm()
    model.fit({'main_input':X[train]}, target[train], epochs=epochs, batch_size=batch_size)

    loss, k_accuracy = (model.evaluate(X[validation], target[validation],verbose=1))
    accuracy.append(k_accuracy)

print('\nK-fold cross validation Accuracy: {}'.format(accuracy))
print('\nK-fold cross validation Accuracy mean: ', np.array(accuracy).mean())

frozen_graph = freeze_session(K.get_session(), output_names=[out.op.name for out in model.outputs])

tf.train.write_graph(frozen_graph, "../../../models/", "keras_LSTM.pb", as_text=False)

builder = tf.saved_model.builder.SavedModelBuilder("../../../models/LSTM/")
builder.add_meta_graph_and_variables(K.get_session(),[tf.saved_model.tag_constants.SERVING],main_op=tf.global_variables_initializer())
#builder.add_meta_graph_and_variables(K.get_session(),[tf.saved_model.tag_constants.SERVING],main_op=tf.local_variables_initializer())
builder.save(False)

# model = simple_lstm()
#
# SVG(model_to_dot(model, show_shapes=True).create(prog='dot',format='svg'))
# plot_model(model,to_file='./LSTM.png',show_shapes=False,show_layer_names=True)

# model.fit(X_train, target_train, epochs=epochs, batch_size=batch_size)
# loss, accuracy, _precision, _recall, _f1score = model.evaluate(X_test, target_test, verbose=1)
# print('\nAccuracy: {}, Precision: {}, Recall: {}, F-1 Score {}'.format(accuracy,_precision, _recall, _f1score))




