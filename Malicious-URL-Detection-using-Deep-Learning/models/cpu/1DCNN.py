# This project from incertum's cyber-matrix-ai in GitHub
# https://github.com/incertum/cyber-matrix-ai/tree/master/Malicious-URL-Detection-Deep-Learning

# Load Libraries
import os
import sys

import warnings

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import tensorflow as tf
from datetime import datetime
from keras import backend as K
from keras import regularizers
from keras.layers import Input, ELU, Embedding, BatchNormalization, Convolution1D, concatenate, MaxPooling1D
from keras.layers.core import Dense, Dropout, Lambda
from keras.models import Model
from keras.optimizers import Adam

import data_preprocessor
import model_evaluator
import model_saver

warnings.filterwarnings("ignore")

# 1D Convolution and Fully Connected Layers
def conv_fully(max_len=75, emb_dim=32, max_vocab_len=100, W_reg=regularizers.l2(1e-4)):
    # Input
    main_input = Input(shape=(max_len,), dtype='int32', name='main_input')

    # Embedding layer
    emb = Embedding(input_dim=max_vocab_len, output_dim=emb_dim, input_length=max_len, W_regularizer=W_reg)(main_input)
    emb = Dropout(0.25)(emb)

    def sum_1d(X):
        return K.sum(X, axis=1)

    def get_conv_layer(emb, kernel_size=5, filters=256):
        # Conv layer
        conv = Convolution1D(kernel_size=kernel_size, filters=filters, border_mode='same')(emb)
        conv = MaxPooling1D(5)(conv)
        conv = ELU()(conv)
        conv = Lambda(sum_1d, output_shape=(filters,))(conv)
        conv = Dropout(0.5)(conv)

        return conv

    # Multiple Conv Layers
    conv1 = get_conv_layer(emb, kernel_size=2, filters=256)
    conv2 = get_conv_layer(emb, kernel_size=3, filters=256)
    conv3 = get_conv_layer(emb, kernel_size=4, filters=256)
    conv4 = get_conv_layer(emb, kernel_size=5, filters=256)

    # Fully Connected Layers
    merged = concatenate([conv1, conv2, conv3, conv4], axis=1)

    hidden1 = Dense(1024)(merged)
    hidden1 = ELU()(hidden1)
    hidden1 = BatchNormalization(mode=0)(hidden1)
    hidden1 = Dropout(0.5)(hidden1)

    hidden2 = Dense(1024)(hidden1)
    hidden2 = ELU()(hidden2)
    hidden2 = BatchNormalization(mode=0)(hidden2)
    hidden2 = Dropout(0.5)(hidden2)

    # Output layer (last fully connected layer)
    output = Dense(1, activation='sigmoid', name='main_output')(hidden2)

    # Compile model and define optimizer
    model = Model(input=[main_input], output=[output])
    adam = Adam(lr=1e-4, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
    model.compile(optimizer=adam, loss='binary_crossentropy', metrics=['accuracy', evaluator.recall, evaluator.precision, evaluator.fmeasure])
    return model

# with tf.device("/GPU:0"):

# Keras Session Load
sess = K.get_session()
init = tf.global_variables_initializer()
sess.run(init)

epochs = 10
batch_size = 64

# Load data using model preprocessor
preprocessor = data_preprocessor.Preprocessor()

evaluator = model_evaluator.Evaluator()

''' Simple cross validation '''
X_train, X_test, y_train, y_test = preprocessor.load_data(kfold=False)

model_name = "1DCNN"
model = conv_fully()

dt_start_train = datetime.now()

history = model.fit({'main_input':X_train}, y_train, epochs=epochs, batch_size=batch_size, validation_split=0.11)

dt_end_train = datetime.now()

# Validation curves
evaluator.plot_validation_curves(model_name, history)
evaluator.print_validation_report(history)

# Save confusion matrix
y_pred = model.predict(X_test, batch_size=64)
evaluator.plot_confusion_matrix(model_name, y_test, y_pred, title='Confusion matrix', normalize=True)

# Experimental result
evaluator.calculate_measure(model, X_test, y_test)

result = model.evaluate(X_test, y_test, batch_size=64)
result_dic = dict(zip(model.metrics_names, result))

print('\nAccuracy: {}\n'
      'Precision: {}\nRecall: {}\n F-1Score {}\n'
      .format(result_dic['acc'],
              result_dic['precision'], result_dic['recall'], result_dic['fmeasure']))

# Save trained model
saver = model_saver.Saver()
# saver.saved_model_builder(sess, "cpu", model_name)

# Print Training and predicting time
print('Train time: ' + str((dt_end_train - dt_start_train)))

''' K-fold cross validation '''
# # Use 5-fold cross validation
# accuracy = []
# X, target = preprocessor.load_data(kfold=True)
# kfold = KFold(n_splits=5, shuffle=True, random_state=33)
# for train, validation in kfold.split(X, target):
#     model = conv_fully()
#     model.fit({'main_input':X[train]}, target[train], epochs=epochs, batch_size=batch_size)
#
#     loss, k_accuracy = (model.evaluate(X[validation], target[validation],verbose=1))
#     accuracy.append(k_accuracy)
#
# print('\nK-fold cross validation Accuracy: {}'.format(accuracy))
# print('\nK-fold cross validation Accuracy mean: ', np.array(accuracy).mean())