# Load Libraries

from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from keras import backend as K
from sklearn import metrics
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix

class Evaluator:
    def __init__(selfs):
        pass

    # save validation curves(.png format)
    def plot_validation_curves(self, model_name, history):
        history_dict = history.history
        print(history_dict.keys())

        # validation curves
        epochs = range(1, len(history_dict['loss']) + 1)
        # "bo" is for "blue dot"
        plt.plot(epochs, history_dict['val_fmeasure'], 'r',label='f1')
        plt.plot(epochs, history_dict['val_precision'], 'g',label='precision')
        plt.plot(epochs, history_dict['val_recall'], 'k',label='recall')
        plt.plot(epochs, history_dict['val_acc'], 'c', label='accuracy')

        plt.xlabel('Epochs')
        plt.grid()
        plt.legend(loc='lower right')
        #plt.show()
        now = datetime.now()
        nowDatetime = now.strftime('%Y_%m_%d-%H%M%S')

        plt.savefig('../../output/validation_curves/' + model_name + '_val_curve_' + nowDatetime + '.png')

    # print validation
    def print_validation_report(self, history):
        history_dict = history.history

        for key in history_dict:
            if "val" in key:
                print('[' + key + '] '+ str(history_dict[key]))

    # Calculate measure(categorical accuracy, precision, recall, f1-score)
    def calculate_measure(self, model, X_test, y_test):
        y_pred_class_prob = model.predict(X_test, batch_size=64)
        y_pred_class = np.argmax(y_pred_class_prob, axis=1)
        y_true_class = np.array(y_test)

        # classification report(sklearn)
        print(classification_report(y_true_class, y_pred_class, digits=4))

        print ("precision" , metrics.precision_score(y_true_class, y_pred_class, average = 'weighted'))
        print ("recall" , metrics.recall_score(y_true_class, y_pred_class, average = 'weighted'))
        print ("f1" , metrics.f1_score(y_true_class, y_pred_class, average = 'weighted'))

    # save confusion matrix(.png)
    def plot_confusion_matrix(self, model_name, y_true, y_pred,
                              classes=[0, 1],
                              normalize=False,
                              title=None,
                              cmap=plt.cm.Blues):
        dga_labels_dict = {'benign':0, 'malicious':1 }
        classes_str = []
        for i in classes:
            for dga_str, dga_int in dga_labels_dict.items():
                if dga_int == i:
                    classes_str.append(dga_str)

        y_pred_class = np.argmax(y_pred, axis=1)
        y_true_class = np.array(y_true)

        """
        This function prints and plots the confusion matrix.
        Normalization can be applied by setting `normalize=True`.
        """
        if not title:
            if normalize:
                title = 'Normalized confusion matrix'
            else:
                title = 'Confusion matrix, without normalization'

        # Compute confusion matrix
        cm = confusion_matrix(y_true_class, y_pred_class)
        accuracy = np.trace(cm) / float(np.sum(cm))
        misclass = 1 - accuracy
        # Only use the labels that appear in the data
        #classes = list(classes[unique_labels(y_true, y_pred)])
        if normalize:
            cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
            print("Normalized confusion matrix")
        else:
            print('Confusion matrix, without normalization')

        fig, ax = plt.subplots()
        im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
        ax.figure.colorbar(im, ax=ax)
        # We want to show all ticks...
        ax.set(xticks=np.arange(cm.shape[1]),
               yticks=np.arange(cm.shape[0]),
               # ... and label them with the respective list entries
               xticklabels=classes_str, yticklabels=classes_str,
               title=title,
               ylabel='True label',
               xlabel='Predicted label')

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                 rotation_mode="anchor")

        # Loop over data dimensions and create text annotations.
        fmt = '.3f' if normalize else 'd'
        thresh = cm.max() / 2.
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, format(cm[i, j], fmt),
                        ha="center", va="center",
                        color="white" if cm[i, j] > thresh else "black")
        fig.tight_layout()

        precision = metrics.precision_score(y_true_class, y_pred_class, average = 'weighted')
        recall = metrics.recall_score(y_true_class, y_pred_class, average = 'weighted')
        f1 = metrics.f1_score(y_true_class, y_pred_class, average = 'weighted')

        plt.xlabel('Predicted label\naccuracy={:0.4f}; precision={:0.4f}; recall={:0.4f}; f1={:0.4f}; misclass={:0.4f}'
                   .format(accuracy, precision, recall, f1, misclass))
        now = datetime.now()
        nowDatetime = now.strftime('%Y_%m_%d-%H%M%S')
        figure = plt.gcf()
        figure.set_size_inches(15, 15)
        plt.savefig('../../output/confusion_matrix/' + model_name + '_confusion_matrix_' + nowDatetime + '.png', dpi=100)

    def precision(self, y_true, y_pred):
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
        precision = true_positives / (predicted_positives + K.epsilon())
        return precision

    def recall(self, y_true, y_pred):
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
        recall = true_positives / (possible_positives + K.epsilon())
        return recall

    def fbeta_score(self, y_true, y_pred, beta=1):
        if beta < 0:
            raise ValueError('The lowest choosable beta is zero (only precision).')

            # If there are no true positives, fix the F score at 0 like sklearn.
        if K.sum(K.round(K.clip(y_true, 0, 1))) == 0:

            return 0
        p = Evaluator.precision(self, y_true, y_pred)
        r = Evaluator.recall(self, y_true, y_pred)
        bb = beta ** 2
        fbeta_score = (1 + bb) * (p * r) / (bb * p + r + K.epsilon())
        return fbeta_score

    def fmeasure(self, y_true, y_pred):
        return Evaluator.fbeta_score(self, y_true, y_pred, beta=1)