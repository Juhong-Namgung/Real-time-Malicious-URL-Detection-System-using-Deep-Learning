# Load Libraries
from string import printable
import numpy as np
import pandas as pd
from keras.preprocessing import sequence
from sklearn import model_selection

class Preprocessor:
    '''
        데이터 불러와서 str->int 변환, padding
        Simple cross validation, K-fold cross validation 선택
    '''
    def __init__(selfs):
        pass

    # Load and preprocess data
    def load_data(__self__, kfold=False):

        # Load data
        DATA_HOME ='../../data/'

        df = pd.read_csv(DATA_HOME + 'url_label.csv',encoding='ISO-8859-1', sep=',')

        # Initial Data Preparation URL
        # Step 1: Convert raw URL string in list of lists where characters that are contained in "printable" are sotred encoded as integer
        url_int_tokens = [[printable.index(x) + 1 for x in url if x in printable] for url in df.url]

        # Step 2: Cut URL string at max_len or pad with zeros if shorter
        max_len = 80
        X = sequence.pad_sequences(url_int_tokens, maxlen=max_len)

        # Step 3: Extract labels form df to numpy array
        label_arr = []
        for i in df['class']:
            if i == 0:
                label_arr.append(0)
            else :
                label_arr.append(1)

        target = np.array(label_arr)

        if kfold:
            return X, target
        else :
            # Simple Cross-Validation: Split the data set into training and test data
            X_train, X_test, target_train, target_test = model_selection.train_test_split(X, target, test_size=0.2, random_state=33)

            return X_train, X_test, target_train, target_test
