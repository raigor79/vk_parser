from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import KNeighborsClassifier


def preparation_data(data, keys):
    list_comm = []
    for item in data:
        signs_str = ''
        for sign in keys:
            if sign in item:
                signs_str += ' '+ item[sign]
        list_comm.append(signs_str)
    return list_comm


def group_classification(data, data_training, par_train, keys):
    
    list_comm_sig = preparation_data(data, keys)
    
    list_data_train = []
    y_train = []
    for key, value in data_training.items():
        for item in value:
            list_data_train.append(item)
            if key == par_train:
                y_train.append(1)
            else:
                y_train.append(0)
    list_comm_sig = [sign.lower() for sign in list_comm_sig]
    list_data_training = [sign.lower() for sign in list_data_train]
    full_data = list_comm_sig + list_data_training
    
    vector = TfidfVectorizer()
    x_data = vector.fit_transform(full_data)
    x_train = x_data[len(list_comm_sig):]
    
    knn_classif = KNeighborsClassifier(n_neighbors=2)
    knn_classif.fit(x_train,y_train)
    
    return knn_classif.predict(x_data[:len(list_comm_sig)])


if __name__== "__main__":
    pass