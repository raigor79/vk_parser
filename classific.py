import numpy
import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neighbors import KNeighborsClassifier


def preparation_data(data, keys):
    list_comm = []
    for item in data:
        signs_str = ''
        for sign in keys:
            if sign in item:
                signs_str += item[sign]
            list_comm.append(signs_str)
    return list_comm


def group_classification(data, data_training, par_train, keys):
    
    list_comm_sig = preparation_data(data, keys)
    print(list_comm_sig)
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
    knn_classif = KNeighborsClassifier()
    knn_classif.fit(x_train,y_train)
    return knn_classif.predict(x_data[:len(list_comm_sig)])


 


if __name__== "__main__":
    """st_1 = ['Новости науки dfdfsdf  dsfsrer ', 'rwes es ers новости день',  'новости rees resrsers s погоды', 'свежие fssdfshj  fsfef f новости', 'лента новостей', 'новости первые вторые, третьи', 
        'sa dagfef dsdsgd новости', 'sdf sffsdf sfd fdaразвлечения', 'развлекательный канал', "мир кино", "планета кино", "все о кино", 'фильмы', 'кинофильмы', 'новостной канал', 'Новости сегодня', 
        'Новости кино', "Кино сегодня", "развленчение игры кино", "привет", 'свежие новости сегодня', 'объявления города бийска пригородные новости', 'новостная лента', "бизнес котоировки акции", "новости бизнеса", 'сапог', '']
    vector = TfidfVectorizer()
    mas = vector.fit_transform(st_1)
    #print(mas.toarray())
    #print(vector.get_feature_names())   
    #print(vector.get_feature_names().index('кино'))
    x = mas[:15]
    test = mas[15:]
    y = [1,1,1,1,1,1,1,0,0,0,0,0,0,0,1]
    print(x, len(y), test)
    knn = KNeighborsClassifier(n_neighbors=5)
    knn.fit(x,y[:15])
   
    knn_pre = knn.predict(test)
    print(knn_pre)"""
    m = {
        "news": [
            "новости", 
            "новостной сайт",
            "новости сегодня",
            "новостной канал",
            "свежие новости",
            "деловые новости",
            "финансовые новости",
            "cми",
            "средства массовой информации",
            "интернет-сми",
            "информационный канал"
        ], 
        "entertainment": [
            "кино",
            "игры",
            "равлечения",
            "музыка",
            "разлекательный канал",
            "юмор",
            "игра",
            "смешное видео",
            "музыкальный канал",
            "фильмы",
            "кинофильмы"
        ]
        }
    ss = group_classification([{'discription':'кино'},{'discription': "новости"},{'discription':'модные новости'}], m, 'news',['discription'])
    print(ss)