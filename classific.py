import numpy
import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer

def groupg_classification():
    pass





if __name__=="__main__":
    st_1 = ['Новости науки', 'новости день',  'новости погоды', 'свежие новости', 'новости', 'новости первые вторые, третьи', 
        'новости', 'развлечения', 'развлекательный канал', "мир кино", "планета кино", "все о кино", 'фильмы', 'кинофильмы', 'новостной канал']
    vector = TfidfVectorizer()
    mas = vector.fit_transform(st_1)
    print(mas.toarray())
    print(vector.get_feature_names())   
    print(vector.get_feature_names().index('кино'))