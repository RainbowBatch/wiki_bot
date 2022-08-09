import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from scipy import spatial

from openai.embeddings_utils import cosine_similarity

import kfio

classified_descriptions = kfio.load('data/final.json')[['episode_number', 'categories', 'mediawiki_description']]
embeddings = kfio.load('data/description_embeddings.json')[['episode_number', 'gpt3_ts_babbage_embedding']]

classified_embeddings = pd.merge(
    classified_descriptions,
    embeddings,
    how='inner',
    on='episode_number'
)

labels = list(set([x for l in classified_embeddings.categories.to_list() for x in l]))

# TODO(woursler): Classify indavidually?
classified_embeddings['category'] = classified_embeddings.categories.apply(lambda l: l[0])
classified_embeddings["gpt3_ts_babbage_embedding"] = classified_embeddings.gpt3_ts_babbage_embedding.apply(np.array)


#print(classified_embeddings)

X_train, X_test, y_train, y_test = train_test_split(
    list(classified_embeddings.gpt3_ts_babbage_embedding.values), classified_embeddings.category, test_size=0.2
)

# print(X_test, y_test)

tree = spatial.KDTree(X_train)

for idx in range(len(y_test)):
    _, neighbor_idxs = tree.query(X_test[idx], 5)
    print(neighbor_idxs)
    print("ACTUAL", y_test.iloc[idx])
    for n_idx in neighbor_idxs:
        print("POSSIBLE", y_train.iloc[n_idx])

# TODO(woursler): WTF? Does terribly at this task.
clf = RandomForestClassifier(n_estimators=500)
clf.fit(X_train, y_train)
preds = clf.predict(X_test)
probas = clf.predict_proba(X_test)

report = classification_report(y_test, preds)
print(report)