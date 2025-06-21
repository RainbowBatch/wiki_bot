from thefuzz import fuzz
import rainbowbatch.kfio as kfio
import numpy as np

from box import Box
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D
from sklearn import manifold
from sklearn.preprocessing import normalize
from pprint import pprint
from tqdm import tqdm

raw_entity_listing = kfio.load('data/raw_entities.json')


def jaccard_distance(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    if union == 0:
        return 1
    return 1 - (float(intersection) / union)


ordered_names = []
all_origins = {}

for entity_record in raw_entity_listing.to_dict(orient='records'):
    entity_record = Box(entity_record)
    origin_episode_numbers = set([o.split('__')[0]
                                  for o in entity_record.entity_origin])
    origin_episode_numbers.discard('None')

    entity_name = entity_record.entity_name

    #if len(origin_episode_numbers) < 5:
    #    continue

    ordered_names.append(entity_name)
    all_origins[entity_name] = origin_episode_numbers

N = len(ordered_names)
print("N =", N)

dist_matrix = np.zeros((N, N))
for i, i_name in enumerate(tqdm(ordered_names)):
    for j, j_name in enumerate(ordered_names):
        dist_matrix[i, j] = jaccard_distance(
            all_origins[i_name], all_origins[j_name])


dist2_matrix = np.zeros((N, N))
for i, i_name in enumerate(tqdm(ordered_names)):
    for j, j_name in enumerate(ordered_names):
        dist2_matrix[i, j] = 1 - fuzz.ratio(i_name.lower(), j_name.lower())/100

import networkx as nx
G = nx.Graph()

for i, j in zip(*np.where(dist2_matrix < 0.1)):
    if j >= i:
        continue
    G.add_edge(i, j)
    print(ordered_names[i], "<=>", ordered_names[j])

for component in nx.connected_components(G):
    rep = None
    rep_count = 0

    members = set()

    print("===")
    for i in component:
        entity_count = raw_entity_listing[raw_entity_listing.entity_name == ordered_names[i]].entity_count.to_list()[0]
        members.add(ordered_names[i])
        if entity_count > rep_count:
            rep_count = entity_count
            rep = ordered_names[i]
    members.discard(rep)

    for member in members:
        print("\t'%s': '%s'," % (member, rep))


norm = plt.Normalize(1, 4)

model = manifold.TSNE(n_components=2, random_state=0,
                      metric='precomputed', perplexity=100)
coords = model.fit_transform(dist_matrix)

cmap = plt.get_cmap('Set1')

fig, ax = plt.subplots()

sc = plt.scatter(coords[:, 0], coords[:, 1], marker='o',
                 s=50, edgecolor='None')

plt.tight_layout()
plt.axis('equal')


annot = ax.annotate("", xy=(0, 0), xytext=(20, 20), textcoords="offset points",
                    bbox=dict(boxstyle="round", fc="w"),
                    arrowprops=dict(arrowstyle="->"))
annot.set_visible(False)


def update_annot(ind):
    pos = sc.get_offsets()[ind["ind"][0]]
    annot.xy = pos
    text = ordered_names[ind["ind"][0]]
    annot.set_text(text)
    annot.get_bbox_patch().set_alpha(0.4)


def hover(event):
    vis = annot.get_visible()
    if event.inaxes == ax:
        cont, ind = sc.contains(event)
        if cont:
            update_annot(ind)
            annot.set_visible(True)
            fig.canvas.draw_idle()
        else:
            if vis:
                annot.set_visible(False)
                fig.canvas.draw_idle()


fig.canvas.mpl_connect("motion_notify_event", hover)

plt.show()
