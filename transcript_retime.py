from transcripts import create_full_transcript_listing, parse_transcript
import numpy as np
from thefuzz import fuzz
from attr import attrs, attr
import matplotlib.pyplot as plt
from math import sqrt
from tqdm import tqdm

@attrs
class AStarGraph:
    board = attr()

    @property
    def n(self):
        return self.board.shape[0]

    @property
    def m(self):
        return self.board.shape[1]

    def heuristic(self, start, goal):
        dx = abs(start[0] - goal[0])
        dy = abs(start[1] - goal[1])
        return sqrt(dx**2 + dy**2)

    def get_vertex_neighbours(self, pos):
        #Moves allow only going forward.
        for dx, dy in [(1,0),(0,1),(1,1)]:
            x2 = pos[0] + dx
            y2 = pos[1] + dy
            if x2 < 0 or x2 >= self.n or y2 < 0 or y2 >= self.m:
                continue
            if self.board[x2][y2] >= 1: # TODO(woursler): Remove scaling.
                continue
            yield (x2, y2)

    def move_cost(self, a, b):
        return self.heuristic(a, b) + 2 * self.board[b[0]][b[1]]

def AStarSearch(start, end, graph):

    G = {} #Actual movement cost to each position from the start position
    F = {} #Estimated movement cost of start to end going via this position

    #Initialize starting values
    G[start] = 0
    F[start] = graph.heuristic(start, end)

    closedVertices = set()
    openVertices = set([start])
    cameFrom = {}

    max_completion = 0
    with tqdm(total=100) as progressbar:
        while len(openVertices) > 0:
            #Get the vertex in the open list with the lowest F score
            current = None
            currentFscore = None
            for pos in openVertices:
                if current is None or F[pos] < currentFscore:
                    currentFscore = F[pos]
                    current = pos

            #Check if we have reached the goal
            if current == end:
                #Retrace our route backward
                path = [current]
                while current in cameFrom:
                    current = cameFrom[current]
                    path.append(current)
                path.reverse()
                return path, F[end] #Done!

            #Mark the current vertex as closed
            openVertices.remove(current)
            closedVertices.add(current)

            #Update scores for vertices near the current position
            for neighbour in graph.get_vertex_neighbours(current):
                if neighbour in closedVertices:
                    continue #We have already processed this node exhaustively
                candidateG = G[current] + graph.move_cost(current, neighbour)

                if neighbour not in openVertices:
                    openVertices.add(neighbour) #Discovered a new vertex
                elif candidateG >= G[neighbour]:
                    continue #This G score is worse than previously found

                #Adopt this G score
                cameFrom[neighbour] = current
                G[neighbour] = candidateG
                H = graph.heuristic(neighbour, end)
                F[neighbour] = G[neighbour] + H

                # Update the progress bar.
                completion = 100 * (1 - H/F[neighbour])
                if completion > max_completion:
                    max_completion = completion
                    progressbar.n = int(max_completion)
                    progressbar.refresh()

    raise RuntimeError("A* failed to find a solution")

print("Loading transcripts.")

df = create_full_transcript_listing()


transcript_texts = []
for ep1_transcript in df[df.episode_number == '735'].to_dict(orient='records'):
    print(ep1_transcript)
    transcript_texts.append([x.text for x in parse_transcript(ep1_transcript).blocks])


transcript_texts = [
    transcript_texts[1],
    transcript_texts[2],
]

print("Computing distance matrix.")

D = np.zeros((len(transcript_texts[0]), len(transcript_texts[1]))) # distance matrix (m, n)

M, N = D.shape

for i in range(len(transcript_texts[0])):
    for j in range(len(transcript_texts[1])):

        off_axis = abs(i/M - j/N)
        if off_axis > 0.1:
            continue
        d = fuzz.partial_ratio(transcript_texts[0][i].lower(), transcript_texts[1][j].lower())
        if d <= 0:
            d = 1 # TODO (woursler): Use -1 instead?
        D[i][j] = d

D = 1 - D/100

print("Distances pre-computed.")


# Do A* over the result

print("Starting A* search.")
graph = AStarGraph(D)
result, _ = AStarSearch((0,0), (M-1, N-1), graph)
print("A* search completed. Plotting.")
plt.imshow(D, cmap='hot', interpolation='nearest')
plt.plot([v[1] for v in result], [v[0] for v in result])
plt.show()


print(result)

#for i, j in result:
#    print('\n\n\n====\n', transcript_texts[0][i], '\n-\n', transcript_texts[1][j])
