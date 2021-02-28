import os
import matplotlib.pyplot as plt
from re import search, findall

plt.style.use('ggplot')
directory = './output/characterfrequency/'

# Extract Data
def extractData():
    # Iterate through directory of Mapreduce files
    for file in os.listdir('./output/characterfrequency'):
        if (file != '_SUCCESS'):
            f = open(directory+file, "r")
            keys = []
            values = []
            for key in f:
                # Extract key and values
                (k, v) = key.split(maxsplit=1)
                # Append to array
                keys.append(k)
                values.append(float(v))

            # Determine which file is used based on langauge prefix
            if (search('ENG', keys[0])):
                plotBarChart(keys, values, 'English', 1)
            if (search('FR', keys[0])):
                plotBarChart(keys, values, 'French', 2)
            if (search('NL', keys[0])):
                plotBarChart(keys, values, 'Dutch', 3)

def plotBarChart(keys, values, lang, fig):
    # Generate all 3 graphs
    p = plt.figure(fig)

    # Plot KV to chart
    for character, value in zip(keys, values):
        # Remove language prefix for graph
        plt.bar(''.join(findall("[^A-Z_]", character)).upper(), value)

    # Graph Labels
    plt.xlabel("Characters")
    plt.ylabel("Frequency")
    plt.title("{} Character Frequency".format(lang))

extractData()
plt.show()
