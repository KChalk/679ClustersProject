# m

import json

vocab=[]

nterms=30

with open('m_lda_vocab.txt') as f: 
    for line in f: 
        vocab.append(line.strip())

#print(vocab[:15])

topics=[]
with open('m_lda_topics.json') as f: 
    for line in f: 
        topics.append(json.loads(line))

for topic in topics: 
    #nterms=len(topic['termIndices'])
    topic['terms']=[None]*nterms
    print('Topic', topic['topic'], 'top', nterms, 'words:' )
    for i in range(nterms):
        index=topic['termIndices'][i]
        topic['terms'][i]=vocab[index]
        #print(topic['terms'][i],':%.4f' % (float(topic['termWeights'][i])*100))
    print(topic['terms'])
