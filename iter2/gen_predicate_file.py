from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
import gensim.models.word2vec
import pickle
import gzip

def save_zipped_pickle(obj, filename, protocol=-1):
    with gzip.open(filename, 'wb') as f:
        pickle.dump(obj, f, protocol)

def load_zipped_pickle(filename):
    with gzip.open(filename, 'rb') as f:
        loaded_object = pickle.load(f)
        return loaded_object

eer_filelist = ["label-train-uniq-raw-rel.db.TRAIN", 
            "label-test-uniq-raw-rel.db.TRAIN", 
            "seed.165.rel.uniq.out",
            "seed.165.rel.uniq_te.out"]
w2v_vocab_file = 'freebase-vectors-skipgram1000-vocab.pkl.gz'
nid2mid_file = 'nid2mid.pkl.gz'
w2vmodel_file = 'freebase-vectors-skipgram1000.bin.gz'

def filter_n2m(n2m):
    return {k:v.pop()[0] for k,v in n2m.items() if len(v)==1 and len(list(v)[0])==1}


def preproc_for_avg_rel_vec(eer_filelist,w2v_vocab_file,nid_to_w2v_file):
    '''
    Get the true relations in the form
     {
        relation1: [(e11,e12),(e13,e14)...],
        relation2: [(e21,e22),(e23,e24)...]
     }
    '''

    preproc_for_avg_rel_vec = defaultdict(set)

    num_both_entities_present = 0
    n2m = filter_n2m(load_zipped_pickle(nid_to_w2v_file))
    vocab = load_zipped_pickle(w2v_vocab_file)

    def do_both_entities_have_vectors(e1,e2):
    	return e1 in n2m and e2 in n2m and n2m[e1] in vocab and n2m[e2] in vocab

    for file in eer_filelist:
        
        with open(file) as f:
            
            for line in f:
                
                e1id,e2id,relid,truth = line.strip('\n').split('\t')
                
                e1 = int(e1id)
                e2 = int(e2id)
                
                if truth == '1' and do_both_entities_have_vectors(e1,e2):
                    num_both_entities_present += 1
                    preproc_for_avg_rel_vec[int(relid)].add((e1,e2))
                    
    print("Number of relations that have a vector:",len(preproc_for_avg_rel_vec))
    print("Number of cases where both entities were present",num_both_entities_present)
    return preproc_for_avg_rel_vec

'''
Calculate average relation vector for the relations
'''
def create_w2v_predicate_file(rel_ent_pairs_dict,w2vmodel_file,nid2mid_file,outfile):
    model = gensim.models.Word2Vec.load_word2vec_format(w2vmodel_file, binary=True)

    avg_rel_vectors = dict()
    n2m = filter_n2m(load_zipped_pickle(nid2mid_file))

    for rel,elist in rel_ent_pairs_dict.items():
        n = len(elist)
        vector_sum = 0
        for e1,e2 in elist:
            vector_sum += model[n2m[e1]] - model[n2m[e2]]
        avg_rel_vectors[rel] = vector_sum / n    

    counter = 0
    
    with open(outfile,'w') as f:
        for rel,elist in rel_ent_pairs_dict.items():
            for e1,e2 in elist:
                cosine_score = cosine_similarity(model[n2m[e1]] - model[n2m[e2]], avg_rel_vectors[rel])[0][0]
                #print(cosine_score,end=' ')
                if cosine_score < 0:
                    cosine_score = 0
                string = str(e1)+"\t"+str(e2)+"\t"+str(rel)+"\t"+str(cosine_score)+"\n"
                f.write(string)
                counter += 1
                
    print(counter," lines written to the predicate file",outfile)            


rel_ent_pairs_dict = preproc_for_avg_rel_vec(eer_filelist,w2v_vocab_file,nid2mid_file)
create_w2v_predicate_file(rel_ent_pairs_dict,w2vmodel_file,nid2mid_file,'W2v.WhateverEntitiesResolved.csv')
