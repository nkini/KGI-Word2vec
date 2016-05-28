#Written for and (so far) executed with python 3
import pickle
import json
from collections import defaultdict
import gzip


def save_zipped_pickle(obj, filename, protocol=-1):
    with gzip.open(filename, 'wb') as f:
        pickle.dump(obj, f, protocol)


def load_zipped_pickle(filename):
    with gzip.open(filename, 'rb') as f:
        loaded_object = pickle.load(f)
        return loaded_object


def get_nell_labels_to_ids(id_label_filename):
    '''
    id_label_filename: gzipped text file name (with path) where 
    Each line in the TSV file contains 
    NELL_id \t normalized_NELL_entity_label
    Normalization is assumed to be all lower case, [', ] replaced by _
    
    Returns: {normalized_entity_name:nell_id}
    '''
    label2id = dict()
    strange_activity_detector = 0
    with gzip.open(id_label_filename,'rb') as f:
        for line in f:
            uniqid,label = line.decode().strip('\n').split('\t')
            if label in label2id:
                strange_activity_detector += 1
            label2id[label] = int(uniqid)
    if strange_activity_detector:
        print('strange activity detected',strange_activity_detector,'times')
    print(len(label2id),'Normalized NELL entity names to NELL id')
    return label2id


def normalize(entity_label):
    return entity_label.strip('\n').replace("'",'_').replace(".",'_').replace('-',"_").replace(" ",'_')


def get_fblbls_to_mids(wikidata_file):
    '''
    wikidata_file: gzipped text file, 1 json object per line,
                   json has the following schema
                {
                    wikidata_id:    "Q1234",               #d['id']
                    en_label:       "somelabel",           #d['labels']['en']['value'] 
                    en_aliases:     ["alias1","alias2"],   #d['aliases']['en'][0-n]['value']
                    freebase_ids:   ["m/som1","m/som2"],   #d['claims']['P646'][0-n]['mainsnak']['datavalue']['value']
                    en_wikipedia_page: {
                                        title:  ""         #d['sitelinks']['enwiki']['title']
                                        url:    ""         #d['sitelinks']['enwiki']['url']
                                       }
                }
    '''
    wikidata_str2mid = defaultdict(set)
    with gzip.open(wikidata_file, 'rb') as f:
        for line in f:
            d = json.loads(line.decode().strip('\n'))
            if 'en_label' in d:
                wiki_entity = normalize(d['en_label'])
                wikidata_str2mid[wiki_entity].add(tuple(d['freebase_ids']))
            if 'en_aliases' in d:
                for alias in d['en_aliases']:
                    wiki_entity = normalize(alias)
                    wikidata_str2mid[wiki_entity].add(tuple(d['freebase_ids']))
            if 'en_wikipedia_page' in d:
                if 'title' in d['en_wikipedia_page']:
                    wiki_entity = normalize(d['en_wikipedia_page']['title'])
                    wikidata_str2mid[wiki_entity].add(tuple(d['freebase_ids']))
    print(len(wikidata_str2mid),"freebase labels found that correspond to one or more mids")
    return wikidata_str2mid


def print_nid2mid_stats(nid2mid_file):
    nid2mid = load_zipped_pickle(nid2mid_file)
    multiple_tuples = 0
    multiple_mids = 0
    for nid,midset in nid2mid.items():
        if len(midset) > 1:
        #A string where multiple wikidata entities had the same label
            multiple_tuples += 1
        for midtuple in midset:
            if len(midtuple) > 1:
            #A wikidata entity which itself contains multiple freebase mids
                multiple_mids += 1
    print("Total nids:",len(nid2mid))
    print("Entities where multiple wikidata entities had the same label:",multiple_tuples)
    print("Entity where the wikidata entity itself contained multiple freebase mids:",multiple_mids)


def nell_ids_to_freebase_mids(nell_id2name_file,wikidata_file,nellids=None):
    '''
    nellids: set of NELL ids of entities or relations
    Returns: { nell_id: freebase_mid}
    '''
    nlbl2nid = get_nell_labels_to_ids(nell_id2name_file)
    fblbl2mid = get_fblbls_to_mids(wikidata_file)
    nid2mid = {}
    for nlbl,nid in nlbl2nid.items():
        if nlbl in fblbl2mid:
            nid2mid[nid] = fblbl2mid[nlbl]
    print(len(nid2mid),"nell ids found with corresponding mids")
    if nellids:
        return {k:v for k,v in nid2mid.items() if k in nellids}
    else:
        return nid2mid


def mid2wdlbl(wikidata_file):
    mid2wdlbl = {}
    with gzip.open(wikidata_file, 'rb') as f:
        for line in f:
            d = json.loads(line.decode().strip('\n'))
            for fbid in d['freebase_ids']:
                mid2wdlbl[fbid] = d['en_label']
    save_zipped_pickle(mid2wdlbl, 'mid2wdlbl.pkl.gz')
    print("freebase mid to wikidata label correspondence created for",len(mid2wdlbl),"mids")
    return mid2wdlbl


nell_id2name_file = 'names.txt.gz'
wikidata_file = 'wikidata-relevant-schema-only-en-label-with-fbmids.json.txt.gz'
nid2mid_file = 'nid2mid.pkl.gz'

#nid2mid = nell_ids_to_freebase_mids(nell_id2name_file,wikidata_file)
#save_zipped_pickle(nid2mid, 'nid2mid.pkl.gz')
#print_nid2mid_stats(nid2mid_file)
#mid2wdlbl(wikidata_file)
get_nell_labels_to_ids(nell_id2name_file)
