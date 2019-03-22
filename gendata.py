import csv
import random
import numpy
import json
import uuid
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from datetime import datetime

host = "srvelk1.labs.local.lan"
port = 9200
es = Elasticsearch(hosts=[{'host': host, 'port': port}],
                   connection_class=RequestsHttpConnection)
idx_name = 'doleances'
doc_type = 'vote'
firsttime = True

def gendata(idx_base, doc_type):
    ID = 0
    OLDPACKET = 0
    exprimes_non_exprimes = ["exprimes", "nesaispas", "nonexprimes"]
    doleances = ["doleance_1", "doleance_2","doleance_3"]
    with open("villes_france.csv", mode="r", encoding="utf-8") as csvfile:
        villes = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for ville in villes:
            repart_doleance = numpy.random.dirichlet(
                numpy.ones(len(doleances)), size=1)
            nbdol = len(doleances)
            d = 0

            for dol in range(0, nbdol):
                repart_exp_on_dol = numpy.random.dirichlet(
                    numpy.ones(len(exprimes_non_exprimes)), size=1)
                nbexp = len(exprimes_non_exprimes)

                for j in range(0, nbexp):
                    explib = exprimes_non_exprimes[j]
                    exp = repart_exp_on_dol[0][j]
                    dolexp = repart_doleance[0][d]
                    population = int(ville["population_2012"])
                    nbvotestogenerate = int(
                        numpy.round(dolexp * exp * population))



                    for i in range(1, nbvotestogenerate):
                        ID = ID + 1
                        if numpy.round(ID/1000000) > OLDPACKET or firsttime:
                            firsttime = Falseok
                            OLDPACKET = numpy.round(ID/1000000)
                            idx_name = idx_base + '-' + str(numpy.round(ID/1000000))
                            if not es.indices.exists(index=idx_name):
                                es.indices.create(idx_name)
                                mapping = {
                                    "vote": {
                                        "properties": {
                                            "_timestamp": {
                                                "type": "date"
                                            },
                                            "uuid": {"type": "text",
                                                        "fields": {
                                                            "keyword": {
                                                                "type": "keyword",
                                                                "ignore_above": 256
                                                            }
                                                        }
                                                        },
                                            "ville": {"type": "text",
                                                        "fields": {
                                                            "keyword": {
                                                                "type": "keyword",
                                                                "ignore_above": 256
                                                            }
                                                        }},
                                            "departement": {"type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "type": "keyword",
                                                                    "ignore_above": 256
                                                                }
                                                            }},
                                            "doleance": {"type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "type": "keyword",
                                                                    "ignore_above": 256
                                                                }
                                                            }},
                                            "vote_value": {"type": "text",
                                                            "fields": {
                                                                "keyword": {
                                                                    "type": "keyword",
                                                                    "ignore_above": 256
                                                                }
                                                            }},
                                            "local_vote_id": {"type": "long"},
                                            "id": {"type": "long"},
                                            "location": {
                                                "type": "geo_point"
                                            }
                                        }
                                    }
                                }

                                es.indices.put_mapping(index=idx_name, doc_type=doc_type, body=mapping)



                        yield {
                            "_index": idx_name,
                            "_type": doc_type,
                            "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                            "uuid": uuid.uuid4(),
                            "ville": ville["nom_reel"],
                            "departement": ville["departement"],
                            "doleance": doleances[dol],
                            "vote_value": explib,
                            "local_vote_id": i,
                            "id": ID,
                            "location": {
                                "lat": ville["latitude_deg"],
                                "lon": ville["longitude_deg"]
                            }

                        }

                d = d + 1


helpers.bulk(es, gendata(idx_name, doc_type))
