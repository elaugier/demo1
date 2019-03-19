import csv, random
import numpy
import json
import uuid
from elasticsearch import Elasticsearch, RequestsHttpConnection

host = "srvelk1.labs.local.lan"
port = 9200
es = Elasticsearch(hosts=[{'host':host, 'port':port}],connection_class=RequestsHttpConnection)

exprimes_non_exprimes = ["exprimes", "nesaispas", "nonexprimes"] 
doleances = ["doleance_1", "doleance_2"]

n = 1

with open("villes_france.csv",mode= "r", encoding="utf-8") as csvfile:
    villes = csv.DictReader(csvfile, delimiter=",", quotechar='"')
    for ville in villes:
        print("repartion des doleances pour la ville de ", ville["nom_reel"])
        repart_doleance = numpy.random.dirichlet(numpy.ones(len(doleances)), size=1)
        print( repart_doleance )
        for dol in doleances:
            #print("répartition exprimé/blanc/non exprimé pour doleance '", dol,"'")
            repart_exp_on_dol = numpy.random.dirichlet(numpy.ones(len(exprimes_non_exprimes)), size=1) 
            #print( repart_exp_on_dol )
            #print("*************************************")

            liste_votes = {}
            nbexp = len(exprimes_non_exprimes)
            for j in range(0, nbexp - 1):
                explib = exprimes_non_exprimes[j]
                exp = repart_exp_on_dol[0][j]
                population = int(ville["population_2012"])
                nbvotestogenerate = int(numpy.round(exp * population))
                #print("nombre de votes à générer : ", nbvotestogenerate)
                #print("*************************************")

                for i in range(1, nbvotestogenerate):
                    vote = {
                        "uuid": uuid.uuid4(),
                        "ville": ville["nom_reel"],
                        "departement": ville["departement"],
                        "doleance": dol,
                        "vote_value": explib,
                        "location": {
                            "lat": ville["latitude_deg"],
                            "lon": ville["longitude_deg"]
                        }
                    }
                    #print(vote)
                    #print("_______")
                    res = es.index(index="demo1",doc_type='vote',id = n, body=vote )
                    #print(repr(res))
                    n = n + 1



            
        

