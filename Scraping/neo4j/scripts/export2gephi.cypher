match path = (:TG_CHANNEL)-[:FORWARDED_BY]->(:TG_CHANNEL)
call apoc.gephi.add(null,'workspace1',path,'weight')
yield nodes
return *