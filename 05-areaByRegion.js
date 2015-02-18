db.cData.aggregate([
	{"$group" : {"_id" : "$region", 
                 "totalArea" : {$sum : "$areaM"},  
	         "avgArea" : {$avg : "$areaM"},
		 "numStates" : {$sum : 1},   
                 "states" : {$push : "$name"}}}
])
