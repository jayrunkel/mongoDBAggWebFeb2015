db.cData.aggregate([
	{"$group" : {"_id" : null, 
                           "totalArea" : {$sum : "$areaM"},
                           "avgArea" : {$avg : "$areaM"}}}])
