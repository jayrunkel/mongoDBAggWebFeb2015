db.cData.aggregate([
    {$unwind : "$data"},    
    {$group : {"_id" : "$data.year", "totalPop" : {$sum : "$data.totalPop"}}},
    {$sort : {"totalPop" : 1}}
])
