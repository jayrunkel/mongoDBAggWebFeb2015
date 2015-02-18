db.cData.aggregate(
   [{$match : {"region" : "South"}},
    {$unwind : "$data"},  
    {$group : {"_id" : "$data.year",
               "totalPop" : {"$sum" : "$data.totalPop"}}}
   ])
