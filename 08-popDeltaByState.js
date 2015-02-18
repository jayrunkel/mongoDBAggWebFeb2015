db.cData.aggregate(
  [{$unwind : "$data"},  
   {$sort : {"data.year" : 1}},
   {$group : {"_id" : "$name",
              "pop1990" : {"$first" : "$data.totalPop"},
              "pop2010" : {"$last" : "$data.totalPop"}}},
   {$project : {"_id" : 0,
                "name" : "$_id",
                "delta" : {"$subtract" : 
                           ["$pop2010", "$pop1990"]},
                "pop1990" : 1,
                "pop2010" : 1}
   }]
)
