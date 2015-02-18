db.cData.aggregate([
  {$geoNear : {
      "near" : {"type" : "Point", "coordinates" : [90, 35]},
      distanceField : "dist.calculated",
      maxDistance : 500000,
      includeLocs : "dist.location",
      spherical : true
  }},
  {$unwind : "$data"},
  {$group : {"_id" : "$data.year",
             "totalPop" : {"$sum" : "$data.totalPop"},
             "states" : {"$addToSet" : "$name"}}},
  {$sort : {"_id" : 1}}             
  ]
  )
//db.cData.ensureIndex({"center" : "2dsphere"})