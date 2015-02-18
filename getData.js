var MongoClient = require('mongodb').MongoClient,
    request = require('request-json'),
    format = require('util').format,
    http = require('http'),
    assert = require('assert');

var urls = {
//    "1990" : 'http://api.census.gov/data/1990/sf1?for=state:*&get=P0010001,H0010001,H0100001,STUSAB&key=25ca0a1b29ff01d0868792954a47715829b2d9d9',
    "1990" : 'http://api.census.gov/data/1990/sf1?for=state:*&get=P0010001,H0010001,H0190001,STUSAB&key=25ca0a1b29ff01d0868792954a47715829b2d9d9',
//    "2000" : 'http://api.census.gov/data/2000/sf1?for=state:*&key=25ca0a1b29ff01d0868792954a47715829b2d9d9&get=P001001,H001001,H010001,STUSAB',
    "2000" : 'http://api.census.gov/data/2000/sf1?for=state:*&key=25ca0a1b29ff01d0868792954a47715829b2d9d9&get=P001001,H001001,H003002,STUSAB',
//    "2010" : 'http://api.census.gov/data/2010/sf1?for=state:*&get=P0010001,H00010001,H0100001,NAME&key=25ca0a1b29ff01d0868792954a47715829b2d9d9'
    "2010" : 'http://api.census.gov/data/2010/sf1?for=state:*&get=P0010001,H00010001,H0030002,NAME&key=25ca0a1b29ff01d0868792954a47715829b2d9d9'
};

var urlBase = 'http://api.census.gov/data';

// 1990 API variables
// http://api.census.gov/data/1990/sf1/variables.json

// 2000 API variables
// http://api.census.gov/data/2000/sf1/variables.json

var client = request.newClient(urlBase);

var fieldMapping = {
    "P0010001" : "totalPop",
    "P001001" : "totalPop",
    "H00010001" : "totalHouse",
    "H0010001" : "totalHouse",
    "H001001" : "totalHouse",
    "H0190001" : "occHouse",
    "H0100001" : "occHouse",
    "H010001" : "occHouse",
    "H003002" : "occHouse",
    "H0030002" : "occHouse",
    "STUSAB" : "abv",
    "NAME" : "name"
};

var stateFields = ["state", "name", "abv"];


function mapFieldNames(heading) {

    var result = [];
    for (var i = 0; i < heading.length; i++) {
        if (fieldMapping[heading[i]]) {
            result[i] = fieldMapping[heading[i]];
        }
        else {
            result[i] = heading[i];
        }
           
    };

    return result;
};

function removeStateFields(record) {

    var result = {};
    
    for (var p in record) {
        if (stateFields.indexOf(p) >= 0) {
            result[p] = record[p];
            delete record[p];
        }
        else {
            record[p] = Number(record[p]);
        };
    };

    return result;
};

function addAreaData(stateName, areaColl, stateColl) {

    areaColl.findOne({"state" : stateName}, function(error, areaInfo) {

        assert.equal(error, null);


        stateColl.update({"name" : stateName},
                         {$set: { "areaM" : areaInfo["areaM"],
                                  "areaKM" : areaInfo["areaKM"],
                                  "perWater" : areaInfo["perWater"]}},
                         {upsert:true},
                         function (err, result) {
                             if (err) {
                                 console.log("ERROR: $s", err);
                             };
                         });
        });
};

function addGeoData(stateName, geoColl, stateColl) {
    geoColl.findOne({"state" : stateName}, function(error, geoInfo) {
        
        assert.equal(error, null);


        if (geoInfo) {
            stateColl.update({"name" : stateName},
                             {$set: { "center" : {"type" : "Point", "coordinates" : [geoInfo["longitude"], geoInfo["latitude"]]}}},
                             {upsert:true},
                             function (err, result) {
                                 if (err) {
                                     console.log("ERROR: $s", err);
                                 };
                             });
        };
    });
};

MongoClient.connect('mongodb://localhost:27017/census', function(err, db) {
    if(err) throw err;

    var collection = db.collection('cData');
    var regColl = db.collection('regions');
    var areaColl = db.collection('stateArea');
    var geoColl = db.collection('geoCenter');

    collection.drop();

    for (var y in urls) {

        (function (year) {
            client.get(urls[year], function (err, res, body) {
                assert.equal(err, null);
            
                var header = body[0];
                header = mapFieldNames(header);
                console.log(header);
            
                for (var c = 1; c < body.length; c++) {

                    var state = body[c];
                    
                    console.log("state: " + state);
                    var censusRecord = {};
                    var stateFields;
            
                    for (var i = 0; i < header.length; i++) {
                        censusRecord[header[i]] = state[i];
                        //                    console.log("Record Field %s given value %s", header[i], state[i]);
                    };
                    censusRecord["year"] = Number(year);
                    console.log("Year %s record: %j", year, censusRecord);

                    stateFields = removeStateFields(censusRecord);

                    //addRegionFields(stateFields, regColl);
                    (function (stateStuff, record) {
                        regColl.findOne({"state" : stateStuff["name"]}, function(error, regInfo) {
                            assert.equal(error, null);
                            console.log("Region info for state %s: %j", stateStuff["name"], regInfo);

                            for (var p in regInfo) {
                                if (p != "state" && p != "_id") {
                                    stateStuff[p] = regInfo[p];
                                };
                            };

                            (function (stateName) {
                                collection.update({"stateNum" : Number(stateStuff["state"])},
                                                  {$set: stateStuff,
                                                   $push : {"data" : record}},
                                                  {upsert:true},
                                                  function (err, result) {
                                                      //                                                  assert.equal(err, null);
                                                      if (err) {
                                                          console.log("ERROR: $s", err);
                                                      }
                                                      else if (stateName) {
                                                          addAreaData(stateName, areaColl, collection);
                                                          addGeoData(stateName, geoColl, collection);
                                                      };

                                              //     console.log("insert record: %j", result);
                                              });
                            })(stateStuff["name"]);
                    
                        });
//               db.close();
                    })(stateFields, censusRecord);

                };
            });
        })(y);
    };
    
});


