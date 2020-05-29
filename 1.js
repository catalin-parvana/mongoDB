const MongoClient = require('mongodb').MongoClient;
const url="mongodb://10.244.0.28";
var readline = require('readline-sync');
var assert= require('assert');

var allDatabases = [];
var allCollections=[];

var allUsers=[];
var totalGreaterThan=10;


/* Main Function */
async function main(){
    const client = new MongoClient(url, {useNewUrlParser: true, useUnifiedTopology: true});
    try{
        /* Connect to DB */
        await client.connect();
        
        /* List All Databases */
        await listDatabases(client);

        /* Select Database */
        const databaseName= await selectDatabaseName(client);
        
        /* List All Collections from Provided Database */
        await listCollections(client,databaseName);

        /* Select Collection */
        selectCollectionName(client,databaseName);
        console.log("array_length",allCollections.length);

        /* List All Tenants */
        await listTenants(client);

        /* Query Every Collection */
        await getTotalPerTenant(client,databaseName,totalGreaterThan);   

    }catch(e){
        /* Catch Errors if any */
        console.error(e);
    }finally{
        /* Close DB connection */
        await client.close();
    }
}

main().catch(console.err);

/* List All Databases */
async function listDatabases(client){
    const databasesList=await client.db().admin().listDatabases();
    console.log("Databases:");
    databasesList.databases.forEach(db => console.log(` - ${db.name}`));
}

/* Select Database */
async function selectDatabaseName(client){
    const cursor=await client.db().admin().listDatabases();
    cursor.databases.forEach(db => allDatabases.push(db.name));
    
var databaseName;
    while (!allDatabases.includes(databaseName)){
        databaseName = readline.question("Chose Database: ");
        databaseName=databaseName.trim();
        if(!allDatabases.includes(databaseName)){
            console.log("Database don't exist.")
            console.log();
        }else{
            return databaseName;
        }
    }
}

/* List Tenants */
async function listTenants(client){
    databaseName="Users-nirmata-devtest2";
    collectionName="Tenant";

    const cursor= await client.db(databaseName).collection(collectionName)
    .find({},{_id:1 ,ownerEmail:1, name:1});
    // const results=cursor.toArray();
    console.log(cursor);
}


/* List All Collections */
async function listCollections(client, databaseName){
    const db= await client.db(databaseName);
    console.log("Collections: ");
    db.collections(function(e, cols) {
        cols.forEach(function(col) {
            var collectionName=col.collectionName;
            // allCollections.push(collectionName);
            console.log(` - ${collectionName}`);
        });
    });
}


/* Select Collection */
async function selectCollectionName(client, databaseName){
    const db= await client.db(databaseName);
    db.collections(function(e, cols) {
        cols.forEach(function(col) {
            allCollections.push(col.collectionName)
        });
    });
        
var collectionName;
    while (!allCollections.includes(collectionName)){
        collectionName = readline.question("Chose Collection: ");
        collectionName=collectionName.trim();
        if(!allCollections.includes(collectionName)){
            console.log("Collection don't exist.")
            console.log();
        }else{
            return collectionName;
        }
    }
}



/* List All Documents */
async function listDocuments(client, databaseName){
    const db=  client.db(databaseName);
    console.log("Documents: ");
    db.collections(function(e, cols) {
        cols.forEach(function(col) {
            var collectionName=col.collectionName;
            console.log(` - ${collectionName}`);
        });
    });
}

 /* Query Every Collection */
async function getTotalPerTenant(client, databaseName, totalGreaterThan){

    var myQuery=[
        {$group:{_id:"$tenantId", total:{$sum:1}}},
        {$match:{total: {$gt:totalGreaterThan}}},
        {$sort:{total: -1}}
        ];

        const db= await client.db(databaseName);
        db.collections(function(e, cols) {
            cols.forEach(function(col) {
                var collectionName=col.collectionName;
                db.collection(collectionName).aggregate(myQuery).toArray(function (err,result){
                    assert.equal(null,err);
                    if(err) throw err;
                    if(result.length>0){
                        console.log("Collection: ", collectionName);
                        console.log(result); 
                    }
                });
            });
        });

}