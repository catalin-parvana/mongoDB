const MongoClient = require('mongodb').MongoClient;
const url="mongodb://10.244.0.28";
var totalGreaterThan=10;
var envDatabase="Environments-nirmata-devtest2";
var usersDatabase="Users-nirmata-devtest2";

async function main(){
    const client = new MongoClient(url, {useNewUrlParser: true, useUnifiedTopology: true});
    try{
        await client.connect();
        var allUsers=await getListOfTenants(client,usersDatabase);
        var allCollections=await getListOfColectionsInDb(client,envDatabase);
        await getTotalPerTenant(client, envDatabase, allCollections, allUsers, totalGreaterThan);
    }catch(e){
        console.error(e);
    }finally{
        await client.close();
    }
}
main().catch(console.err);


/* Get List Of Tenants */
async function getListOfTenants(client,usersDatabase){
    var allUsers=[];
        allUsers= await client.db(usersDatabase).collection("Tenant")
        .find({}).project({"_id":1, "ownerEmail":1, "name":1}).toArray();
        return allUsers;
}


/* Get list Of Collections In Db */
async function getListOfColectionsInDb(client,databaseName){
var allCollections=[];
    const col= await client.db(databaseName).listCollections().toArray();   
    for(i=0;i<col.length;i++){
        allCollections.push(col[i].name);
    }
    return allCollections;
}


/* Query Every Collection */
async function getTotalPerTenant(client, databaseName, allCollections, allUsers, totalGreaterThan){
    var myQuery=[
        {$group:{_id:"$tenantId", total:{$sum:1}}},
        {$match:{total: {$gt:totalGreaterThan}}},
        {$sort:{total: -1}}
        ];

        for(i=0;i<allCollections.length;i++){
            collectionName=allCollections[i];
            result=await client.db(databaseName).collection(collectionName).aggregate(myQuery).toArray();
                if(result.length>0){
                    console.log("________________________________________________________________________________________");
                    console.log("Collection: ", collectionName);
                    result.forEach((res)=>{
                        allUsers.forEach((user)=>{
                            if (user._id===res._id){
                                console.log(`id: ${user._id}, email: ${user.ownerEmail}; name: ${user.name}; total:${res.total}`);
                            }
                        });
                    });
                }
        }     
}

