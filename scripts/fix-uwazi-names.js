import mongodb from "mongodb";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });
const passedArgs = process.argv.slice(2);

const fixNames = async () => {
  console.log("Fixing Names (titles)...");
  try {
    await client.connect();
    const database = client.db("metmuseum");
    const objects = database.collection("entities");

    const startID = passedArgs[0] || 0;
    const endID = passedArgs[1] || 1000000;

    const cursor = objects.find(
      {
        $and: [
          {
            template: mongodb.ObjectID.createFromHexString(
              "5bfbb1a0471dd0fc16ada146"
            ),
          },
          { "metadata.object_id.0.value": { $gte: Number(startID) } },
          { "metadata.object_id.0.value": { $lte: Number(endID) } },
        ],
      },
      { title: 1, "metadata.object_id.0.value": 1 }
    );

    let object;
    let fixed = 0;

    while ((object = await cursor.next())) {
      process.stdout.write(`Fixing: ${object.metadata.object_id[0].value}\r`);
      if (object.title.indexOf("|") !== -1) {
        await objects.updateOne(
          { _id: object._id },
          { $set: { title: object.title.replace(/\|/g, " | ") } }
        );
        fixed += 1;
      }
    }
    process.stdout.write("\r\n");

    console.log("Fixed", fixed, "objects.");
    await client.close();
  } catch (err) {
    await client.close();
    throw err;
  }
};

fixNames();
