import superagent from "superagent";
import mongodb from "mongodb";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });
const passedArgs = process.argv.slice(2);

const populateImages = async () => {
  console.log("Populating Images...");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const objects = database.collection("objects");

    const startID = passedArgs[0] || 0;
    const endID = passedArgs[1] || 1000000;

    const cursor = objects
      .find({
        $and: [
          { "Object ID": { $gte: Number(startID) } },
          { "Object ID": { $lte: Number(endID) } },
        ],
      })
      .sort({ "Object ID": 1 })
      .addCursorFlag("noCursorTimeout", true);

    let object;
    let added = 0;

    while ((object = await cursor.next())) {
      process.stdout.write(
        `Processing: ${object["Object ID"]}, already added: ${added}\r`
      );
      if (object["Is Public Domain"] === "True") {
        const apiRes = await superagent.get(
          `https://collectionapi.metmuseum.org/public/collection/v1/objects/${object["Object ID"]}`
        );

        const {
          primaryImage,
          primaryImageSmall,
          additionalImages,
        } = apiRes.body;

        if (primaryImage || primaryImageSmall || additionalImages.length) {
          await objects.updateOne(
            { _id: object._id },
            { $set: { primaryImage, primaryImageSmall, additionalImages } }
          );
          added += 1;
        }
      }
    }
    process.stdout.write("\r\n");

    console.log("Populated", added, "images.");
    cursor.close();
    await client.close();
  } catch (err) {
    await client.close();
    throw err;
  }
};

populateImages();
