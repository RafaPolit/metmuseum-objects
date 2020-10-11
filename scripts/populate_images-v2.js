import superagent from "superagent";
import mongodb from "mongodb";
import PromisePool from "@supercharge/promise-pool";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });
const passedArgs = process.argv.slice(2);

const populateImages = async () => {
  console.log("Populating Images...");
  console.time("populating");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const objects = database.collection("objects");

    const startID = passedArgs[0] || 0;
    const endID = passedArgs[1] || 1000000;

    let added = 0;

    const idsToProcess = await objects
      .find(
        {
          $and: [
            { "Object ID": { $gte: Number(startID) } },
            { "Object ID": { $lte: Number(endID) } },
          ],
        },
        { "Object ID": 1 }
      )
      .sort({ "Object ID": 1 })
      .toArray();

    const { results, errors } = await PromisePool.for(idsToProcess)
      .withConcurrency(15)
      .process(async (object) => {
        process.stdout.write(
          `Processing: ${object["Object ID"]}, total additions: ${added}\r`
        );

        try {
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
        } catch (err) {
          process.stdout.write("\r\n");
          console.log("Error on:", object["Object ID"]);
          err["Object ID"] = object["Object ID"];
          throw err;
        }
      });

    console.log("Errors", errors);
    console.log("Populated", added, "images.");
    console.timeEnd("populating");
    await client.close();
  } catch (err) {
    console.timeEnd("populating");
    await client.close();
    throw err;
  }
};

populateImages();
