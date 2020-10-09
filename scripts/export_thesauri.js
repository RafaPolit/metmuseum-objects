import fs from "fs";
import mongodb from "mongodb";
import objectsStructure from "../config/objects-structure.js";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });
const passedArgs = process.argv.slice(2);

const keysToPopulate = objectsStructure;

const processString = (string) => {
  const noQuotes = string.replace(/\"/g, '""');
  return noQuotes.replace(/\\r\\n/g, " ");
};

const export_thesauri = async () => {
  console.log("Exporting thesauri...");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const objects = database.collection("objects");

    const startID = passedArgs[0] || 0;
    const endID = passedArgs[1] || 1000000;

    const cursor = objects.find({
      $and: [
        { "Object ID": { $gte: Number(startID) } },
        { "Object ID": { $lte: Number(endID) } },
      ],
    });

    let object;

    const sets = {};
    const csvs = {};

    keysToPopulate.forEach((keyData) => {
      if (keyData.thesaurus) {
        sets[keyData.key] = new Set();
        csvs[keyData.key] = fs.createWriteStream(
          `../csvs/thesaurus-${keyData.key.replace(/\s/g, "")}.csv`,
          {
            flags: "a",
          }
        );
        csvs[keyData.key].write("English\r\n");
      }
    });

    while ((object = await cursor.next())) {
      process.stdout.write(`Analizing: ${object["Object ID"]}\r`);

      keysToPopulate.forEach((keyData) => {
        if (keyData.thesaurus) {
          const valuesArray = object[keyData.key].split("|");
          valuesArray.forEach((value) => {
            if (value) {
              sets[keyData.key].add(value);
            }
          });
        }
      });
    }

    process.stdout.write("\r\n");

    console.log("Summary:");

    keysToPopulate.forEach((keyData) => {
      if (keyData.thesaurus) {
        console.log(`${keyData.key}: ${sets[keyData.key].size}`);
        const itemsArray = [...sets[keyData.key]].sort();
        itemsArray.forEach((item) => {
          csvs[keyData.key].write(`"${processString(item)}"\r\n`);
        });
      }
    });

    await client.close();
  } catch (err) {
    await client.close();
    throw err;
  }
};

export_thesauri();
