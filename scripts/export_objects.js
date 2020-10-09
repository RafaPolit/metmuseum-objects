import fs from "fs";
import mongodb from "mongodb";
import objectsStructure from "../config/objects-structure.js";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });
const passedArgs = process.argv.slice(2);

const keysToPopulate = objectsStructure;

const escapeQuotes = (string) => {
  return string.replace(/\"/g, '""');
};

const extractYear = (string) => {
  if (string !== "") {
    let date = string;
    const firstCharacter = string.indexOf("-") === 0 ? "-" : "";
    if (firstCharacter === "-") {
      date = date.substr(1, date.length - 1);
    }

    for (let pos = 0; pos < 5; pos++) {
      if (date.indexOf("0") === 0) {
        date = date.substr(1, date.length - 1);
      }
    }

    const year = Number(date.split("-")[0]);
    return Number(`${firstCharacter}${year}`);
  }

  return "";
};

const conformLink = (string) => {
  let link = '""';
  if (string && string.indexOf("http") === 0) {
    link = `"${string.substr(0, 33)}...|${string}"`;
  }
  return link;
};

const processField = {
  string: (string) => `"${escapeQuotes(string)}"`,
  integer: (value) =>
    value ? mongodb.Long.fromString(value.toString()) : '""',
  year: (string) => extractYear(string),
  link: (string) => conformLink(string),
};

const export_objects = async () => {
  console.log("Exporting objects...");
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
    let exported = 0;

    var objectsCsv = fs.createWriteStream(
      `../csvs/objects-${startID}-to-${endID}.csv`,
      {
        flags: "a",
      }
    );

    const keys = [];

    keysToPopulate.forEach((key) => {
      keys.push(key.rename || key.key);
    });

    objectsCsv.write(`${keys.join(";")}\r\n`);

    while ((object = await cursor.next())) {
      process.stdout.write(`Exporting: ${object["Object ID"]}\r`);

      const row = [];
      keysToPopulate.forEach((keyData) => {
        row.push(processField[keyData.type](object[keyData.key]));
      });
      objectsCsv.write(`${row.join(";")}\r\n`);
      exported += 1;
    }
    process.stdout.write("\r\n");

    console.log("Exported", exported, "objects.");
    await client.close();
  } catch (err) {
    await client.close();
    throw err;
  }
};

export_objects();
