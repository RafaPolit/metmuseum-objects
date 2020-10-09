import fs from "fs";
import mongodb from "mongodb";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });
const passedArgs = process.argv.slice(2);

const keysToPopulate = [
  { key: "Object Number", type: "string" },
  { key: "Is Highlight", type: "string" },
  { key: "Is Timeline Work", type: "string" },
  { key: "Is Public Domain", type: "string" },
  { key: "Object ID", type: "integer" },
  { key: "Gallery Number", type: "string" },
  { key: "Department", type: "string", thesaurus: true },
  { key: "AccessionYear", type: "year", rename: "Accession Year" },
  { key: "Object Name", type: "string" },
  { key: "Title", type: "string" },
  { key: "Culture", type: "string", thesaurus: true },
  { key: "Period", type: "string", thesaurus: true },
  { key: "Dynasty", type: "string", thesaurus: true },
  { key: "Reign", type: "string", thesaurus: true },
  { key: "Portfolio", type: "string" },
  { key: "Constituent ID", type: "integer" },
  { key: "Object Date", type: "string" },
  { key: "Object Begin Date", type: "year" },
  { key: "Object End Date", type: "year" },
  { key: "Medium", type: "string" },
  { key: "Dimensions", type: "string" },
  { key: "Credit Line", type: "string" },
  { key: "Geography Type", type: "string", thesaurus: true },
  { key: "City", type: "string", thesaurus: true },
  { key: "State", type: "string", thesaurus: true },
  { key: "County", type: "string", thesaurus: true },
  { key: "Region", type: "string", thesaurus: true },
  { key: "Subregion", type: "string", thesaurus: true },
  { key: "Locale", type: "string" },
  { key: "Locus", type: "string" },
  { key: "Excavation", type: "string" },
  { key: "River", type: "string" },
  { key: "Classification", type: "string", thesaurus: true },
  { key: "Rights and Reproduction", type: "string" },
  { key: "Link Resource", type: "link" },
  { key: "Object Wikidata URL", type: "link" },
  { key: "Metadata Date", type: "string" },
  { key: "Repository", type: "string", thesaurus: true },
  { key: "Tags", type: "string", thesaurus: true },
];

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
