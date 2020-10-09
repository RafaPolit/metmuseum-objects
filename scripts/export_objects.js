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
  { key: "Department", type: "string" },
  { key: "AccessionYear", type: "year", rename: "Accession Year" },
  { key: "Object Name", type: "string" },
  { key: "Title", type: "string" },
  { key: "Culture", type: "string" },
  { key: "Period", type: "string" },
  { key: "Dynasty", type: "string" },
  { key: "Reign", type: "string" },
  { key: "Portfolio", type: "string" },
  { key: "Constituent ID", type: "integer" },
  { key: "Object Date", type: "string" },
  { key: "Object Begin Date", type: "year" },
  { key: "Object End Date", type: "year" },
  { key: "Medium", type: "string" },
  { key: "Dimensions", type: "string" },
  { key: "Credit Line", type: "string" },
  { key: "Geography Type", type: "string" },
  { key: "City", type: "string" },
  { key: "State", type: "string" },
  { key: "County", type: "string" },
  { key: "Region", type: "string" },
  { key: "Subregion", type: "string" },
  { key: "Locale", type: "string" },
  { key: "Locus", type: "string" },
  { key: "Excavation", type: "string" },
  { key: "River", type: "string" },
  { key: "Classification", type: "string" },
  { key: "Rights and Reproduction", type: "string" },
  { key: "Link Resource", type: "link" },
  { key: "Object Wikidata URL", type: "link" },
  { key: "Metadata Date", type: "string" },
  { key: "Repository", type: "string" },
  { key: "Tags", type: "string" },
];

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
