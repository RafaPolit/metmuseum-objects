import fs from "fs";
import mongodb from "mongodb";
import { stdout } from "process";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });

const keysToPopulate = [
  "Artist Display Name",
  "Artist Display Bio",
  "Artist Alpha Sort",
  "Artist Nationality",
  "Artist Begin Date",
  "Artist End Date",
  "Artist Gender",
  "Artist ULAN URL",
  "Artist Wikidata URL",
];

const escapeQuotes = (string) => {
  return string.replace(/\"/g, '""');
};

let extractYear = (string) => {
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

const export_artists = async () => {
  console.log("Exporting artists...");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const artists = database.collection("aritsts");

    // const cursor = artists.find({}, { limit: 30 });
    const cursor = artists.find({});

    let artist;
    let exported = 0;

    var artistsCsv = fs.createWriteStream("../csvs/artists.csv", {
      flags: "a",
    });

    artistsCsv.write(
      "title;Artist Display Bio;Artist Alpha Sort;Artist Nationality;Artist Begin Date;Artist End Date;Artist Gender;Artist ULAN URL;Artist Wikidata URL\r\n"
    );

    while ((artist = await cursor.next())) {
      process.stdout.write(`Exporting: ${artist["Artist Display Name"]}\r`);
      const row = [];
      keysToPopulate.forEach((key) => {
        if (key === "Artist Begin Date" || key === "Artist End Date") {
          row.push(extractYear(artist[key]));
        } else if (key === "Artist ULAN URL" || key === "Artist Wikidata URL") {
          if (artist[key].indexOf("http") === 0) {
            row.push(
              artist[key]
                ? `"${artist[key].substr(0, 33)}...|${artist[key]}"`
                : '""'
            );
          } else {
            row.push('""');
          }
        } else {
          row.push(`"${escapeQuotes(artist[key])}"`);
        }
      });
      artistsCsv.write(`${row.join(";")}\r\n`);
      exported += 1;
    }
    process.stdout.write("\r\n");

    console.log("Exported ", exported, " artists.");
    await client.close();
  } catch (err) {
    await client.close();
    throw err;
  }
};

export_artists();
