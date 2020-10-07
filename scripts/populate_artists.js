import mongodb from "mongodb";

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

const conformArtist = (object, index) => {
  const artist = {};
  keysToPopulate.forEach((key) => {
    const values = object[key].split("|");
    artist[key] = values[index] ? values[index].trim() : "";
  });
  return artist;
};

const populate_artists = async () => {
  console.log("Populating artists...");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const objects = database.collection("objects");
    const artists = database.collection("aritsts");

    const cursor = objects.find({});

    let object;
    let inserted = 0;

    while ((object = await cursor.next())) {
      const objectArtists = object["Artist Display Name"].split("|");

      const artistsData = [];

      objectArtists.forEach((artistDisplayName, index) => {
        if (artistDisplayName) {
          artistsData.push(conformArtist(object, index));
        }
      });

      if (artistsData.length) {
        await artistsData.reduce(async (prev, artistData) => {
          await prev;
          const artistInDB = await artists
            .find({ "Artist Display Name": artistData["Artist Display Name"] })
            .count();

          if (artistInDB) {
            return Promise.resolve();
          }
          inserted += 1;
          console.log("Insert", inserted);
          return artists.insertOne(artistData);
        }, Promise.resolve());
      }
    }

    console.log("Inserted ", inserted, " artists.");
    await client.close();
  } catch (err) {
    throw err;
  }
};

populate_artists();
