import { EventEmitter } from "events";
import mongodb from "mongodb";
import csv from "./csv.js";

export class CSVLoader extends EventEmitter {
  stopOnError;

  _errors;

  constructor(options = { stopOnError: true }) {
    super();
    this._errors = {};
    this.stopOnError = options.stopOnError;
  }

  errors() {
    return this._errors;
  }

  throwErrors() {
    if (Object.keys(this._errors).length === 1) {
      const firstKey = Object.keys(this._errors)[0];
      throw this._errors[Number(firstKey)];
    }

    if (Object.keys(this._errors).length) {
      throw new Error("multiple errors ocurred !");
    }
  }

  async insertRowsFrom(csvPath) {
    const csvProcess = csv(csvPath, this.stopOnError);

    const mongoUri = "mongodb://localhost/";
    const client = new mongodb.MongoClient(mongoUri);

    try {
      await client.connect();
      const database = client.db("metmuseum-raw");
      const collection = database.collection("objects");

      await csvProcess
        .onRow(async (row, index) => {
          row["Object ID"] = mongodb.Long.fromString(row["Object ID"]);
          const result = await collection.insertOne(row);
          console.log("Inserted row:", index);
          this.emit("rowLoaded", row);
        })
        .onError(async (e, row, index) => {
          this._errors[index] = e;
          this.emit("loadError", e, row, index);
        })
        .read();

      this.throwErrors();
      await client.close();
    } catch (err) {
      console.log("ERR:", err);
    }
  }
}
