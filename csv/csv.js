import csvtojson from "csvtojson";

const csv = (file, stopOnError = false) => ({
  onRowCallback: async (_row, _index) => {},
  onErrorCallback: async (_error, _row, _index) => {},
  onCompleteCallback: async () => {},

  onRow(onRowCallback) {
    this.onRowCallback = onRowCallback;
    return this;
  },

  onError(onErrorCallback) {
    this.onErrorCallback = onErrorCallback;
    return this;
  },

  onComplete(onCompleteCallback) {
    console.log("Complete!");
    this.onComplete = onCompleteCallback;
    return this;
  },

  async read() {
    return csvtojson({ delimiter: [",", ";"], needEmitAll: false })
      .fromFile(file)
      .subscribe(
        (row, index) => {
          return new Promise(async (resolve, reject) => {
            try {
              await this.onRowCallback(row, index);
              resolve();
            } catch (e) {
              await this.onErrorCallback(e, row, index);
              if (stopOnError) {
                reject(e);
              } else {
                resolve();
              }
            }
          });
        },
        this.onError,
        this.onComplete
      );
  },
});

export default csv;
