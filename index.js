/**
 * Generic background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */

exports.storageNotify2 = (event, callback) => {
  const file = event.data;
  const context = event.context;

  console.log(`  Event ${context.eventId}`);
  console.log(`  Event Type: ${context.eventType}`);
  console.log(`  Bucket: ${file.bucket}`);
  console.log(`  File: ${file.name}`);
  console.log(`  Metageneration: ${file.metageneration}`);
  console.log(`  Created: ${file.timeCreated}`);
  console.log(`  Updated: ${file.updated}`);

  // Imports the Google Cloud client libraries
  const BigQuery = require('@google-cloud/bigquery');
  const Storage = require('@google-cloud/storage');

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  const projectId = "spring-iris-191608";
  const datasetId = "csv";
  const tableId = "test";

  /**
   * This sample loads the CSV file at
   * https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.json
   *
   * TODO(developer): Replace the following lines with the path to your file.
   */
  const bucketName = 'oreobox';
  const filename = file.name;

  // Instantiates clients
  const bigquery = new BigQuery({
    projectId: projectId,
  });

  const storage = new Storage({
    projectId: projectId,
  });

  // Configure the load job. For full list of options, see:
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load
  const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 1,
    autodetect: true,

    // Set the write disposition to append to an existing table.
    // Append Data Difference 
    // writeDisposition: 'WRITE_TRUNCATE',
    writeDisposition: 'WRITE_APPEND',
  };

  // Loads data from a Google Cloud Storage file into the table
  bigquery
    .dataset(datasetId)
    .table(tableId)
    .load(storage.bucket(bucketName).file(filename), metadata)
    .then(results => {
      const job = results[0];

      // load() waits for the job to finish
      // assert.equal(job.status.state, 'DONE');
      console.log(`Job ${job.id} completed.`);

      // Check the job's status for errors
      const errors = job.status.errors;
      if (errors && errors.length > 0) {
        throw errors;
      }
    })
    .catch(err => {
      console.error('ERROR:', err);
  });

  callback();
};
