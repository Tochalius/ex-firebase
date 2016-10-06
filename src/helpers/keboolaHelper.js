'use strict';
import {
  isNumber
} from 'lodash';
import {
  DEFAULT_BATCH,
  IS_INCREMENTAL,
  DEFAULT_PAGE_SIZE
} from '../constants';

/**
 * This is a simple helper that checks whether the input configuration is valid.
 * If so, the particular object with relevant parameters is returned.
 * Otherwise, an error is thrown.
 */
export function parseConfiguration(configObject) {
  return new Promise((resolve, reject) => {
    // Read information about the api key.
    const apiKey = configObject.get('parameters:#apiKey');
    if (!apiKey) {
      reject('Parameter #apiKey is not defined! Please check out the documentation for more information!');
    }
    // Read information about the domain.
    const domain = configObject.get('parameters:domain');
    if (!domain) {
      reject('Parameter domain is not defined! Please check out the documentation for more information!');
    }
    // Read information about the endpoint.
    const endpoint = configObject.get('parameters:endpoint');
    if (!endpoint) {
      reject('Parameter endpoint is not defined! Please check out the documentation for more information!');
    }
    // Read bucketName
    const bucketName = configObject.get('parameters:bucketName');
    if (!bucketName) {
      reject('Parameter bucketName is not defined! Please check out the documentation for more information!');
    }
    // Read the authDomain parameter
    const authDomain = configObject.get('parameters:authDomain');
    if (!authDomain) {
      reject('Parameter authDomain is not defined! Please check out the documentation for more information!');
    }
    // Read the databaseURL parameter
    const databaseURL = configObject.get('parameters:databaseURL');
    if (!databaseURL) {
      reject('Parameter databaseURL is not defined! Please check out the documentation for more information!');
    }
    // Read the storageBucket parameter
    const storageBucket = configObject.get('parameters:storageBucket');
    if (!storageBucket) {
      reject('Parameter storageBucket is not defined! Please check out the documentation for more information!');
    }
    // Read page size
    const pageSize = configObject.get('parameters:pageSize') || DEFAULT_PAGE_SIZE;
    if (!isNumber(pageSize)) {
      reject('Invalid pageSize parameter! Please make sure there is a numeric value!');
    }

    // Read clientEmail
    const clientEmail = configObject.get('parameters:clientEmail');
    if (!clientEmail) {
      reject('Parameter clientEmail is not specified! Check out the documentation for more details!');
    }

    // Read privateKey
    const privateKey = configObject.get('parameters:#privateKey');
    if (!privateKey) {
      reject('Parameter privateKey is not specified! Check out the documentation for more details!');
    }

    // Read batch
    const batch = configObject.get('parameters:batch') || DEFAULT_BATCH;
    if (!batch.match(/firstHalf|secondHalf/)) {
      reject('Invalid value of the parameter batch. Please specify either firstHalf or secondHalf');
    }

    resolve({
      batch,
      apiKey,
      domain,
      endpoint,
      pageSize,
      privateKey,
      authDomain,
      bucketName,
      clientEmail,
      databaseURL,
      storageBucket
    });
  });
}

/**
 * This function prepares object containing metadata required for writing
 * output data into Keboola (output files & manifests).
 */
export function getKeboolaStorageMetadata(tableOutDir, bucketName, tableName) {
  const incremental = IS_INCREMENTAL;
  const destination = `${bucketName}.${tableName}`;
  const fileName = `${tableOutDir}/${tableName}.csv`;
  const manifestFileName = `${fileName}.manifest`;
  return { fileName, incremental, destination, manifestFileName };
}
