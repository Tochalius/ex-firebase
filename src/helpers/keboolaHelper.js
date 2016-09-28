'use strict';
import { IS_INCREMENTAL } from '../constants';

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

    resolve({ apiKey, domain, endpoint });
  });
}

/**
 * This function prepares object containing metadata required for writing
 * output data into Keboola (output files & manifests).
 */
export function getKeboolaStorageMetadata(tableOutDir, tableName) {
  const incremental = IS_INCREMENTAL;
  const destination = `${tableName}`;
  const fileName = `${tableOutDir}/${tableName}.csv`;
  const manifestFileName = `${fileName}.manifest`;
  return { fileName, incremental, destination, manifestFileName };
}
