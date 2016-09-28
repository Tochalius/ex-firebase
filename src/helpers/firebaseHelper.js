import fs from 'fs';
import csv from 'fast-csv';
import path from 'path';
import moment from 'moment';
import isThere from 'is-there';
import request from 'request-promise';
import jsonfile from 'jsonfile';
import {
  size,
  first,
  groupBy,
  isArray,
  replace,
  isNumber,
  toString
} from 'lodash';
import {
  EVENT_ERROR,
  EVENT_FINISH
} from '../constants';
import { getKeboolaStorageMetadata } from './keboolaHelper';

/**
 * This function request and endpoint and returns data for the further processing.
 */
export function fetchData({ apiKey, domain, endpoint }) {
  return request({ uri: `https://${domain}.firebaseio.com/${endpoint}.json?auth=${apiKey}`, json: true });
}

/**
 * This function just group data by event type.
 */
export function groupDataByEventType(input) {
  return groupBy(input, 'eventType');
}

/**
 * This function generates output promises which lead into creating output files.
 */
export function generateOutputFiles(outputDirectory, data) {
  return Object
    .keys(data)
    .map(key => createOutputFile(path.join(outputDirectory, `${replace(key,'.', '_')}.csv`), data[key]));
}

/**
 * This function generates output manifests which help to upload data into KBC
 */
export function generateOutputManifests(outputDirectory, bucketName, data) {
  return Object
    .keys(data)
    .map(key => {
      const {
        fileName,
        incremental,
        destination,
        manifestFileName
      } = getKeboolaStorageMetadata(outputDirectory, bucketName, replace(key,'.', '_'));
      return createManifestFile(manifestFileName, { destination, incremental });
    });
}

/**
 * This function just stores data to selected destination.
 * Data is appending to a file, the first one needs to have a header.
 */
export function createOutputFile(fileName, data) {
  return new Promise((resolve, reject) => {
    const headers = !isThere(fileName);
    const includeEndRowDelimiter = true;
    csv
      .writeToStream(fs.createWriteStream(fileName, {'flags': 'a'}), data, { headers, includeEndRowDelimiter })
      .on(EVENT_ERROR, () => reject('Problem with writing data into output!'))
      .on(EVENT_FINISH, () => resolve('File created!'));
  });
}

/**
 * This function simply create a manifest file related to the output data
 */
export function createManifestFile(fileName, data) {
  return new Promise((resolve, reject) => {
    jsonfile.writeFile(fileName, data, {}, (error) => {
      if (error) {
        reject(error);
      } else {
        resolve('Manifest created!');
      }
    });
  });
}

/**
 * This function reads input data and prepare the output objects suitable for CSV output.
 */
export function prepareDataForOutput(data) {
  return mergeFirebaseIdWithObject(
    convertArrayOfObjectsIntoObject(
      Object.keys(data)
        .map(object => {
          return { [ object ]: normalizeOutput(data[object])};
        }
      )
    )
  );
}

/**
 * This function reads all fields except data array itself and returns object
 * which is going to be included in the final result.
 */
export function getParentData(object) {
  return Object.keys(object).reduce((previous, current) => {
    if (!isArray(object[current])) {
      return { ...previous, [ current ] : convertEpochToDateIfAvailable(object[current]) };
    }
  }, {});
}

/**
 * This function detects whether the input is in epoch date format.
 * If so, the function returns a converted value, otherwise the original
 * value is returned.
 */
export function convertEpochToDateIfAvailable(value) {
  return isNumber(value) && toString(value).length === 10
    ? moment(value, 'X').utc().format("YYYY-MM-DD HH:mm:ss")
    : value;
}

/**
 * This function detects whether a particular value is an array.
 * If so, join the values together, otherwise it
 * returns just a value.
 */
export function convertArrayToStringIfAvailable(object) {
  return Object.keys(object).reduce((previous, current) => {
      return isArray(object[current])
        ? { ...previous, [ current ] : object[current].join(',') }
        : { ...previous, [ current ] : object[current] }
  }, {});
}

/**
 * This function gets data for particular firebaseId and return object.
 */
export function normalizeOutput(data) {
  const parentObject = getParentData(data);
  return size(data['data']) > 0
    ? data['data'].map(event => {
      return Object.assign({}, parentObject, convertArrayToStringIfAvailable(event))
    }) : null;
}

/**
 * Convert array of object into single object.
 */
export function convertArrayOfObjectsIntoObject(input) {
  return input.reduce((memo, object) => {
    const key = first(Object.keys(object));
    memo[key] = object[key];
    return memo;
  }, {});
}

/**
 * Merge firebaseId with the other attributes
 */
export function mergeFirebaseIdWithObject(data) {
  return Object.keys(data)
    .map(element => {
      return data[element]
        .map(object => {
          return Object.assign({}, { ...object, firebaseId: element } )
        });
    });
}
