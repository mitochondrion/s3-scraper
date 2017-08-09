#!/usr/local/bin/python3

import time
import sys
from collections import defaultdict
import xml.etree.ElementTree as etree
import requests

MIN_SIZE = 250000
THROTTLE = 0.5
MAX_PAGES = 15000

args = sys.argv

if len(args) < 2:
    print('Missing bucket argument!')
    exit()
else:
    bucket = args[1]

initialUrl = f'https://s3.amazonaws.com/{bucket}/'
print(initialUrl)

queryParams = {
    'list-type': '2',
    'max-keys': '1000'
}

files = []
fileTypeCounts = defaultdict(int)
page = 1
errorCount = 0

def writeDiagnostic(lastResponse, finalStateInfo=''):
    fileTypeCountsMsg = ' | '.join([f'{key}: {fileTypeCounts[key]}' for key in fileTypeCounts])
    diagnosticMsg = f'Pages: {page}, Errors: {errorCount}\n\n==FINAL STATE==\n\n{finalStateInfo}\n\n==FINAL RESPONSE==\n\n{lastResponse}\n\n==HEADERS==\n\n{lastResponse.headers}\n\n==CONTENT==\n\n{lastResponse.content}\n\n==FILE TYPES==\n\nTotal: {len(files)} ({fileTypeCountsMsg})\n\n=====\n'
    diagnosticFile = open('./diagnostic.out', 'w')
    diagnosticFile.write(diagnosticMsg)
    diagnosticFile.close()

while page <= MAX_PAGES:
    print(f'Fetching page {page}...')

    response = requests.request('GET', initialUrl, params=queryParams)

    if response.status_code != 200:
        errorCount += 1
        print(f'ERROR: Received {response.status_code}\n\n==HEADERS==\n\n{response.headers}\n\n==CONTENT==\n\n{response.content}\n\n=====\n\n')

        # Backoff and retry once
        print('Retrying...\n\n')
        time.sleep(5)
        response = requests.request('GET', initialUrl, params=queryParams)

        if response.status_code != 200:
            print(f'FATAL: Failed on retry. Received {response.status_code}. Aborting.\n\n')
            writeDiagnostic(response, 'Failed on retry.')
            break

    page += 1

    root = etree.fromstring(response.content)
    contentElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents')

    for content in contentElements:
        sizeText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text
        size = int(sizeText)
        pathText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text

        fileType = 'NONE'
        pathTokens = pathText.split('.')
        if len(pathTokens) > 1:
            fileType = pathTokens[-1]

            if fileType in ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'old'] and len(pathTokens) > 2:
                fileType = pathTokens[-2]

            fileTypeCounts[fileType] += 1

            if fileType == 'mp3' and size > MIN_SIZE:
                fileTypeCounts['mp3+'] += 1

        files.append((size, fileType, pathText))

    print('Done processing page.')
    fileTypeCountsMsg = ' | '.join([f'{key}: {fileTypeCounts[key]}' for key in fileTypeCounts])
    print(f'File count: {len(files)} ({fileTypeCountsMsg})')

    # Handle paging
    isTruncatedText = ''
    isTruncatedElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated')
    if isTruncatedElements:
        isTruncatedText = isTruncatedElements[0].text

    continuationTokenElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}NextContinuationToken')

    if continuationTokenElements and isTruncatedText == 'true' and page < MAX_PAGES:
        continuationToken = continuationTokenElements[0].text
        queryParams['continuation-token'] = continuationToken
    else:
        diagnosticMsg = f'continuationToken count: {len(continuationTokenElements)}, isTruncated: {isTruncatedText}'
        writeDiagnostic(response, diagnosticMsg)
        break

    # Throttle otherwise AWS will start to return empty results
    time.sleep(THROTTLE)

filesSorted = sorted(files, key=lambda file: file[0], reverse=True)
print(f'Largest file: {filesSorted[0]}')

def writeFile(fileName, dataList, dataTransform):
    data = '\n'.join(map(dataTransform, dataList))
    outFile = open(fileName, 'w')
    outFile.write(data)
    outFile.close()

writeFile('./files.txt', filesSorted, str)
writeFile('./files.txt', filesSorted, lambda file: f'{file[0]},{file[1]},{file[2]}')
