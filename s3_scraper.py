#!/usr/local/bin/python3

import time
import sys
import xml.etree.ElementTree as etree
import requests

MIN_SIZE = 250000
THROTTLE = 1
MAX_PAGES = 2500

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

targetFiles = []
mp3Sizes = []
fileTypes = {}
page = 1
errorCount = 0

def writeDiagnostic(lastResponse, finalStateInfo=''):
    diagnosticMsg = f'Pages: {page}, Errors: {errorCount}\n\n==FINAL STATE==\n\n{finalStateInfo}\n\n==FINAL RESPONSE==\n\n{lastResponse}\n\n==HEADERS==\n\n{lastResponse.headers}\n\n==CONTENT==\n\n{lastResponse.content}\n\n==FILE TYPES==\n\n{fileTypes}\n\n=====\n'
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
        mp3Sizes.append(size)
        path = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key')
        pathText = path.text

        pathTokens = pathText.split('.')
        if len(pathTokens) > 1:
            fileType = pathTokens[-1]
            fileTypes[fileType] = pathText

        if size > MIN_SIZE and path is not None:
            if pathText.endswith('.mp3'):
                targetFiles.append((size, pathText))

    print('Done processing.')
    print(f'Count: {len(targetFiles)} target files of {len(mp3Sizes)} total mp3 files.\n')

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
        # '{etree.tostring(root, encoding="utf8", method="xml")}'
        writeDiagnostic(response, diagnosticMsg)
        break

    # Throttle otherwise AWS will start to return empty results
    time.sleep(THROTTLE)

targetFilesSorted = sorted(targetFiles, key=lambda file: file[0], reverse=True)
print(f'{targetFilesSorted[0]}, {targetFilesSorted[-1]}')

sizesData = '\n'.join(map(str, mp3Sizes))
sizesFile = open('./sizes.txt', 'w')
sizesFile.write(sizesData)
sizesFile.close()

filesData = '\n'.join(
    map(
        lambda file: f'{file[0]},{file[1]}\n',
        targetFilesSorted
    )
)
filesFile = open('./files.txt', 'w')
filesFile.write(filesData)
filesFile.close()
